//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   All rights reserved.
//
//   Permission is hereby granted, free of charge, to any person obtaining a copy
//   of this software and associated documentation files (the "Software"), to deal
//   in the Software without restriction, including without limitation the rights
//   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//   copies of the Software, and to permit persons to whom the Software is
//   furnished to do so, subject to the following conditions:
//
//   The above copyright notice and this permission notice shall be included in all
//   copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//   SOFTWARE.

//! PG wire protocol handler that bridges pgwire traits to the Teide engine.
//!
//! Architecture: Session (which contains !Send C pointers) lives on a dedicated
//! OS thread. The async pgwire handler communicates with it via channels,
//! avoiding any Send/Sync issues with the C engine's thread-local arenas.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{Response, Tag};
use pgwire::api::{ClientInfo, NoopHandler, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::catalog;
use super::encode;
use crate::sql::{ExecResult, Session, StoredTable};

// ---------------------------------------------------------------------------
// SendableTables: safe cross-thread transfer of StoredTable collections
// ---------------------------------------------------------------------------

/// Wrapper for cross-thread transfer of StoredTable collections.
///
/// # Safety
/// `StoredTable` contains `Table { ptr: *mut td_t, .. }` which is `!Send`.
/// This wrapper is safe because:
/// - Clones use `clone_ref()` (atomic `td_retain`) — the refcount is atomic.
/// - Each clone is moved exactly once into a dedicated engine thread.
/// - The engine thread exclusively owns its Session and StoredTable copies.
/// - The factory only creates clones; it never dereferences the raw pointers.
struct SendableTables(Vec<(String, StoredTable)>);
unsafe impl Send for SendableTables {}
unsafe impl Sync for SendableTables {}

impl SendableTables {
    fn into_inner(self) -> Vec<(String, StoredTable)> {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Engine thread: owns Session, processes queries sequentially
// ---------------------------------------------------------------------------

/// A query result serialized into Send-safe types.
/// All cell values are pre-formatted as text-protocol strings.
pub struct WireResult {
    pub columns: Vec<(String, i8)>, // (name, td_type)
    pub rows: Vec<Vec<Option<String>>>,
}

/// Response from the engine thread.
pub enum EngineResponse {
    Query(WireResult),
    Ddl(String),
}

/// Request sent to the engine thread.
struct EngineRequest {
    sql: String,
    reply: tokio::sync::oneshot::Sender<Result<EngineResponse, String>>,
}

/// A Send-safe handle to a Session running on a dedicated OS thread.
/// Cloneable — each clone shares the same engine thread.
#[derive(Clone)]
pub struct SessionBridge {
    tx: std::sync::mpsc::Sender<EngineRequest>,
}

impl SessionBridge {
    /// Spawn a dedicated OS thread that owns a Session with the given tables.
    fn spawn(tables: SendableTables) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<EngineRequest>();

        std::thread::spawn(move || {
            // into_inner() consumes `tables` (SendableTables), forcing Rust 2021
            // edition to capture the whole Send wrapper, not the inner !Send Vec.
            let base_tables = tables.into_inner();

            // Create a fresh Session on this thread (initializes arena)
            let mut session = match Session::new() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[engine] Failed to create session: {e}");
                    return;
                }
            };
            // Insert base tables into this session
            for (name, stored) in base_tables {
                session.tables.insert(name, stored);
            }

            // Process requests until the channel is closed
            while let Ok(req) = rx.recv() {
                let result = match session.execute(&req.sql) {
                    Ok(ExecResult::Query(sql_result)) => {
                        let ncols = sql_result.table.ncols() as usize;
                        let nrows = sql_result.table.nrows() as usize;

                        let columns: Vec<(String, i8)> = (0..ncols)
                            .map(|i| {
                                let name = if i < sql_result.columns.len() {
                                    sql_result.columns[i].clone()
                                } else {
                                    sql_result.table.col_name_str(i)
                                };
                                let td_type = sql_result.table.col_type(i);
                                (name, td_type)
                            })
                            .collect();

                        let mut rows = Vec::with_capacity(nrows);
                        for r in 0..nrows {
                            let mut row = Vec::with_capacity(ncols);
                            for c in 0..ncols {
                                row.push(super::types::format_cell(&sql_result.table, c, r));
                            }
                            rows.push(row);
                        }

                        Ok(EngineResponse::Query(WireResult { columns, rows }))
                    }
                    Ok(ExecResult::Ddl(msg)) => Ok(EngineResponse::Ddl(msg)),
                    Err(e) => Err(e.to_string()),
                };

                // If the receiver dropped, the connection closed — just move on
                let _ = req.reply.send(result);
            }
        });

        SessionBridge { tx }
    }

    /// Send a SQL query to the engine thread and await the result.
    pub async fn query(&self, sql: String) -> Result<EngineResponse, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(EngineRequest {
                sql,
                reply: reply_tx,
            })
            .map_err(|_| "engine thread stopped".to_string())?;

        reply_rx
            .await
            .map_err(|_| "engine thread dropped reply".to_string())?
    }
}

// ---------------------------------------------------------------------------
// Table metadata (Send-safe snapshot of session table registry)
// ---------------------------------------------------------------------------

/// Send-safe metadata about a stored table (no raw pointers).
#[derive(Clone)]
pub struct TableMeta {
    pub columns: Vec<(String, i8)>, // (col_name, td_type)
    pub nrows: i64,
}

/// Send-safe snapshot of the session's table registry for catalog queries.
#[derive(Clone)]
pub struct SessionMeta {
    pub tables: Vec<(String, TableMeta)>,
}

impl SessionMeta {
    pub fn from_session(session: &Session) -> Self {
        let tables = session
            .table_names()
            .into_iter()
            .filter_map(|name| {
                session.tables.get(name).map(|st| {
                    let ncols = st.columns.len().max(st.table.ncols() as usize);
                    let columns: Vec<(String, i8)> = (0..ncols)
                        .map(|i| {
                            let col_name = if i < st.columns.len() {
                                st.columns[i].clone()
                            } else {
                                st.table.col_name_str(i)
                            };
                            let td_type = st.table.col_type(i);
                            (col_name, td_type)
                        })
                        .collect();
                    let nrows = st.table.nrows();
                    (name.to_string(), TableMeta { columns, nrows })
                })
            })
            .collect();
        SessionMeta { tables }
    }
}

// ---------------------------------------------------------------------------
// Per-connection pgwire handler
// ---------------------------------------------------------------------------

/// Per-connection handler. Owns a SessionBridge (engine-thread handle) and
/// SessionMeta (for catalog queries without hitting the engine).
pub struct TeideHandler {
    bridge: SessionBridge,
    meta: SessionMeta,
}

#[async_trait]
impl SimpleQueryHandler for TeideHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Check for catalog queries (handled locally, no engine needed)
        if catalog::is_catalog_query(query) {
            if let Some(result) = catalog::handle_catalog_query(query, &self.meta) {
                return result;
            }
        }

        // Send query to engine thread
        let sql = query.to_string();
        let result = self.bridge.query(sql).await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e,
            )))
        })?;

        match result {
            EngineResponse::Query(wire_result) => {
                let qr = encode::encode_wire_result(&wire_result)?;
                Ok(vec![Response::Query(qr)])
            }
            EngineResponse::Ddl(msg) => Ok(vec![Response::Execution(Tag::new(&msg).with_rows(0))]),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler factory
// ---------------------------------------------------------------------------

/// Factory that creates per-connection handlers.
/// Each connection gets its own engine thread with a cloned Session.
pub struct TeideHandlerFactory {
    base_tables: Arc<SendableTables>,
    meta: SessionMeta,
}

impl TeideHandlerFactory {
    /// Create a factory from an existing session. Snapshots the current
    /// table registry as the template for new connections.
    pub fn from_session(session: &Session) -> Self {
        let base_tables: Vec<(String, StoredTable)> = session
            .tables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let meta = SessionMeta::from_session(session);
        TeideHandlerFactory {
            base_tables: Arc::new(SendableTables(base_tables)),
            meta,
        }
    }

    fn make_handler(&self) -> TeideHandler {
        let tables = SendableTables(self.base_tables.0.clone());
        let bridge = SessionBridge::spawn(tables);
        TeideHandler {
            bridge,
            meta: self.meta.clone(),
        }
    }
}

impl PgWireServerHandlers for TeideHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.make_handler())
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(NoopHandler)
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(NoopHandler)
    }
}
