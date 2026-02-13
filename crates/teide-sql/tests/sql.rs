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

//! SQL integration tests for teide-sql.
//!
//! Small-data tests (26) use a ~20-row inline CSV for deterministic assertions.
//! Benchmark tests (15) use 10M-row H2OAI CSVs and are gated with `#[ignore]`.

use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use teide_sql::{ExecResult, Session, SqlResult};

// The C engine uses global state — serialize all tests.
static ENGINE_LOCK: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Test data helpers
// ---------------------------------------------------------------------------

const CSV_HEADER: &str = "id1,id2,id3,id4,id5,id6,v1,v2,v3";
const CSV_ROWS: &[&str] = &[
    "id001,id001,id0000000001,1,10,100,1,2,1.5",
    "id001,id001,id0000000002,2,20,200,2,3,2.5",
    "id001,id002,id0000000003,3,30,300,3,4,3.5",
    "id001,id002,id0000000004,1,10,100,4,5,4.5",
    "id002,id001,id0000000005,2,20,200,5,6,5.5",
    "id002,id001,id0000000006,3,30,300,6,7,6.5",
    "id002,id002,id0000000007,1,10,100,7,8,7.5",
    "id002,id002,id0000000008,2,20,200,8,9,8.5",
    "id003,id001,id0000000009,3,30,300,9,10,9.5",
    "id003,id001,id0000000010,1,10,100,10,11,10.5",
    "id003,id002,id0000000011,2,20,200,1,2,11.5",
    "id003,id002,id0000000012,3,30,300,2,3,12.5",
    "id004,id001,id0000000013,1,10,100,3,4,1.5",
    "id004,id001,id0000000014,2,20,200,4,5,2.5",
    "id004,id002,id0000000015,3,30,300,5,6,3.5",
    "id004,id002,id0000000016,1,10,100,6,7,4.5",
    "id005,id001,id0000000017,2,20,200,7,8,5.5",
    "id005,id001,id0000000018,3,30,300,8,9,6.5",
    "id005,id002,id0000000019,1,10,100,9,10,7.5",
    "id005,id002,id0000000020,2,20,200,10,11,8.5",
];

fn create_test_csv() -> (tempfile::NamedTempFile, String) {
    let mut f = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .unwrap();
    writeln!(f, "{CSV_HEADER}").unwrap();
    for row in CSV_ROWS {
        writeln!(f, "{row}").unwrap();
    }
    f.flush().unwrap();
    let path = f.path().to_str().unwrap().to_string();
    (f, path)
}

fn create_join_right_csv() -> (tempfile::NamedTempFile, String) {
    let mut f = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .unwrap();
    writeln!(f, "id1,x1").unwrap();
    writeln!(f, "id001,100").unwrap();
    writeln!(f, "id002,200").unwrap();
    writeln!(f, "id003,300").unwrap();
    writeln!(f, "id006,400").unwrap();
    f.flush().unwrap();
    let path = f.path().to_str().unwrap().to_string();
    (f, path)
}

/// Create a session with the test CSV registered as table "csv".
fn setup_session() -> (Session, tempfile::NamedTempFile) {
    let (file, path) = create_test_csv();
    let mut session = Session::new().unwrap();
    session
        .execute(&format!("CREATE TABLE csv AS SELECT * FROM '{path}'"))
        .unwrap();
    (session, file)
}

/// Extract SqlResult from ExecResult::Query, panicking otherwise.
fn unwrap_query(result: ExecResult) -> SqlResult {
    match result {
        ExecResult::Query(r) => r,
        ExecResult::Ddl(msg) => panic!("expected Query, got Ddl: {msg}"),
    }
}

// ---------------------------------------------------------------------------
// Small-data SQL correctness tests
// ---------------------------------------------------------------------------

#[test]
fn select_star() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT * FROM csv").unwrap());
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 9);
}

#[test]
fn select_columns() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT id1, v1 FROM csv").unwrap());
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.columns[0], "id1");
    assert_eq!(r.columns[1], "v1");
}

#[test]
fn select_literal_scalar_rejected() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let err = match session.execute("SELECT 'x' as s FROM csv") {
        Ok(_) => panic!("expected scalar literal projection to be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("scalar type"));
}

#[test]
fn where_clause() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT * FROM csv WHERE v1 > 3").unwrap());
    assert_eq!(r.table.nrows(), 14);
}

#[test]
fn group_by_sum() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, SUM(v1) as s FROM csv GROUP BY id1 ORDER BY id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 5);
    assert_eq!(r.columns.len(), 2);

    // Sorted by id1: id001=10, id002=26, id003=22, id004=18, id005=34
    assert_eq!(r.table.get_i64(1, 0).unwrap(), 10); // id001
    assert_eq!(r.table.get_i64(1, 1).unwrap(), 26); // id002
    assert_eq!(r.table.get_i64(1, 2).unwrap(), 22); // id003
    assert_eq!(r.table.get_i64(1, 3).unwrap(), 18); // id004
    assert_eq!(r.table.get_i64(1, 4).unwrap(), 34); // id005
}

#[test]
fn group_by_avg() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, AVG(v3) as avg_v3 FROM csv GROUP BY id1 ORDER BY id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 5);

    assert!((r.table.get_f64(1, 0).unwrap() - 3.0).abs() < 1e-10); // id001
    assert!((r.table.get_f64(1, 1).unwrap() - 7.0).abs() < 1e-10); // id002
    assert!((r.table.get_f64(1, 2).unwrap() - 11.0).abs() < 1e-10); // id003
}

#[test]
fn group_by_multi() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, id4, SUM(v1) as s, COUNT(*) as cnt FROM csv GROUP BY id1, id4",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 15);
    assert_eq!(r.columns.len(), 4);
}

#[test]
fn having() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, SUM(v1) as s FROM csv GROUP BY id1 HAVING SUM(v1) > 10")
            .unwrap(),
    );
    // id001=10 excluded, id002=26, id003=22, id004=18, id005=34 pass
    assert_eq!(r.table.nrows(), 4);
}

#[test]
fn order_by_asc() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT * FROM csv ORDER BY id4").unwrap());
    assert_eq!(r.table.nrows(), 20);

    // id4 is the first column in the sorted output or at its original position
    // Find id4 column index
    let id4_idx = r.columns.iter().position(|c| c == "id4").unwrap();
    let mut prev = i64::MIN;
    for row in 0..20 {
        let val = r.table.get_i64(id4_idx, row).unwrap();
        assert!(val >= prev, "id4 not ascending at row {row}");
        prev = val;
    }
}

#[test]
fn order_by_desc() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT * FROM csv ORDER BY v3 DESC").unwrap());
    assert_eq!(r.table.nrows(), 20);

    let v3_idx = r.columns.iter().position(|c| c == "v3").unwrap();
    let mut prev = f64::MAX;
    for row in 0..20 {
        let val = r.table.get_f64(v3_idx, row).unwrap();
        assert!(val <= prev, "v3 not descending at row {row}");
        prev = val;
    }
}

#[test]
fn order_by_limit() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT * FROM csv ORDER BY id4 LIMIT 3")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 3);

    // All 3 rows should have id4=1
    let id4_idx = r.columns.iter().position(|c| c == "id4").unwrap();
    for row in 0..3 {
        assert_eq!(r.table.get_i64(id4_idx, row).unwrap(), 1);
    }
}

#[test]
fn order_by_offset_limit() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT * FROM csv ORDER BY id4 LIMIT 3 OFFSET 2")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 3);
}

#[test]
fn distinct() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT DISTINCT id1 FROM csv").unwrap());
    assert_eq!(r.table.nrows(), 5);
    assert_eq!(r.columns.len(), 1);
}

#[test]
fn count_star() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(session.execute("SELECT COUNT(*) FROM csv").unwrap());
    assert_eq!(r.table.nrows(), 1);
    assert_eq!(r.table.get_i64(0, 0).unwrap(), 20);
}

#[test]
fn nested_agg() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, SUM(v1) as s, MIN(v2) as mn, MAX(v3) as mx \
                 FROM csv GROUP BY id1 ORDER BY id1",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 5);
    assert_eq!(r.columns.len(), 4);

    // id001: SUM(v1)=10, MIN(v2)=2, MAX(v3)=4.5
    assert_eq!(r.table.get_i64(1, 0).unwrap(), 10);
    assert_eq!(r.table.get_i64(2, 0).unwrap(), 2);
    assert!((r.table.get_f64(3, 0).unwrap() - 4.5).abs() < 1e-10);
}

#[test]
fn case_when() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT CASE WHEN v1 > 3 THEN 'high' ELSE 'low' END as label FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 1);
}

#[test]
fn like_filter() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT * FROM csv WHERE id1 LIKE 'id001'")
            .unwrap(),
    );
    // Exact match: 4 rows where id1='id001'
    assert_eq!(r.table.nrows(), 4);
}

#[test]
fn ilike_filter() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT * FROM csv WHERE id1 ILIKE 'ID001'")
            .unwrap(),
    );
    // Case-insensitive exact match: still 4 rows where id1='id001'
    assert_eq!(r.table.nrows(), 4);
}

#[test]
fn join_inner_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (_left_file, left_path) = create_test_csv();
    let (_right_file, right_path) = create_join_right_csv();
    let mut session = Session::new().unwrap();
    session
        .execute(&format!("CREATE TABLE x AS SELECT * FROM '{left_path}'"))
        .unwrap();
    session
        .execute(&format!("CREATE TABLE y AS SELECT * FROM '{right_path}'"))
        .unwrap();

    let r = unwrap_query(
        session
            .execute("SELECT * FROM x INNER JOIN y ON x.id1 = y.id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 12);
}

#[test]
fn join_left_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (_left_file, left_path) = create_test_csv();
    let (_right_file, right_path) = create_join_right_csv();
    let mut session = Session::new().unwrap();
    session
        .execute(&format!("CREATE TABLE x AS SELECT * FROM '{left_path}'"))
        .unwrap();
    session
        .execute(&format!("CREATE TABLE y AS SELECT * FROM '{right_path}'"))
        .unwrap();

    let r = unwrap_query(
        session
            .execute("SELECT * FROM x LEFT JOIN y ON x.id1 = y.id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
}

#[test]
fn cte() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // Use HAVING inside the CTE to filter (avoids column-name propagation issue)
    let r = unwrap_query(
        session
            .execute(
                "WITH t AS (SELECT id1, SUM(v1) as s FROM csv GROUP BY id1 HAVING SUM(v1) > 20) \
                 SELECT * FROM t",
            )
            .unwrap(),
    );
    // id002=26, id003=22, id005=34 pass; id001=10, id004=18 fail
    assert_eq!(r.table.nrows(), 3);
}

#[test]
fn union_all() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1 FROM csv WHERE v1 = 1 \
                 UNION ALL \
                 SELECT id1 FROM csv WHERE v1 = 2",
            )
            .unwrap(),
    );
    // v1=1 → 2 rows (rows 0,10); v1=2 → 2 rows (rows 1,11); total = 4
    assert_eq!(r.table.nrows(), 4);
}

#[test]
fn union_distinct() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1 FROM csv WHERE v1 = 1 \
                 UNION \
                 SELECT id1 FROM csv WHERE v1 = 2",
            )
            .unwrap(),
    );
    // v1=1 → id001,id003; v1=2 → id001,id003; UNION deduplicates to 2
    assert_eq!(r.table.nrows(), 2);
}

#[test]
fn union_all_literal_column_from_table() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let err = match session.execute(
        "SELECT 'x' AS s FROM csv \
         UNION ALL \
         SELECT 'x' AS s FROM csv",
    ) {
        Ok(_) => panic!("expected UNION ALL literal-column rejection"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("scalar type"));
}

#[test]
fn intersect_all_literal_column_from_table() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let err = match session.execute(
        "SELECT 'x' AS s FROM csv \
         INTERSECT ALL \
         SELECT 'x' AS s FROM csv",
    ) {
        Ok(_) => panic!("expected INTERSECT ALL literal-column rejection"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("scalar type"));
}

#[test]
fn except_all_literal_column_from_table() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let err = match session.execute(
        "SELECT 'x' AS s FROM csv \
         EXCEPT ALL \
         SELECT 'x' AS s FROM csv",
    ) {
        Ok(_) => panic!("expected EXCEPT ALL literal-column rejection"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("scalar type"));
}

#[test]
fn intersect_all_distinct_keys() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1 FROM csv WHERE id1 = 'id001' \
                 INTERSECT ALL \
                 SELECT id1 FROM csv WHERE id1 = 'id002'",
            )
            .unwrap(),
    );
    // No shared key values between the two inputs.
    assert_eq!(r.table.nrows(), 0);
}

#[test]
fn except_all_distinct_keys() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1 FROM csv WHERE id1 = 'id001' \
                 EXCEPT ALL \
                 SELECT id1 FROM csv WHERE id1 = 'id002'",
            )
            .unwrap(),
    );
    // Left side has four id001 rows; right side does not remove any of them.
    assert_eq!(r.table.nrows(), 4);
    for row in 0..r.table.nrows() as usize {
        assert_eq!(r.table.get_str(0, row).unwrap(), "id001");
    }
}

#[test]
fn subquery() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // Filter inside the subquery via HAVING (avoids alias-in-WHERE limitation)
    let r = unwrap_query(
        session
            .execute(
                "SELECT * FROM (SELECT id1, SUM(v1) as s FROM csv GROUP BY id1 HAVING SUM(v1) > 20) sub",
            )
            .unwrap(),
    );
    // id002=26, id003=22, id005=34 pass
    assert_eq!(r.table.nrows(), 3);
}

#[test]
fn string_functions() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT UPPER(id1) as u, LENGTH(id1) as l FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 2);

    // First row: UPPER('id001') = 'ID001', LENGTH('id001') = 5
    assert_eq!(r.table.get_str(0, 0).unwrap(), "ID001");
    assert_eq!(r.table.get_i64(1, 0).unwrap(), 5);
}

#[test]
fn math_expressions() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT v1 + v2 as s, v3 * 2.0 as d FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 2);

    // Row 0: v1=1 + v2=2 = 3; v3=1.5 * 2.0 = 3.0
    assert_eq!(r.table.get_i64(0, 0).unwrap(), 3);
    assert!((r.table.get_f64(1, 0).unwrap() - 3.0).abs() < 1e-10);
}

#[test]
fn scalar_subexpr_broadcast() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();

    let r = unwrap_query(
        session
            .execute("SELECT v1 + 2 * 1 as x, v1 + 2 as y FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 2);
    for i in 0..r.table.nrows() as usize {
        assert_eq!(r.table.get_i64(0, i).unwrap(), r.table.get_i64(1, i).unwrap());
    }

    let agg = unwrap_query(
        session
            .execute("SELECT SUM(v1 + 2 * 1) as a, SUM(v1 + 2) as b, SUM(v1 - 2 * 1) as d, SUM(v1 - 2) as e, SUM(2 * 1) as c FROM csv")
            .unwrap(),
    );
    assert_eq!(agg.table.nrows(), 1);
    assert_eq!(agg.table.get_i64(0, 0).unwrap(), agg.table.get_i64(1, 0).unwrap());
    assert_eq!(agg.table.get_i64(2, 0).unwrap(), agg.table.get_i64(3, 0).unwrap());
    assert_eq!(agg.table.get_i64(4, 0).unwrap(), 40);

    let linear = unwrap_query(
        session
            .execute("SELECT SUM((v1 + 1) * 2) as a, SUM(v1) * 2 + COUNT(*) * 2 as b, AVG(v1 + v2 + 1) as c, AVG(v1 + v2) + 1 as d FROM csv")
            .unwrap(),
    );
    assert_eq!(linear.table.nrows(), 1);
    assert_eq!(linear.table.get_i64(0, 0).unwrap(), linear.table.get_i64(1, 0).unwrap());
    let c = linear.table.get_f64(2, 0).unwrap();
    let d = linear.table.get_f64(3, 0).unwrap();
    assert!((c - d).abs() < 1e-10, "expected AVG linear forms to match: {c} vs {d}");
}

#[test]
fn create_table_as() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();

    let ddl = session
        .execute("CREATE TABLE t AS SELECT id1, SUM(v1) as s FROM csv GROUP BY id1")
        .unwrap();
    match ddl {
        ExecResult::Ddl(msg) => assert!(msg.contains("Created table"), "unexpected: {msg}"),
        _ => panic!("expected Ddl result"),
    }

    let r = unwrap_query(session.execute("SELECT * FROM t ORDER BY id1").unwrap());
    assert_eq!(r.table.nrows(), 5);
    assert_eq!(r.columns.len(), 2);
}

#[test]
fn multi_sort_keys() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT * FROM csv ORDER BY id1, id4 DESC LIMIT 5")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 5);

    // First 4 rows should be id001 (sorted first), id4 DESC within
    let id1_idx = r.columns.iter().position(|c| c == "id1").unwrap();
    assert_eq!(r.table.get_str(id1_idx, 0).unwrap(), "id001");
    assert_eq!(r.table.get_str(id1_idx, 3).unwrap(), "id001");
}

#[test]
fn error_table_not_found() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = Session::new().unwrap();
    let result = session.execute("SELECT * FROM nonexistent");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Benchmark tests (10M rows) — gated with #[ignore]
// ---------------------------------------------------------------------------

fn bench_dataset_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("rayforce-bench/datasets")
}

fn groupby_csv() -> Option<String> {
    let p = bench_dataset_dir().join("G1_1e7_1e2_0_0/G1_1e7_1e2_0_0.csv");
    if p.exists() {
        Some(p.to_str().unwrap().to_string())
    } else {
        None
    }
}

fn join_csv_x() -> Option<String> {
    let p = bench_dataset_dir().join("h2oai_join_1e7/J1_1e7_NA_0_0.csv");
    if p.exists() {
        Some(p.to_str().unwrap().to_string())
    } else {
        None
    }
}

fn join_csv_y() -> Option<String> {
    let p = bench_dataset_dir().join("h2oai_join_1e7/J1_1e7_1e7_0_0.csv");
    if p.exists() {
        Some(p.to_str().unwrap().to_string())
    } else {
        None
    }
}

/// Setup a session with 10M groupby CSV registered as "t".
fn setup_bench_groupby() -> Option<Session> {
    let csv = groupby_csv()?;
    let mut session = Session::new().unwrap();
    session
        .execute(&format!("CREATE TABLE t AS SELECT * FROM '{csv}'"))
        .unwrap();
    Some(session)
}

/// Setup a session with join CSVs registered as "x" and "y".
fn setup_bench_join() -> Option<Session> {
    let x_csv = join_csv_x()?;
    let y_csv = join_csv_y()?;
    let mut session = Session::new().unwrap();
    session
        .execute(&format!("CREATE TABLE x AS SELECT * FROM '{x_csv}'"))
        .unwrap();
    session
        .execute(&format!("CREATE TABLE y AS SELECT * FROM '{y_csv}'"))
        .unwrap();
    Some(session)
}

// ---- Groupby benchmarks ----

#[test]
#[ignore]
fn bench_q1() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT id1, SUM(v1) as v1 FROM t GROUP BY id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 100);
    assert_eq!(r.columns.len(), 2);
}

#[test]
#[ignore]
fn bench_q2() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT id1, id2, SUM(v1) as v1 FROM t GROUP BY id1, id2")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000);
    assert_eq!(r.columns.len(), 3);
}

#[test]
#[ignore]
fn bench_q3() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT id3, SUM(v1) as v1, AVG(v3) as v3 FROM t GROUP BY id3")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 100_000);
    assert_eq!(r.columns.len(), 3);
}

#[test]
#[ignore]
fn bench_q4() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute(
                "SELECT id4, AVG(v1) as v1, AVG(v2) as v2, AVG(v3) as v3 FROM t GROUP BY id4",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 100);
    assert_eq!(r.columns.len(), 4);
}

#[test]
#[ignore]
fn bench_q5() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute(
                "SELECT id6, SUM(v1) as v1, SUM(v2) as v2, SUM(v3) as v3 FROM t GROUP BY id6",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 100_000);
    assert_eq!(r.columns.len(), 4);
}

#[test]
#[ignore]
fn bench_q6() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT id3, MAX(v1) as v1, MIN(v2) as v2 FROM t GROUP BY id3")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 100_000);
    assert_eq!(r.columns.len(), 3);
}

#[test]
#[ignore]
fn bench_q7() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(v1) as cnt \
                 FROM t GROUP BY id1, id2, id3, id4, id5, id6",
            )
            .unwrap(),
    );
    // Nearly every row is its own group (~10M)
    assert!(r.table.nrows() > 9_000_000);
    assert_eq!(r.columns.len(), 8);
}

// ---- Sort benchmarks ----

#[test]
#[ignore]
fn bench_s1() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY id1")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

#[test]
#[ignore]
fn bench_s2() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY id3")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

#[test]
#[ignore]
fn bench_s3() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY id4")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

#[test]
#[ignore]
fn bench_s4() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY v3 DESC")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

#[test]
#[ignore]
fn bench_s5() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY id1, id2")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

#[test]
#[ignore]
fn bench_s6() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    let r = unwrap_query(
        session
            .execute("SELECT * FROM t ORDER BY id1, id2, id3")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 10_000_000);
    assert_eq!(r.columns.len(), 9);
}

// ---- Join benchmarks ----

#[test]
#[ignore]
fn bench_j1() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_join().expect("10M join CSVs not found");
    let r = unwrap_query(
        session
            .execute(
                "SELECT * FROM x INNER JOIN y ON x.id1 = y.id1 AND x.id2 = y.id2 AND x.id3 = y.id3",
            )
            .unwrap(),
    );
    // ~99,983 matched rows
    assert!(r.table.nrows() > 90_000 && r.table.nrows() < 120_000);
}

#[test]
#[ignore]
fn bench_j2() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_join().expect("10M join CSVs not found");
    let r = unwrap_query(
        session
            .execute(
                "SELECT * FROM x LEFT JOIN y ON x.id1 = y.id1 AND x.id2 = y.id2 AND x.id3 = y.id3",
            )
            .unwrap(),
    );
    // All 10M+ left rows preserved
    assert!(r.table.nrows() >= 10_000_000);
}

// ---------------------------------------------------------------------------
// Window function benchmarks (10M rows)
// ---------------------------------------------------------------------------

fn run_window_bench(label: &str, sql: &str) {
    let mut session = setup_bench_groupby().expect("10M CSV not found");
    // Warmup
    let _ = session.execute(sql).unwrap();
    let n_iter = 5;
    let mut times = Vec::with_capacity(n_iter);
    for _ in 0..n_iter {
        let t0 = std::time::Instant::now();
        let _ = session.execute(sql).unwrap();
        times.push(t0.elapsed().as_secs_f64());
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times[n_iter / 2];
    eprintln!("  {label:12}  {:.1} ms", median * 1000.0);
}

#[test]
#[ignore]
fn bench_w1() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w1", "SELECT id1, v1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1) as rn FROM t");
}

#[test]
#[ignore]
fn bench_w2() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w2", "SELECT id1, id4, RANK() OVER (PARTITION BY id1 ORDER BY id4) as rnk FROM t");
}

#[test]
#[ignore]
fn bench_w3() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w3", "SELECT id3, v1, SUM(v1) OVER (PARTITION BY id3 ORDER BY v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM t");
}

#[test]
#[ignore]
fn bench_w4() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w4", "SELECT id1, v1, LAG(v1, 1) OVER (PARTITION BY id1 ORDER BY v1) as lag_v1 FROM t");
}

#[test]
#[ignore]
fn bench_w5() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w5", "SELECT id1, v1, AVG(v1) OVER (PARTITION BY id1) as avg_v1 FROM t");
}

#[test]
#[ignore]
fn bench_w6() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    run_window_bench("w6", "SELECT id1, id2, v1, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY v1) as rn FROM t");
}

// ---------------------------------------------------------------------------
// Window function SQL tests
// ---------------------------------------------------------------------------

#[test]
fn window_row_number_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, v1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1) as rn FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.columns[2], "rn");

    // Each id1 partition (5 groups × 4 rows) → rn ∈ {1,2,3,4}
    for row in 0..r.table.nrows() as usize {
        let rn = r.table.get_i64(2, row).unwrap();
        assert!((1..=4).contains(&rn), "ROW_NUMBER should be 1..4, got {rn}");
    }
}

#[test]
fn window_rank_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, id4, RANK() OVER (PARTITION BY id1 ORDER BY id4) as rnk FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns[2], "rnk");
    for row in 0..r.table.nrows() as usize {
        let rnk = r.table.get_i64(2, row).unwrap();
        assert!(rnk >= 1, "RANK should be >= 1, got {rnk}");
    }
}

#[test]
fn window_dense_rank_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, id4, DENSE_RANK() OVER (PARTITION BY id1 ORDER BY id4) as dr FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns[2], "dr");
    for row in 0..r.table.nrows() as usize {
        let dr = r.table.get_i64(2, row).unwrap();
        assert!(
            (1..=3).contains(&dr),
            "DENSE_RANK should be 1..3 (3 distinct id4 values), got {dr}"
        );
    }
}

#[test]
fn window_running_sum_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, v1, SUM(v1) OVER (PARTITION BY id1 ORDER BY v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM csv",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns[2], "running_sum");
}

#[test]
fn window_lag_lead_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT v1, LAG(v1) OVER (ORDER BY v1) as lag_v1, LEAD(v1) OVER (ORDER BY v1) as lead_v1 FROM csv",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.columns[1], "lag_v1");
    assert_eq!(r.columns[2], "lead_v1");
}

#[test]
fn window_no_partition_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // Entire table is one partition
    let r = unwrap_query(
        session
            .execute("SELECT v1, ROW_NUMBER() OVER (ORDER BY v1) as rn FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    // rn should range from 1 to 20
    let mut rns: Vec<i64> = (0..20).map(|i| r.table.get_i64(1, i).unwrap()).collect();
    rns.sort();
    assert_eq!(rns, (1..=20).collect::<Vec<_>>());
}

#[test]
fn window_mixed_specs_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, id3, \
                 ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY id3) as rn_part, \
                 ROW_NUMBER() OVER (ORDER BY id3) as rn_all \
                 FROM csv ORDER BY id3",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 4);

    let mut per_id1_counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in 0..r.table.nrows() as usize {
        let id1 = r.table.get_str(0, row).unwrap().to_string();
        let rn_part = r.table.get_i64(2, row).unwrap();
        let rn_all = r.table.get_i64(3, row).unwrap();

        // ORDER BY id3 over full table: row_number must be strictly 1..20.
        assert_eq!(rn_all, row as i64 + 1, "global row number mismatch at row {row}");

        // PARTITION BY id1: row_number must increment per id1 partition.
        let next = per_id1_counts.entry(id1).or_insert(0);
        *next += 1;
        assert_eq!(rn_part, *next, "partition row number mismatch at row {row}");
    }
}

#[test]
fn window_full_partition_avg_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // No ORDER BY → whole partition frame, AVG over entire partition
    let r = unwrap_query(
        session
            .execute("SELECT id1, v1, AVG(v1) OVER (PARTITION BY id1) as avg_v1 FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns[2], "avg_v1");

    // All rows in same partition should have same avg
    // id001: v1 = 1,2,3,4 → avg = 2.5
    for row in 0..r.table.nrows() as usize {
        let id1 = r.table.get_str(0, row).unwrap().to_string();
        let avg = r.table.get_f64(2, row).unwrap();
        if id1 == "id001" {
            assert!((avg - 2.5).abs() < 1e-10, "id001 avg should be 2.5, got {avg}");
        }
    }
}

#[test]
fn window_count_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT id1, COUNT(v1) OVER (PARTITION BY id1) as cnt FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    assert_eq!(r.columns.len(), 2);
    // Each id1 partition has 4 rows → count should be 4
    for row in 0..r.table.nrows() as usize {
        let cnt = r.table.get_i64(1, row).unwrap();
        assert_eq!(cnt, 4, "COUNT per partition should be 4, got {cnt}");
    }
}

#[test]
fn window_ntile_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute("SELECT v1, NTILE(4) OVER (ORDER BY v1) as tile FROM csv")
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    // NTILE(4) over 20 rows → 5 per tile, tiles 1-4
    for row in 0..r.table.nrows() as usize {
        let tile = r.table.get_i64(1, row).unwrap();
        assert!((1..=4).contains(&tile), "NTILE(4) should be 1..4, got {tile}");
    }
}

#[test]
fn window_first_last_value_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    let r = unwrap_query(
        session
            .execute(
                "SELECT id1, v1, FIRST_VALUE(v1) OVER (PARTITION BY id1 ORDER BY v1) as fv FROM csv",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    // FIRST_VALUE within each partition ordered by v1 → the min v1 in partition
    // id001: v1={1,2,3,4} → first_value = 1
    for row in 0..r.table.nrows() as usize {
        let id1 = r.table.get_str(0, row).unwrap().to_string();
        let fv = r.table.get_i64(2, row).unwrap();
        if id1 == "id001" {
            assert_eq!(fv, 1, "id001 FIRST_VALUE should be 1, got {fv}");
        }
    }
}

#[test]
fn window_expr_with_wildcard_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // SELECT *, ROW_NUMBER() OVER (...) <= 2 should produce original cols + bool column
    let r = unwrap_query(
        session
            .execute(
                "SELECT *, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) <= 2 FROM csv",
            )
            .unwrap(),
    );
    assert_eq!(r.table.nrows(), 20);
    // 9 original columns + 1 bool expression column = 10
    assert_eq!(r.columns.len(), 10, "expected 10 columns, got {:?}", r.columns);
    // Last column should be bool type (type code 1)
    let last_col = r.columns.len() - 1;
    assert_eq!(r.table.col_type(last_col), 1, "last column should be bool");
    // id001 has 4 rows; top-2 by DESC v1 (v1=4,3) → true; v1=2,1 → false
    let mut true_count = 0;
    for row in 0..r.table.nrows() as usize {
        let id1 = r.table.get_str(0, row).unwrap().to_string();
        let flag = r.table.get_i64(last_col, row).unwrap();
        if id1 == "id001" && flag != 0 {
            true_count += 1;
        }
    }
    assert_eq!(true_count, 2, "id001 should have exactly 2 rows with <= 2 = true");
}

#[test]
fn cte_column_alias_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();
    // CTE with aliased columns should preserve aliases for the outer query
    let r = unwrap_query(
        session
            .execute(
                "WITH w AS (SELECT id1, v1 + v2 AS total FROM csv) \
                 SELECT id1, total FROM w WHERE total > 10 ORDER BY total DESC LIMIT 5",
            )
            .unwrap(),
    );
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.columns[0], "id1");
    assert_eq!(r.columns[1], "total");
    // Verify values are correct (v1 + v2 > 10 means large rows)
    for row in 0..r.table.nrows() as usize {
        let total = r.table.get_i64(1, row).unwrap();
        assert!(total > 10, "total should be > 10, got {}", total);
    }

    // CTE with window function alias
    let r2 = unwrap_query(
        session
            .execute(
                "WITH w AS (SELECT id1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) AS rn FROM csv) \
                 SELECT * FROM w WHERE rn <= 2 ORDER BY id1, rn",
            )
            .unwrap(),
    );
    assert_eq!(r2.columns.len(), 2);
    assert_eq!(r2.columns[1], "rn");
    // 5 partitions (id001..id005) × 2 rows each = 10 rows
    assert_eq!(r2.table.nrows(), 10, "expected 10 rows from 5 partitions × top-2");
    for row in 0..r2.table.nrows() as usize {
        let rn = r2.table.get_i64(1, row).unwrap();
        assert!(rn <= 2, "rn should be <= 2, got {}", rn);
    }
}

#[test]
fn subquery_predicate_pushdown_sql() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let (mut session, _f) = setup_session();

    // Subquery without alias should work (aliasless derived table)
    let r = unwrap_query(
        session
            .execute(
                "SELECT * FROM (SELECT id1, v1 FROM csv) sub WHERE id1 = 'id001'",
            )
            .unwrap(),
    );
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.table.nrows(), 4, "id001 has 4 rows");
    for row in 0..r.table.nrows() as usize {
        assert_eq!(r.table.get_str(0, row).unwrap(), "id001");
    }

    // Predicate pushdown: equality on PARTITION BY key pushed into subquery
    // The result must be identical whether or not pushdown happens
    let r2 = unwrap_query(
        session
            .execute(
                "SELECT * FROM (\
                     SELECT *, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) <= 2 AS top2 \
                     FROM csv\
                 ) sub WHERE id1 = 'id001'",
            )
            .unwrap(),
    );
    // id001 has 4 rows; top2 = true for the 2 with highest v1 (v1=4, v1=3)
    assert_eq!(r2.table.nrows(), 4, "id001 has 4 rows after pushdown");
    let mut true_count = 0;
    for row in 0..r2.table.nrows() as usize {
        assert_eq!(r2.table.get_str(0, row).unwrap(), "id001");
        let top2 = r2.table.get_i64(r2.columns.len() - 1, row).unwrap();
        if top2 != 0 {
            true_count += 1;
        }
    }
    assert_eq!(true_count, 2, "exactly 2 rows should have top2=true");

    // Non-partition-key predicates stay in outer WHERE (not pushed)
    let r3 = unwrap_query(
        session
            .execute(
                "SELECT * FROM (\
                     SELECT *, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) AS rn \
                     FROM csv\
                 ) sub WHERE id1 = 'id001' AND rn <= 2",
            )
            .unwrap(),
    );
    assert_eq!(r3.table.nrows(), 2, "top-2 rows of id001 partition");
    for row in 0..r3.table.nrows() as usize {
        assert_eq!(r3.table.get_str(0, row).unwrap(), "id001");
        let rn = r3.table.get_i64(r3.columns.len() - 1, row).unwrap();
        assert!(rn <= 2, "rn should be <= 2, got {}", rn);
    }

    // Aliasless subquery (no AS sub) should also work
    let r4 = unwrap_query(
        session
            .execute(
                "SELECT * FROM (\
                     SELECT *, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) <= 2 AS top2 \
                     FROM csv\
                 ) WHERE id1 = 'id002'",
            )
            .unwrap(),
    );
    assert_eq!(r4.table.nrows(), 4, "id002 has 4 rows");
    for row in 0..r4.table.nrows() as usize {
        assert_eq!(r4.table.get_str(0, row).unwrap(), "id002");
    }
}

#[test]
#[ignore]
fn bench_subquery_window_filter() {
    let _guard = ENGINE_LOCK.lock().unwrap();
    let mut session = setup_bench_groupby().expect("10M CSV not found");

    let sql = "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1 DESC) <= 3 FROM t) WHERE id1 = 'id001' AND id2 = 'id085' AND id3 = 'id000094499'";

    // Warmup
    let _ = session.execute(sql).unwrap();

    // Timed runs
    let mut times = Vec::new();
    for _ in 0..5 {
        let t = std::time::Instant::now();
        let r = unwrap_query(session.execute(sql).unwrap());
        times.push(t.elapsed());
        assert_eq!(r.table.nrows(), 1);
    }
    times.sort();
    let median = times[times.len() / 2];
    let min = times[0];
    eprintln!("bench_subquery_window_filter: min={:?} median={:?}", min, median);
}
