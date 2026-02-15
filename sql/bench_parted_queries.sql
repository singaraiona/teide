-- H2OAI Groupby Benchmark — Partitioned Table (10M rows)
--
-- Usage (REPL):
--   teide
--   > .timer on
--   > \i sql/bench_parted_queries.sql
--
-- Usage (file mode):
--   teide -f sql/bench_parted_queries.sql

-- q1: 1 low-cardinality key, 1 SUM
SELECT id1, SUM(v1) FROM '/tmp/teide_db/quotes' GROUP BY id1;

-- q2: 2 low-cardinality keys, 1 SUM
SELECT id1, id2, SUM(v1) FROM '/tmp/teide_db/quotes' GROUP BY id1, id2;

-- q3: 1 high-cardinality key, SUM + AVG
SELECT id3, SUM(v1), AVG(v3) FROM '/tmp/teide_db/quotes' GROUP BY id3;

-- q4: 1 medium-cardinality key, 3 AVGs
SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM '/tmp/teide_db/quotes' GROUP BY id4;

-- q5: 1 high-cardinality key, 3 SUMs
SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM '/tmp/teide_db/quotes' GROUP BY id6;

-- q6: 1 high-cardinality key, MAX + MIN
SELECT id3, MAX(v1), MIN(v2) FROM '/tmp/teide_db/quotes' GROUP BY id3;

-- q7: 6 keys, SUM + COUNT
SELECT id1, id2, id3, id4, id5, id6, SUM(v3), COUNT(v1)
  FROM '/tmp/teide_db/quotes'
  GROUP BY id1, id2, id3, id4, id5, id6;
