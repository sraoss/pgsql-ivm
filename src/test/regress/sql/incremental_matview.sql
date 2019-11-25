-- create a table to use as a basis for views and materialized views in various combinations
CREATE TABLE mv_base_a (i int, j int);
INSERT INTO mv_base_a VALUES
  (1,10),
  (2,20),
  (3,30),
  (4,40),
  (5,50);
CREATE TABLE mv_base_b (i int, k int);
INSERT INTO mv_base_b VALUES
  (1,101),
  (2,102),
  (3,103),
  (4,104);

CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_1 AS SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i);

SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
-- immediaite maintenance
BEGIN;
INSERT INTO mv_base_b VALUES(5,105);
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
UPDATE mv_base_a SET j = 0 WHERE i = 1;
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
DELETE FROM mv_base_b WHERE (i,k) = (5,105);
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
ROLLBACK;
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;

-- result of materliazied view have DISTINCT clause or the duplicate result.
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_duplicate AS SELECT j FROM mv_base_a;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_distinct AS SELECT DISTINCT j FROM mv_base_a;
INSERT INTO mv_base_a VALUES(6,20);
SELECT * FROM mv_ivm_duplicate ORDER BY 1;
SELECT * FROM mv_ivm_distinct ORDER BY 1;
DELETE FROM mv_base_a WHERE (i,j) = (2,20);
SELECT * FROM mv_ivm_duplicate ORDER BY 1;
SELECT * FROM mv_ivm_distinct ORDER BY 1;
ROLLBACK;

-- support SUM(), COUNT() and AVG() aggregation function
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_agg AS SELECT i, SUM(j), COUNT(i),AVG(j)  FROM mv_base_a GROUP BY i;
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
UPDATE mv_base_a SET j = 200 WHERE (i,j) = (2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
DELETE FROM mv_base_a WHERE (i,j) = (2,200);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
ROLLBACK;

-- support COUNT(*) aggregation function
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_agg AS SELECT i, SUM(j),COUNT(*)  FROM mv_base_a GROUP BY i;
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
ROLLBACK;

-- support having only aggregation function without GROUP clause
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_group AS SELECT SUM(j)FROM mv_base_a;
SELECT * FROM mv_ivm_group ORDER BY 1;
INSERT INTO mv_base_a VALUES(6,20);
SELECT * FROM mv_ivm_group ORDER BY 1;
ROLLBACK;

-- resolved issue: When use AVG() function and values is indivisible, result of AVG() is incorrect.
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_avg_bug AS SELECT i, SUM(j), COUNT(j), AVG(j) FROM mv_base_A GROUP BY i;
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES
  (1,0),
  (1,0),
  (2,30),
  (2,30);
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
DELETE FROM mv_base_a WHERE (i,j) = (1,0);
DELETE FROM mv_base_a WHERE (i,j) = (2,30);
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
ROLLBACK;

-- support MIN(), MAX() aggregation functions
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_min_max AS SELECT i, MIN(j), MAX(j)  FROM mv_base_a GROUP BY i;
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES
  (1,11), (1,12),
  (2,21), (2,22),
  (3,31), (3,32),
  (4,41), (4,42),
  (5,51), (5,52);
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
DELETE FROM mv_base_a WHERE (i,j) IN ((1,10), (2,21), (3,32));
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
ROLLBACK;

-- support MIN(), MAX() aggregation functions without GROUP clause
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_min_max AS SELECT MIN(j), MAX(j)  FROM mv_base_a;
SELECT * FROM mv_ivm_min_max;
INSERT INTO mv_base_a VALUES
  (0,0), (6,60), (7,70);
SELECT * FROM mv_ivm_min_max;
DELETE FROM mv_base_a WHERE (i,j) IN ((0,0), (7,70));
SELECT * FROM mv_ivm_min_max;
ROLLBACK;

-- support self join view and multiple change on the same table
BEGIN;
CREATE TABLE t (i int, v int);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
CREATE INCREMENTAL MATERIALIZED VIEW mv_self(v1, v2) AS
 SELECT t1.v, t2.v FROM t t1 JOIN t t2 ON t1.i = t2.i;
SELECT * FROM mv_self ORDER BY v1;
INSERT INTO t VALUES (4,40);
DELETE FROM t WHERE i = 1;
UPDATE t SET v = v*10 WHERE i=2;
SELECT * FROM mv_self ORDER BY v1;
WITH
 ins_t1 AS (INSERT INTO t VALUES (5,50) RETURNING 1),
 ins_t2 AS (INSERT INTO t VALUES (6,60) RETURNING 1),
 upd_t AS (UPDATE t SET v = v + 100  RETURNING 1),
 dlt_t AS (DELETE FROM t WHERE i IN (4,5)  RETURNING 1)
SELECT NULL;
SELECT * FROM mv_self ORDER BY v1;
ROLLBACK;

-- support simultaneous table changes
BEGIN;
CREATE TABLE r (i int, v int);
CREATE TABLE s (i int, v int);
INSERT INTO r VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO s VALUES (1, 100), (2, 200), (3, 300);
CREATE INCREMENTAL MATERIALIZED VIEW mv(v1, v2) AS
 SELECT r.v, s.v FROM r JOIN s USING(i);
SELECT * FROM mv ORDER BY v1;
WITH
 ins_r AS (INSERT INTO r VALUES (1,11) RETURNING 1),
 ins_r2 AS (INSERT INTO r VALUES (3,33) RETURNING 1),
 ins_s AS (INSERT INTO s VALUES (2,222) RETURNING 1),
 upd_r AS (UPDATE r SET v = v + 1000 WHERE i = 2 RETURNING 1),
 dlt_s AS (DELETE FROM s WHERE i = 3 RETURNING 1)
SELECT NULL;
SELECT * FROM mv ORDER BY v1;
ROLLBACK;

-- support foreign reference constrains
BEGIN;
CREATE TABLE ri1 (i int PRIMARY KEY);
CREATE TABLE ri2 (i int PRIMARY KEY REFERENCES ri1(i) ON UPDATE CASCADE ON DELETE CASCADE, v int);
INSERT INTO ri1 VALUES (1),(2),(3);
INSERT INTO ri2 VALUES (1),(2),(3);
CREATE INCREMENTAL MATERIALIZED VIEW mv_ri(i1, i2) AS
 SELECT ri1.i, ri2.i FROM ri1 JOIN ri2 USING(i);
SELECT * FROM mv_ri ORDER BY i1;
UPDATE ri1 SET i=10 where i=1;
DELETE FROM ri1 WHERE i=2;
SELECT * FROM mv_ri ORDER BY i2;
ROLLBACK;
-- support subquery for using EXISTS()
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_exists_subquery AS SELECT a.i, a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i);
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery ORDER BY i, j;
INSERT INTO mv_base_a VALUES(1,10),(6,60),(3,30),(3,300);
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery ORDER BY i, j;
INSERT INTO mv_base_b VALUES(1,101);
INSERT INTO mv_base_b VALUES(1,111);
INSERT INTO mv_base_b VALUES(2,102);
INSERT INTO mv_base_b VALUES(6,106);
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery ORDER BY i, j;
UPDATE mv_base_a SET i = 1 WHERE j =60;
UPDATE mv_base_b SET i = 10  WHERE k = 101;
UPDATE mv_base_b SET k = 1002 WHERE k = 102;
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery ORDER BY i, j;
DELETE FROM mv_base_a WHERE (i,j) = (1,60);
DELETE FROM mv_base_b WHERE i = 2;
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery ORDER BY i, j;
ROLLBACK;

BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_exists_subquery2 AS SELECT a.i, a.j FROM mv_base_a a WHERE i >= 3 AND EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i);
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_exists_subquery3 AS SELECT a.i, a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) AND EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i + 100 = b.k);
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery2 ORDER BY i, j;
SELECT *,__ivm_count__, __ivm_exists_count_0__, __ivm_exists_count_1__ FROM mv_ivm_exists_subquery3 ORDER BY i, j;

INSERT INTO mv_base_b VALUES(1,101);
UPDATE mv_base_b SET k = 200  WHERE i = 2;
INSERT INTO mv_base_a VALUES(1,10);
INSERT INTO mv_base_a VALUES(3,30);
INSERT INTO mv_base_b VALUES(3,300);
SELECT *,__ivm_count__, __ivm_exists_count_0__ FROM mv_ivm_exists_subquery2 ORDER BY i, j;
SELECT *,__ivm_count__, __ivm_exists_count_0__, __ivm_exists_count_1__ FROM mv_ivm_exists_subquery3 ORDER BY i, j;
ROLLBACK;
-- support simple subquery in FROM cluase
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm_subquery AS SELECT a.i,a.j FROM mv_base_a a,( SELECT * FROM mv_base_b) b WHERE a.i = b.i;
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_ivm_subquery ORDER BY i,j;

ROLLBACK;

-- views including NULL
BEGIN;
CREATE TABLE t (i int, v int);
INSERT INTO t VALUES (1,10),(2, NULL);
CREATE INCREMENTAL MATERIALIZED VIEW mv AS SELECT * FROM t;
SELECT * FROM mv ORDER BY i;
UPDATE t SET v = 20 WHERE i = 2;
SELECT * FROM mv ORDER BY i;
ROLLBACK;

-- support outer joins
BEGIN;
CREATE TABLE r (i int);
CREATE TABLE s (i int, j int);
CREATE TABLE t (j int);
INSERT INTO r VALUES (1), (2), (3), (3);
INSERT INTO s VALUES (2,1), (2,2), (3,1), (4,1), (4,2);
INSERT INTO t VALUES (2), (3), (3);

CREATE FUNCTION is_match() RETURNS text AS $$
DECLARE
x text;
BEGIN
 EXECUTE
 'SELECT CASE WHEN count(*) = 0 THEN ''OK'' ELSE ''NG'' END FROM (
	SELECT * FROM (SELECT * FROM mv EXCEPT ALL SELECT * FROM v) v1
	UNION ALL
 SELECT * FROM (SELECT * FROM v EXCEPT ALL SELECT * FROM mv) v2
 ) v' INTO x;
 RETURN x;
END;
$$ LANGUAGE plpgsql;

-- 3-ways outer join (full & full)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
SELECT * FROM mv ORDER BY r, si, sj, t;
SAVEPOINT p1;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (full & left)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (full & right)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (full & inner)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i INNER JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r FULL JOIN s ON r.i=s.i INNER JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (left & full)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (left & left)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (left & right)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (left & inner)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i INNER JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r LEFT JOIN s ON r.i=s.i INNER JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (right & full)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (right & left)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (right & right)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r RIGHT JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (inner & FULL)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i FULL JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (inner & left)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i LEFT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- 3-ways outer join (inner & right)
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM r INNER JOIN s ON r.i=s.i RIGHT JOIN t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
INSERT INTO t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
DELETE FROM t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- outer join with WHERE clause
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i WHERE s.i > 0;
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i WHERE s.i > 0;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

INSERT INTO s VALUES (1,3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
INSERT INTO s VALUES (2,3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM s WHERE i=2;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
DELETE FROM s WHERE i=3;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
DELETE FROM s WHERE i=4;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- self outer join
CREATE INCREMENTAL MATERIALIZED VIEW mv(r1, r2) AS
 SELECT r.i, r2.i
   FROM r FULL JOIN r as r2 ON r.i=r2.i;
CREATE VIEW v(r1, r2) AS
 SELECT r.i, r2.i
   FROM r FULL JOIN r as r2 ON r.i=r2.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r1, r2;

INSERT INTO r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match();
INSERT INTO r VALUES (4),(5);
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match();
ROLLBACK TO p1;

DELETE FROM r WHERE i=1;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match();
DELETE FROM r WHERE i=2;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match();
DELETE FROM r WHERE i=3;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- support simultaneous table changes on outer join
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i;
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

WITH
 ri AS (INSERT INTO r VALUES (1),(2),(3) RETURNING 0),
 si AS (INSERT INTO s VALUES (1,3) RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

WITH
 rd AS (DELETE FROM r WHERE i=1 RETURNING 0),
 sd AS (DELETE FROM s WHERE i=2 RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

-- multiple change of the same table on outer join
CREATE INCREMENTAL MATERIALIZED VIEW mv(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i;
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM r FULL JOIN s ON r.i=s.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

WITH
 ri1 AS (INSERT INTO r VALUES (1),(2),(3) RETURNING 0),
 ri2 AS (INSERT INTO r VALUES (4),(5) RETURNING 0),
 rd AS (DELETE FROM r WHERE i IN (3,4) RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match();
ROLLBACK TO p1;

DROP MATERIALIZED VIEW mv;
DROP VIEW v;

ROLLBACK;

-- outer join view's targetlist must contain vars in the join conditions
CREATE INCREMENTAL MATERIALIZED VIEW mv AS SELECT a.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i;
CREATE INCREMENTAL MATERIALIZED VIEW mv AS SELECT a.i,j,k FROM mv_base_a a LEFT JOIN mv_base_b b USING(i);

-- outer join view's targetlist cannot contain non strict functions
CREATE INCREMENTAL MATERIALIZED VIEW mv AS SELECT a.i, b.i, (k > 10 OR k = -1) FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i;
CREATE INCREMENTAL MATERIALIZED VIEW mv AS SELECT a.i, b.i, CASE WHEN k>0 THEN 1 ELSE 0 END FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i;

-- outer join supports only simple equijoin
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b) AS SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i>b.i;
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b,k,j) AS SELECT a.i, b.i, k j FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i AND k=j;

-- outer join view's WHERE clause cannot contain non null-rejecting predicates
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b) AS SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE k IS NULL;
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b) AS SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE (k > 10 OR k = -1);

-- aggregate is not supported with outer join
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b,v) AS SELECT a.i, b.i, sum(k) FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i GROUP BY a.i, b.i;

-- subquery is not supported with outer join
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b) AS SELECT a.i, b.i FROM mv_base_a a LEFT JOIN (SELECT * FROM mv_base_b) b ON a.i=b.i;
CREATE INCREMENTAL MATERIALIZED VIEW mv(a,b) AS SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE EXISTS (SELECT 1 FROM mv_base_b b2 WHERE a.j = b.k);

-- contain system column
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm01 AS SELECT i,j,xmin FROM mv_base_a;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm02 AS SELECT i,j FROM mv_base_a WHERE xmin = '610';
-- targetlist or WHERE clause without EXISTS contain subquery
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm03 AS SELECT i,j FROM mv_base_a WHERE i IN (SELECT i FROM mv_base_b WHERE k < 103 );
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm05 AS SELECT i,j, (SELECT k FROM mv_base_b b WHERE a.i = b.i) FROM mv_base_a a;
-- contain CTE
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm06 AS WITH b AS (SELECT i,k FROM mv_base_b WHERE k < 103) SELECT a.i,a.j FROM mv_base_a a,b WHERE a.i = b.i;
-- contain ORDER BY
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm07 AS SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) ORDER BY i,j,k;
-- contain HAVING
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm08 AS SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) GROUP BY i,j,k HAVING SUM(i) > 5;

-- contain view or materialized view
CREATE VIEW b_view AS SELECT i,k FROM mv_base_b;
CREATE MATERIALIZED VIEW b_mview AS SELECT i,k FROM mv_base_b;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm07 AS SELECT a.i,a.j FROM mv_base_a a,b_view b WHERE a.i = b.i;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm08 AS SELECT a.i,a.j FROM mv_base_a a,b_mview b WHERE a.i = b.i;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm09 AS SELECT a.i,a.j FROM mv_base_a a, (SELECT i, COUNT(*) FROM mv_base_b GROUP BY i) b WHERE a.i = b.i;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm10 AS SELECT a.i,a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) OR a.i > 5;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm11 AS SELECT a.i,a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE EXISTS(SELECT 1 FROM mv_base_b c WHERE b.i = c.i));

DROP TABLE mv_base_b CASCADE;
DROP TABLE mv_base_a CASCADE;
