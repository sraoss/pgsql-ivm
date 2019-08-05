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

-- contain system column
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm01 AS SELECT i,j,xmin FROM mv_base_a;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm02 AS SELECT i,j FROM mv_base_a WHERE xmin = '610';
-- contain subquery
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm03 AS SELECT i,j FROM mv_base_a WHERE i IN (SELECT i FROM mv_base_b WHERE k < 103 );
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm04 AS SELECT a.i,a.j FROM mv_base_a a,( SELECT * FROM mv_base_b) b WHERE a.i = b.i;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm05 AS SELECT i,j, (SELECT k FROM mv_base_b b WHERE a.i = b.i) FROM mv_base_a a;
-- contain CTE
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm06 AS WITH b AS (SELECT i,k FROM mv_base_b WHERE k < 103) SELECT a.i,a.j FROM mv_base_a a,b WHERE a.i = b.i;
-- contain view or materialized view
CREATE VIEW b_view AS SELECT i,k FROM mv_base_b;
CREATE MATERIALIZED VIEW b_mview AS SELECT i,k FROM mv_base_b;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm07 AS SELECT a.i,a.j FROM mv_base_a a,b_view b WHERE a.i = b.i;
CREATE INCREMENTAL MATERIALIZED VIEW  mv_ivm08 AS SELECT a.i,a.j FROM mv_base_a a,b_mview b WHERE a.i = b.i;

DROP TABLE mv_base_b CASCADE;
DROP TABLE mv_base_a CASCADE;
