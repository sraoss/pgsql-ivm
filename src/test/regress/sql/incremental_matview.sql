
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

-- support SUM() and COUNT() aggregation function
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_agg AS SELECT i, SUM(j), COUNT(i)  FROM mv_base_a GROUP BY i;
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
UPDATE mv_base_a SET j = 200 WHERE (i,j) = (2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
DELETE FROM mv_base_a WHERE (i,j) = (2,200);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
ROLLBACK;

-- unsupport aggregation function except for SUM(),COUNT()
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_min AS SELECT i, MIN(j)  FROM mv_base_a GROUP BY i;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_max AS SELECT i, MAX(j)  FROM mv_base_a GROUP BY i;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_avg AS SELECT i, AVG(j)  FROM mv_base_a GROUP BY i;

-- known issues: When use COUNT(*), the result of COUNT(*) may not be correct.
BEGIN;
CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_agg AS SELECT i, SUM(j),COUNT(*)  FROM mv_base_a GROUP BY i;
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
ROLLBACK;


DROP TABLE mv_base_b CASCADE;
DROP TABLE mv_base_a CASCADE;
