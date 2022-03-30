CREATE EXTENSION pg_ivm;

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

-- CREATE INCREMENTAL MATERIALIZED VIEW mv_ivm_1 AS SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) WITH NO DATA;
SELECT create_immv('mv_ivm_1', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i)');
-- SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
-- REFRESH MATERIALIZED VIEW mv_ivm_1;
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;

-- immediate maintenance
-- BEGIN;
-- INSERT INTO mv_base_b VALUES(5,105);
-- SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
-- UPDATE mv_base_a SET j = 0 WHERE i = 1;
-- SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
-- DELETE FROM mv_base_b WHERE (i,k) = (5,105);
-- SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
-- ROLLBACK;
-- SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;

-- rename of IVM columns

-- unique index on IVM columns

-- some query syntax

-- result of materialized view have DISTINCT clause or the duplicate result.

-- support SUM(), COUNT() and AVG() aggregate functions
SELECT create_immv('mv_ivm_agg', 'SELECT i, SUM(j), COUNT(i), AVG(j) FROM mv_base_a GROUP BY i');

-- support COUNT(*) aggregate function

-- support aggregate functions without GROUP clause

-- resolved issue: When use AVG() function and values is indivisible, result of AVG() is incorrect.

-- support MIN(), MAX() aggregate functions

-- support MIN(), MAX() aggregate functions without GROUP clause

-- support self join view and multiple change on the same table

-- support simultaneous table changes

-- support foreign reference constraints

-- support subquery for using EXISTS()

-- support simple subquery in FROM clause

-- support join subquery in FROM clause

-- support simple CTE

-- views including NULL

-- support outer joins

-- 3-way outer join (full & full)

-- 3-way outer join (full & full) with DISTINCT

-- 3-way outer join (full & left)

-- 3-way outer join (full & right)

-- 3-way outer join (full & inner)

-- 3-way outer join (left & full)

-- 3-way outer join (left & left)

-- 3-way outer join (left & right)

-- 3-way outer join (left & inner)

-- 3-way outer join (right & full)

-- 3-way outer join (right & left)

-- 3-way outer join (right & right)

-- 3-way outer join (right & inner)

-- 3-way outer join (inner & full)

-- 3-way outer join (inner & left)

-- 3-way outer join (inner & right)

-- outer join with WHERE clause

-- self outer join

-- support simultaneous table changes on outer join

-- multiple change of the same table on outer join

-- IMMV containing user defined type

-- outer join view's targetlist must contain vars in the join conditions

-- outer join view's targetlist cannot contain non strict functions

-- outer join supports only simple equijoin

-- outer join view's WHERE clause cannot contain non null-rejecting predicates

-- aggregate is not supported with outer join

-- subquery is not supported with outer join

-- contain system column
SELECT create_immv('mv_ivm01', 'SELECT i,j,xmin FROM mv_base_a');
SELECT create_immv('mv_ivm02', 'SELECT i,j FROM mv_base_a WHERE xmin = ''610''');
SELECT create_immv('mv_ivm03', 'SELECT i,j,xmin::text AS x_min FROM mv_base_a');
SELECT create_immv('mv_ivm04', 'SELECT i,j,xidsend(xmin) AS x_min FROM mv_base_a');

-- targetlist or WHERE clause without EXISTS contain subquery
SELECT create_immv('mv_ivm05', 'SELECT i,j FROM mv_base_a WHERE i IN (SELECT i FROM mv_base_b WHERE k < 103)');
SELECT create_immv('mv_ivm06', 'SELECT i,j, (SELECT k FROM mv_base_b b WHERE a.i = b.i) FROM mv_base_a a');

-- contain ORDER BY
SELECT create_immv('mv_ivm07', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) ORDER BY i,j,k');
-- contain HAVING
SELECT create_immv('mv_ivm08', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) GROUP BY i,j,k HAVING SUM(i) > 5');

-- contain view or materialized view

-- contain mutable functions

-- LIMIT/OFFSET is not supported

-- DISTINCT ON is not supported

-- TABLESAMPLE clause is not supported

-- window functions are not supported

-- aggregate function with some options is not supported

-- inheritance parent is not supported

-- UNION statement is not supported

-- DISTINCT clause in nested query are not supported

-- empty target list is not allowed with IVM

-- FOR UPDATE/SHARE is not supported

-- tartget list cannot contain ivm column that start with '__ivm'



DROP TABLE mv_base_b CASCADE;
DROP TABLE mv_base_a CASCADE;
