-- Covers INTERSECT ALL and EXCEPT ALL nesting per SQL:2011 Part 2, 7.13 <query expression>.
-- Duplicate-sensitive set arithmetic is expressed through inline value constructors.
WITH dept_a(department_id) AS (
  SELECT * FROM (VALUES (1), (1), (2), (3))
),
dept_b(department_id) AS (
  SELECT * FROM (VALUES (1), (2), (2), (4))
),
dept_c(department_id) AS (
  SELECT * FROM (VALUES (1), (3))
),
dept_intersection AS (
  SELECT department_id FROM dept_a
  INTERSECT ALL
  SELECT department_id FROM dept_b
)
SELECT department_id
FROM dept_intersection
EXCEPT ALL
SELECT department_id FROM dept_c
ORDER BY department_id;
