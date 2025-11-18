-- Exercises LATERAL derived tables per SQL:2011 Part 2, 7.7 <lateral derived table> to correlate joins with base rows.
-- Each employee row drives a correlated lookup that must expose exactly one department.
SELECT e.id AS employee_id,
       dept.department
FROM employees e
CROSS JOIN LATERAL (
  SELECT d.department
  FROM employee_department ed
  JOIN departments d ON d.id = ed.department
  WHERE ed.employee = e.id
) AS dept
ORDER BY e.id;
