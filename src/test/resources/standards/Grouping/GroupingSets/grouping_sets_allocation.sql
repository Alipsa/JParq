-- Validates GROUPING SETS with parallel GROUPING() flags per SQL:2011 Part 2, 7.3 <grouping operations>.
-- The projection exposes department-only, employee-only and grand-total groupings in a deterministic order.
SELECT d.department,
       e.first_name,
       GROUPING(d.department) AS department_group,
       GROUPING(e.first_name) AS name_group,
       SUM(s.salary) AS total_salary,
       COALESCE(d.department, e.first_name, 'TOTAL') AS sort_key
FROM employees e
JOIN employee_department ed ON e.id = ed.employee
JOIN departments d ON d.id = ed.department
JOIN salary s ON s.employee = e.id
WHERE e.id IN (1, 3, 4)
GROUP BY GROUPING SETS (
  (d.department),
  (e.first_name),
  ()
)
ORDER BY department_group,
         name_group,
         sort_key;
