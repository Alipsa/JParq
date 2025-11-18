-- Ensures GROUP BY expressions that use COALESCE project correctly when reused in the SELECT list.
-- The query exercises the structural expression matcher by repeating the logic with different formatting.
SELECT COALESCE( d.department , 'UNASSIGNED') AS department_label,
       COUNT(*) AS employee_count
FROM employees e
JOIN employee_department ed ON ed.employee = e.id
JOIN departments d ON d.id = ed.department
GROUP BY COALESCE(d.department, 'UNASSIGNED')
ORDER BY department_label;
