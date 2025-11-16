-- Validates GROUP BY ROLLUP and GROUPING() per SQL:2011 Part 2, 7.3 <grouping operations>.
-- Aggregates departmental totals followed by the grand total row.
SELECT d.department,
       GROUPING(d.department) AS department_group,
       SUM(s.salary) AS total_salary
FROM departments d
JOIN employee_department ed ON ed.department = d.id
JOIN salary s ON s.employee = ed.employee
GROUP BY ROLLUP(d.department)
ORDER BY GROUPING(d.department), d.department;
