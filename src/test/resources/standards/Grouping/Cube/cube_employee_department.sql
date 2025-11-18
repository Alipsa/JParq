-- Exercises GROUP BY CUBE across department and employee per SQL:2011 Part 2, 7.3 <grouping operations>.
-- The query limits the domain to two employees to keep the cube result set tractable.
SELECT ed.department,
       s.employee,
       GROUPING(ed.department) AS department_group,
       GROUPING(s.employee) AS employee_group,
       SUM(s.salary) AS total_salary
FROM employee_department ed
JOIN salary s ON s.employee = ed.employee
WHERE ed.employee IN (1, 2)
GROUP BY CUBE(ed.department, s.employee)
ORDER BY department_group, ed.department, employee_group, s.employee;
