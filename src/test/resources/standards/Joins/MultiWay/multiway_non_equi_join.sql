-- Validates multi-way joins combined with a non-equality predicate per SQL:2011 Part 2, 7.7 <joined table>.
-- Salaries that meet the threshold should be returned while preserving unmatched employees through the LEFT JOIN.
SELECT e.id AS employee_id,
       e.first_name,
       d.department,
       s.salary
FROM employees e
JOIN employee_department ed ON e.id = ed.employee
JOIN departments d ON d.id = ed.department
LEFT JOIN salary s ON s.employee = e.id AND s.salary >= 165000
ORDER BY e.id, COALESCE(s.salary, 0);
