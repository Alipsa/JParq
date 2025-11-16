-- Asserts table-qualified wildcard expansion order per SQL:2011 Part 2, 7.2 <table primary>.
-- Joined departments must follow the employee columns emitted by e.*.
SELECT e.*, d.department
FROM employees e
JOIN employee_department ed ON e.id = ed.employee
JOIN departments d ON d.id = ed.department
ORDER BY e.id;
