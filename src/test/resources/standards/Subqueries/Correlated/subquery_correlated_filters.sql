-- Exercises correlated and non-correlated subqueries in SELECT, WHERE and FROM clauses per SQL:2011 Part 2, 7.6 <subquery>.
-- Only employees with salaries >= 180000 are expected, alongside their department and salary history counts.
SELECT derived.employee_id,
       (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name,
       (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
FROM (
  SELECT ed.employee AS employee_id, ed.department AS department_id
  FROM employee_department ed
  WHERE ed.employee IN (
    SELECT employee
    FROM salary
    WHERE salary >= 180000
  )
) AS derived
WHERE EXISTS (
  SELECT 1
  FROM salary s
  WHERE s.employee = derived.employee_id
    AND s.salary >= 180000
)
ORDER BY derived.employee_id;
