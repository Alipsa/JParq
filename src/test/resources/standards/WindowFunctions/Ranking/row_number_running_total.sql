-- Validates ROW_NUMBER and cumulative SUM window frames per SQL:2011 Part 2, 7.11 <window clause>.
-- Ordering by change_date ensures deterministic progression within each employee partition.
SELECT s.employee,
       s.change_date,
       s.salary,
       ROW_NUMBER() OVER (PARTITION BY s.employee ORDER BY s.change_date) AS row_num,
       SUM(s.salary) OVER (
         PARTITION BY s.employee
         ORDER BY s.change_date
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_total
FROM salary s
WHERE s.employee IN (1, 3)
ORDER BY s.employee, s.change_date;
