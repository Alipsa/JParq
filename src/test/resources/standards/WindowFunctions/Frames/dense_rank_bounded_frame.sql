-- Demonstrates DENSE_RANK with a bounded ROWS frame per SQL:2011 Part 2, 7.11 <window clause>.
-- The ORDER BY clause stabilizes ties using the salary identifier for deterministic window sums.
SELECT s.employee,
       s.salary,
       DENSE_RANK() OVER (ORDER BY s.salary DESC) AS dense_rank,
       SUM(s.salary) OVER (
         ORDER BY s.salary DESC, s.id DESC
         ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
       ) AS surrounding_sum
FROM salary s
WHERE s.employee BETWEEN 3 AND 5
ORDER BY s.salary DESC, s.id DESC;
