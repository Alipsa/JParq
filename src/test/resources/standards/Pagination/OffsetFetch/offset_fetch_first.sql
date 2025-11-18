-- Validates OFFSET/FETCH interaction per SQL:2011 Part 2, 10.11 <fetch first clause>.
-- The query orders the deterministic employee identifier sequence before applying pagination.
SELECT id,
       first_name
FROM employees
ORDER BY id
OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY;
