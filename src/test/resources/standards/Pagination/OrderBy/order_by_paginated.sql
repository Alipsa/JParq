-- Ensures ORDER BY is evaluated prior to OFFSET/FETCH per SQL:2011 Part 2, 10.11 <fetch first clause>.
-- Alphabetical sorting of first names is paginated to verify deterministic ordering semantics.
SELECT first_name
FROM employees
ORDER BY first_name
OFFSET 1 ROW FETCH NEXT 3 ROWS ONLY;
