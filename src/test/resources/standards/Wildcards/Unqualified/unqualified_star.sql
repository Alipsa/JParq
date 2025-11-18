-- Validates unqualified SELECT * projection ordering per SQL:2011 Part 2, 7.2 <table primary>.
-- Employee rows are ordered by the primary key to make the expectation deterministic.
SELECT *
FROM employees
ORDER BY id;
