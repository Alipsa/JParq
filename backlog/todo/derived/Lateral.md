**Please add support for LATERAL derived table described in the SQL:1999 standard.**

A LATERAL derived table is a specific type of derived table (subquery in the FROM clause) defined in the SQL standard (since SQL:1999) that allows the subquery to reference (or depend on) columns from the table(s) that appear before it in the FROM clause.

# Standard Behavior vs. LATERAL
## Standard Derived Table (Subquery)
By default, subqueries in the FROM clause are evaluated independently and cannot refer to columns from tables defined earlier in the same FROM clause.

SELECT t1.A, sub.B
FROM Table1 t1,
     (SELECT t2.B FROM Table2 t2 WHERE t2.C = 1) AS sub;
-- 'sub' cannot use columns from 't1' in its WHERE clause.

## LATERAL Derived Table
The LATERAL keyword breaks this rule, enabling correlation. It is syntactically equivalent to a JOIN but allows for row-by-row processing, acting like a procedural loop:

SELECT t1.A, sub.B
FROM Table1 t1
JOIN LATERAL (
    SELECT t2.B 
    FROM Table2 t2 
    WHERE t2.C = t1.A  -- **Correlation is allowed here**
    LIMIT 1            -- Often used to find a single matching row
) AS sub ON TRUE;
For every row processed in Table1 (t1), the database executes the LATERAL subquery (sub), passing the current value of t1.A to be used in the subquery's WHERE clause.

# Primary Use Cases
The LATERAL clause is primarily used to:
- Retrieve Top-N Per Group (Efficiently): It is highly efficient for finding the top few related rows for every row in the primary table, often by combining LATERAL with ORDER BY ... LIMIT N.
- Unnest Complex Data: It's used to process or "un-nest" functions that return multiple rows (like array or JSON functions in some databases) for each row of the input table.
- Replace Complex Joins: It can replace complex correlated subqueries in the SELECT list or non-standard APPLY operators (like SQL Server's CROSS APPLY).

# Important!
- Create test to verify the functionality in a test class called jparq.derived.LateralTest
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true test` to ensure that there is no regression.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
