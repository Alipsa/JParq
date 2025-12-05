**Please add support for JOIN ... USING described in the SQL:1999 standard.**
The primary purpose of JOIN ... USING is to avoid the redundancy of qualifying identically named columns in a join condition.

Instead of specifying a full join condition with ON, you simply list the common column name(s) inside the parentheses of USING.

## Syntax: 
```sql  
  SELECT ...
  FROM TableA [INNER | LEFT | RIGHT | FULL] JOIN TableB
  USING (column_name_1 [, column_name_2, ...])
```
## Key Standard Feature: Column Projection
-The standard treats a JOIN ... USING (col) as logically equivalent to JOIN ... ON TableA.col = TableB.col, but with special handling for the output column list.
- Equivalence: The expression JOIN TableB USING (column_name) is logically equivalent to the more verbose JOIN TableB ON TableA.column_name = TableB.column_name.
- The most distinctive feature of JOIN ... USING, according to the standard, is how it handles the result columns:
  - The common join column (column_name) appears only once in the final result set.
  - In contrast, a standard JOIN ... ON retains both instances of the join column (e.g., TableA.column_name and TableB.column_name), requiring you to alias one of them if you want a clean final output.
  - The USING clause is standard and valid with all types of cross-product joins (inner, left, right, full).

# Example
Imagine two tables, Employees and Departments, both having a column named dept_id.

| Table       | Column Names                   |
|-------------|--------------------------------|
| Employees   | employee_name, dept_id, salary |
| Departments | dept_id, dept_name, location   |

## Sample Data:
### Employees Table:

| employee_name | dept_id | salary |
|---------------|---------|--------|
| Alice         | 10      | 60000  |
| Bob           | 20      | 75000  |
| Charlie       | 10      | 62000  |
| David         | 30      | 50000  |

### Table: Departments

| dept_id | dept_name | location |
|---------|-----------|----------|
| 10      | Marketing | London   |
| 20      | IT        | Berlin   |
| 30      | HR        | Paris    |
| 40      | Finance   | Rome     |

## Standard JOIN ... USING Query
We want to join the tables and select all columns.
Goal: Join the tables on dept_id. Crucially, the standard requires the dept_id column to appear only once in the output, even though we selected *.
```sql
SELECT *
FROM Employees
INNER JOIN Departments
USING (dept_id);
```
dept_id,employee_name,salary,dept_name,location
10,Alice,60000,Marketing,London
20,Bob,75000,IT,Berlin
10,Charlie,62000,Marketing,London
30,David,50000,HR,Paris

If you use SELECT *, the columns will appear in the following standard order:
- dept_id (The single merged column)
- employee_name
- salary
- dept_name
- location

Notice that dept_id = 40 (Finance) is excluded because the INNER JOIN only returns rows where a match exists in both tables. The result set only includes a single dept_id column, as mandated by the JOIN ... USING syntax in the SQL standard.

### Comparison to JOIN ... ON
The equivalent query using the ON clause would require you to explicitly exclude one of the redundant dept_id columns if you wanted a clean result:
```sql
-- Non-standard compliant systems often require column selection to clean up the output
SELECT
    E.dept_id,
    E.employee_name,
    E.salary,
    D.dept_name,
    D.location
FROM Employees E
INNER JOIN Departments D
ON E.dept_id = D.dept_id;
```

# Important!
- Create test to verify the functionality in a test class called jparq.join.JoinUsingTest.java in the src/test/jparq/join directory.
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression.
- No checkstyle, PMD or Spotless violations shall be present after the implementation.