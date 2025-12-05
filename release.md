# Version history

### 1.1.0, in progress
- create a fat jar so the driver can be used in tools that require adding a jar to the classpath
- improved schema support
  - parquet files in the root dir will have the schema PUBLIC
  - subdirectories correspond to a schema
- Improve test coverage to >= 80%
- Improve metadata support and accuracy
- refactored function support for increased coherence and reduced coupling
- add function mapping from jdbc escape syntax to sql standard function names for numeric, string, datetime, and system functions
- several minor bug fixes and improvements
- Improved support for quoted identifiers throughout the codebase

### 1.0.0, 2025-11-24
- Additional Derived Tables support: 
  - LATERAL derived tables
  - VALUES table constructors
  - Support TABLE wrapper for UNNEST table functions
- Complete set-operation coverage.
  - INTERSECT ALL
  - EXCEPT ALL
  - Support for nested set operations
- Add simple schema validation
- Bug fixes to UNNEST, CTE, EXCEPT
- Add SQL compliance test suite to verify that all works 
- support || operator for string and binary concatenations
- support for explicit NULLS FIRST and NULLS LAST in ORDER BY

### 0.11.0, 2025-11-14
- Add ARRAY constructor function support
- Fix column names and types in ResultSetMetaData
- Adhere JParqDatabaseMetaData specifications for getColumns and getTables methods to the JDBC standard
- Add support for derived tables
  - UNNEST with and without ordinality

### 0.10.0, 2025-11-12
- Add support for grouping sets, rollup, and cube in the GROUP BY clause
- Add support for qualified wildcards (table.*) in SELECT statements
- Add support for the ARRAY constructor function to create array literals

### 0.9.0, 2025-11-09
- Add support for Windowing functions i.e:
   - Ranking functions
      - ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE
   -  Aggregate window functions
      - SUM, AVG, MIN, MAX, COUNT
   - Analytic Value/Navigation Functions
      -  LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- Add support for JOIN USING clause

### 0.8.0, 2025-11-02
- Add support for CTE (Common Table Expressions)

### 0.7.0, 2025-11-02
- Add support for self joins.
- Add support for UNION, and UNION ALL
- Add support for INTERSECT and EXCEPT

## 0.6.0, 2025-11-01
- Add support for INNER, LEFT, RIGHT, FULL, and CROSS Join

## 0.5.0, 2025-10-31
The following Additional SQL statements are now supported:
- `OFFSET` and `LIMIT` support
- `DISTINCT` support
- Functions support
  - Date functions
  - Aggregate functions (count, sum, avg, max, min)
  - CAST support
  - coalesce (The COALESCE() function returns the first non-null value in a list.)
  - String functions (all SQL standard string function supported)
  - Numeric functions (abs, ceil, floor, round, sqrt, truncate, mod, power, exp, log, rand, sign, sin, cos, tan, asin, acos, atan, atan2, degrees, radians)
- comments (line --) and block (/* */)
- Subquery support
  - In the SELECT, FROM, WHERE, and HAVING Clause
- `GROUP BY` support
  - `COUNT(*)` aggregation
  - `HAVING` clause with conditions
  - support aggregation functions and case statements in the `GROUP BY` and `SELECT` clause
- exists support 
- any and all support

## 0.1.0, 2025-10-25
The following SQL statements are supported:
- `SELECT` with support for
  - `*` to select all columns
  - alias support for columns and tables
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators
  - `AND`, `OR`, `NOT` logical operators
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Null checks: `IS NULL`, `IS NOT NULL`
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options