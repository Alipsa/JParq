# Version history

### 1.4.0
- Support Hugging Face optimized Parquet files written with content-defined chunking and page indexes, verified with committed PyArrow 21.0.0 fixtures and physical page-pruning tests.
- Fix predicate pushdown for SQL NULL negations, date and timestamp bounds, column-name casing, fractional integer bounds, and out-of-range numeric literals.
- Push supported `AND` terms independently, add NULL checks and string-prefix `LIKE` ranges, and support logical-type bounds in `BETWEEN` and `IN`.
- Enable predicate pushdown with `DISTINCT` and safe single-table join conditions; retain the full post-join filter.
- Read only required columns with `ORDER BY`, retaining sort, alias, ordinal, and correlated-subquery dependencies.
- Read nanosecond and local timestamps and float16 values; preserve logical types in JDBC metadata without reading data rows.
- Add `predicatePushdown` and `projectionPushdown` connection properties (both default to `true`), reported by CLI `/info` and statement pushdown diagnostics.
- CLI `/describe` now reports row groups, row counts, and page-index availability.
- Boolean columns can be used directly as predicates (`WHERE active`, `NOT active`, `CASE WHEN active`, and `HAVING active` on grouping columns); non-Boolean predicates are rejected.
- Support `IS [NOT] TRUE/FALSE` in WHERE, HAVING, and Boolean projections.
- Evaluate `BETWEEN` through the SQL conjunction truth table, including NULL bounds, and handle incomparable values consistently in comparisons, `IN`, `BETWEEN`, and quantified predicates. Identical NaN values are not distinct.
- Fix duplicate first rows in queries with window functions and no outer `ORDER BY` or `OFFSET`, which also affected `LIMIT` and aggregates over such queries.
- **Behavior change:** predicates involving NULL follow SQL three-valued logic. WHERE, JOIN ON, HAVING, CASE WHEN, Boolean projections, and quantified subqueries preserve UNKNOWN until the selection boundary, so negations and NOT IN on nullable columns may return fewer rows. This applies to predicates evaluated by the Java engine and to predicates pushed down to Parquet.
- **Behavior change:** microsecond and nanosecond timestamps retain full precision. Equality and range comparisons that previously matched due to millisecond truncation may return different rows.
- **Behavior change:** sampled tables disable predicate pushdown so `TABLESAMPLE` runs before `WHERE`. Repeatable sample membership is independent of the pushdown setting.
- Resolve `ORDER BY` ordinals to select-list positions and preserve exact numeric literals beyond the signed-long range.

### 1.3.0, 2026-03-14
- com.fasterxml.jackson.core:jackson-databind ......... 2.20.1 -> 2.21.1
- org.apache.hadoop:hadoop-common ....................... 3.4.2 -> 3.4.3
- org.apache.hadoop:hadoop-mapreduce-client-core ........ 3.4.2 -> 3.4.3
- org.apache.parquet:parquet-avro ..................... 1.16.0 -> 1.17.0
- org.apache.parquet:parquet-hadoop ................... 1.16.0 -> 1.17.0
- org.apache.parquet:parquet-variant .................. 1.16.0 -> 1.17.0
- org.junit.jupiter:junit-jupiter ....................... 6.0.1 -> 6.0.3
- org.mockito:mockito-core ............................ 5.20.0 -> 5.23.0
- improved JDBC compliance for supported statement creation overloads, statement closed-state reporting, and connection validation
- replaced unsupported connection factory sentinel values with proper JDBC exceptions and wrapper behavior
- aligned driver, CLI, and metadata version reporting with manifest-derived runtime metadata
- expanded Parquet pushdown observability with documented `PushdownInfo` reporting and broader pruning coverage tests
- `JParqStatement` no longer exposes its accidental `BasicThreadFactory.Builder` inheritance; downstream code should not rely on that implementation detail

### 1.2.0, 2025-12-08
- add support for batch execution in prepared statements
- Implemented full positional parameter binding for prepared statements with safe literal rendering to block SQL injection.
- add support for named parameters in prepared statements
- several minor bug fixes and improvements
- improved test coverage and add more edge case tests

### 1.1.0, 2025-12-06
- create a fat jar so the driver can be used in tools that require adding a jar to the classpath
- optimize dependency footprint by excluding more transitive dependencies
- improved schema support
  - parquet files in the root dir will have the schema PUBLIC
  - subdirectories correspond to a schema
- Improve test coverage to >= 80%
- Improve metadata support and accuracy
- refactored function support for increased coherence and reduced coupling
- add function mapping from jdbc escape syntax to sql standard function names for numeric, string, datetime, and system functions
- several minor bug fixes and improvements
- Improved support for quoted identifiers throughout the codebase
- added a cli interface for interactive querying

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
