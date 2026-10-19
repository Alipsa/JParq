# Version history

### 1.4.0
- Reject invalid calendar dates and out-of-range clock fields in DATE, TIME, and TIMESTAMP literals and casts instead of rolling them over; retain unpadded SQL fields, valid leap days, fractional times, and ISO date-time input. This includes rejecting end-of-day `24:00:00`: `TIME '24:00:00'` and `TIMESTAMP '2024-06-30 24:00:00'` now fail instead of rolling over to midnight or the next day.
- Adapt to JSqlParser 5.4 AST changes for quantified subqueries, temporal literals, JSON functions, and type-named identifiers; preserve qualified-wildcard token positions, the blank-line parsing fallback, and bounded simple-to-complex parsing. Preserve nested ANY/ALL/SOME subquery parentheses and flexible timestamp/time literal formats. Share identifier-aware parsing with LATERAL and pushdown planning. Reject unsupported JSON_VALUE/JSON_QUERY clauses and extra paths explicitly, and reject unsupported temporal literal types such as TIMESTAMPTZ.
- Account for Parquet 1.18's conservative floating-point range pruning when page indexes lack NaN counts; equality, IN, and eligible predicates on other columns can still prune pages.
- Upgrade dependencies: avro 1.12.1 → 1.12.2, checkstyle 13.3.0 → 14.3.0, hadoop 3.4.3 → 3.5.0, jackson 2.21.1 → 2.22.4, jsqlparser 5.3 → 5.4, junit 6.0.3 → 6.1.3, parquet 1.17.0 → 1.18.1, slf4j 2.0.17 → 2.0.20, and Mockito 5.23.0 → 5.24.0.
- Upgrade Maven build plugins: spotless-maven-plugin 3.4.0 → 3.10.4, maven-compiler-plugin 3.15.0 → 3.16.0, maven-enforcer-plugin 3.6.2 → 3.6.3, maven-failsafe-plugin 3.5.5 → 3.6.0, maven-jar-plugin 3.5.0 → 3.5.1, maven-surefire-plugin 3.5.5 → 3.6.0, versions-maven-plugin 2.21.0 → 2.23.0, jacoco-maven-plugin 0.8.14 → 0.8.15, central-publishing-maven-plugin 0.10.0 → 0.12.0.
- Deprecate the correlation rewrite and isolation overloads that ignore inner-scope columns; use the schema-aware overloads for unqualified column binding.
- Add opt-in `datasetLayout=hf` multi-file tables for named Hugging Face shards and numeric directory layouts, with strict discovery, lazy schema validation, per-shard pushdown, and aggregated CLI diagnostics; the default `none` layout preserves single-file discovery.
- Reuse shard validation within each prepared statement and discover the catalog once per column-metadata request; report incompatible shard schemas with the differing field path.
- Honor `Statement.cancel()` during aggregation, sorting, join loading, and nested execution, as well as in-flight JDBC `next()` calls. Cancellation returns without waiting for an active physical read and the executing call throws `SQLException`; cleanup follows the current physical read, which with a pushed-down filter can span a shard.
- Report query cancellation with SQLState `57014`, preserve the original cancellation exception through correlated-subquery evaluation, and keep the outer result and cancellation state active between subquery calls.
- Preserve IN precedence for subqueries and value lists throughout derived tables, CTEs, set operations, joins, HAVING, and computed expressions. Evaluate correlated IN with each outer row's values and analyze uncorrelated IN only once per predicate node.
- Resolve unqualified subquery columns against inner inputs before falling back to the outer row or HAVING group, consistently across IN, EXISTS, scalar, and ANY/ALL subqueries; retain these outer dependencies during projection pushdown.
- Bind information_schema subquery columns using their fixed schemas without catalog scans; keep unqualified names in the inner scope when its input columns cannot be determined.
- Preserve WITH declarations inside parenthesized subqueries and derived tables, so local CTE names take precedence over physical tables and inherited CTEs; continue rejecting duplicate declarations in one WITH scope.
- Preserve filters and computed projections in derived tables over CTEs, including joined derived inputs.
- Keep SQL `USER()`/`DATABASE()` contexts isolated during cross-thread cancellation and make result-set closure idempotent.
- **Behavior change:** unreadable table schemas now fail when a statement is prepared, including in the default `none` layout, instead of continuing without pushdown.
- Make CLI `/describe` resolve literal schema/table names before querying column metadata, escaping JDBC wildcards so similarly named tables cannot mix columns or cause unrelated validation failures.
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
- Evaluate `BETWEEN` through the SQL conjunction truth table, including NULL bounds.
- Order floating-point values consistently in comparisons, `IN`, `BETWEEN`, quantified predicates, `IS [NOT] DISTINCT FROM`, `ORDER BY`, `MIN`, and `MAX`, which no longer throw for `NaN` or infinities.
- Fix duplicate first rows in queries with window functions and no outer `ORDER BY` or `OFFSET`, which also affected `LIMIT` and aggregates over such queries.
- Execute derived-table sorting and row bounds only in their own query scope, fixing correlated sorting when the qualifier matches the base table name; preserve inner aliases that shadow outer qualifiers.
- Preserve computed Boolean types and SQL NULL values through derived tables, CTEs, and top-level set operations; keep metadata stable for empty and exhausted results.
- Resolve computed join metadata against qualified source columns; preserve the combined schema for empty joins independently of predicate pushdown and exclude hidden predicate inputs from wildcard output.
- Support `IS [NOT] UNKNOWN` in predicates, Boolean projections, and HAVING.
- Apply CTE ordering, DISTINCT, and row bounds only in their own query scope; preserve materialized values and column aliases in wildcard projections, including empty results.
- Evaluate DISTINCT over computed projections after window calculation and before final ordering and row bounds; retain unselected navigation-window inputs during projection pushdown.
- Honor explicit `NULLS FIRST` and `NULLS LAST` in set-operation ordering for labels, ordinals, and expressions.
- **Behavior change:** predicates involving NULL follow SQL three-valued logic. WHERE, JOIN ON, HAVING, CASE WHEN, Boolean projections, and quantified subqueries preserve UNKNOWN until the selection boundary, so negations and NOT IN on nullable columns may return fewer rows. This applies to predicates evaluated by the Java engine and to predicates pushed down to Parquet.
- **Behavior change:** comparisons with `NaN` and infinities return TRUE or FALSE instead of UNKNOWN. Values are ordered `-Infinity` < finite < `+Infinity` < `NaN` (`NaN = NaN`, `-0.0 = 0.0`), `NaN` sorts above all numbers, and results no longer depend on the `predicatePushdown` setting. On `FLOAT` and `DOUBLE` columns only `<`, `<=`, `=`, `IN`, and `BETWEEN` against non-zero literals exactly representable in the column type are pushed to Parquet. Known limitations: arithmetic on `NaN` or infinities still fails, and `DISTINCT`/`GROUP BY` keep `-0.0` and `0.0` separate.
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
