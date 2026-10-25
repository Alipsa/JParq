# Version history

### 1.4.0

- Preserve untagged `$$...$$` string contents during SQL comment stripping (#226), including apostrophes, identifier quotes, and comment markers. Reject unterminated untagged dollar quotes in the pre-pass. Tagged literals remain disabled; identifiers starting with `$name$` retain their existing behavior. JDBC parameter scanning remains tracked in #296.
- Fix parenthesized joins in FROM silently dropping joined inputs (#224). Preserve nested join boundaries, outer-join NULL extension, qualified columns and wildcards, group aliases, and positional column aliases. Add regression coverage for invalid decimal declarations inside grouped join inputs.
- Preserve quoted lowercase names in grouped column projections, ON conditions, residual WHERE predicates, and ORDER BY (#224, #292); quoted columns in computed expressions remain tracked in #244. Preserve quoted aliases containing prefix-like text such as `"u:x"`. Report duplicate-label qualified join-input wildcards with a clear ambiguity error and SQLState `42000` (#224). Cache immutable join mappings once and remove name-based VALUES cache writes from join execution. Document eager group materialization and the lack of predicate pushdown into groups.
- Fix independent VALUES join inputs reusing another input's cached rows (#224). In particular, `FULL JOIN ... USING` over non-overlapping VALUES inputs now retains both unmatched rows instead of returning one. Mixed derived-table and VALUES joins now resolve their independent column alias lists correctly (#284).
- **Behavior change:** set operations promote numeric branches to a common type and coerce values before duplicate comparison, multiplicity counting, and ordering (#223). INTEGER/BIGINT uses BIGINT, matching REAL inputs retain REAL/Float, other approximate combinations use DOUBLE, and exact DECIMAL inputs preserve sufficient integer digits and scale. JDBC metadata reports the common type and decimal bounds even for empty results; previously incompatible branches failed or literal branches reported OTHER with mixed Java types.
- **Behavior change:** quantified comparisons in WHERE now reject array, map, and struct candidates returned by subqueries instead of silently filtering out rows. ANY/SOME/ALL requires scalar candidate values from both arrays and subqueries; the same validation applies in HAVING (#214).
- Fix HAVING comparisons using ANY, SOME, and ALL over subqueries and arrays, including correlated grouping values, NULLs, and empty inputs (#214). Share quantified comparison semantics with WHERE and reject invalid array operands explicitly.
- Add regression coverage for select-list scalar subquery failures retaining SQLState `42000` and the original nested diagnostic during projection access and streaming `ResultSet.next()` (#212). The exception handling was already corrected by the SQL error-context changes; document that evaluation can be deferred until iteration or value access.
- **Behavior change:** ON conditions now reject original table or CTE names hidden by an alias with SQLState `42000`. For example, after `FROM employees e`, use `ON e.id = ...`; `ON employees.id = ...` previously worked and now fails. Other clauses retain their existing qualifier resolution; consistency remains tracked in #261. Explicitly reject column alias lists on `information_schema.tables` and `information_schema.columns` with `42000`, including joined inputs, instead of ignoring them or reporting a missing renamed column (#209).
- Preserve alias lists on inherited CTE references inside scalar/EXISTS/IN subqueries, derived tables, LATERAL inputs, and subsequent CTE bodies (#210), including parenthesized references. Reject physical-table column alias lists explicitly with SQLState `42000` pending #263. Check qualified ON references against their own visible input rather than accepting the same column name from an unrelated input (#209).
- Bind column alias lists on ordinary CTE references (`FROM c AS x(a, b)`) as well as parenthesized references, including wildcard output, grouping, recursion, and joins (#210, #211). Reject invalid alias counts and duplicate aliases; cache renamed rows per materialized CTE result without retaining stale correlated or recursive data. Missing-column diagnostics use visible relation aliases, and ON conditions validate against the inputs joined so far. Join validation still occurs during execution, while nonparameterized single-input queries validate during preparation (#209).
- **Behavior change:** reject unknown columns in derived tables and CTEs instead of silently returning NULL (#209). Validate visible SQL output names, including renamed columns and empty inputs, independently of pushdown settings. Missing-column diagnostics now retain the column name and SQLState `42000` for physical and materialized inputs.
- **Behavior change:** scalar subqueries share cardinality validation across ordinary scalar expressions, grouped SELECT lists, and HAVING (#207). Multiple rows or a column count other than one raise SQLSyntaxErrorException with SQLState `42000`; column counts are checked even for empty subquery results. Empty one-column results and SQL NULL values still evaluate to NULL.
- **Behavior change:** HAVING evaluates AND/OR operands from left to right and stops when the left operand determines the result (FALSE for AND, TRUE for OR). Skipped operands, including their errors, are not evaluated; reordering conditions can expose an error (#207).
- Support scalar subqueries in grouped SELECT lists that correlate on outer grouping columns (#207). Evaluate them with each group's values, preserve NULL and grouping-set semantics, and reject correlations to ungrouped columns. Apply HAVING before scalar projections, evaluating HAVING aliases on demand and reusing their values; infer scalar types only from surviving groups and preserve BIGINT for empty COUNT subqueries. Support ORDER BY the scalar expression as well as its alias or position. Keep inner-scope name binding and scalar cardinality checks through derived tables and CTEs.
- **Behavior change:** `DATE - DATE` returns a signed calendar-day count as `Long` (JDBC `BIGINT`) instead of an internal interval, preserving numeric values through derived tables and CTEs. Reject unsupported temporal arithmetic, including `DATE + integer`, with SQLState `42000`; use explicit `INTERVAL` offsets (#205). Day differences can no longer be compared with or added to `INTERVAL` values: these operations raise `42000`; compare against numeric days instead. Recognize parenthesized and `CURRENT_DATE` differences in metadata and preserve numeric results with aliases that shadow source DATE columns.
- **Behavior change:** JDBC `setDate` and `setObject(LocalDate)` parameters now render as typed DATE literals instead of quoted strings (#205). `SELECT ?` returns `java.sql.Date`, date parameters support explicit INTERVAL offsets, and `EXTRACT` reads their date fields. Timestamp and time parameters still render as quoted strings.
- **Behavior change:** unknown or unsupported scalar functions and unsupported expression types now raise `SQLSyntaxErrorException` with SQLState `42000` when evaluated instead of silently returning the function call's SQL text (#204). Preserve the classification through projections, filters, aggregates over scalar expressions, sorting, nested queries, and materialization; private correlated-value bindings continue to resolve typed values. Unsupported parser nodes such as `JSON_ARRAYAGG` and `AT TIME ZONE` also fail with `42000`. Unary signs continue to convert numeric text; invalid numeric text raises `SQLDataException` with SQLState `22018`, while unsupported operand types raise `42000`. Expression diagnostics replace private correlated binding placeholders with `<outer value>`, including inside nested subqueries, and NOT IN lists with signed column operands remain evaluated per row with or without predicate pushdown. Hex literals and JDBC byte parameters retain their legacy SQL-text values (#250).
- **Behavior change:** aggregates that reach scalar evaluation, such as `COALESCE(MAX(cyl), 0)` or `SUM(cyl) + 1`, now raise a contextual SQLState `42000` error instead of returning SQL text or failing numeric conversion. The diagnostic identifies the supported aggregate and points to [#231](https://github.com/Alipsa/JParq/issues/231), which tracks computing aggregate results inside scalar expressions. These examples refer to ungrouped queries; HAVING uses a separate evaluator, and HAVING and grouped select lists are not covered by this classification ([#247](https://github.com/Alipsa/JParq/issues/247)).
- Implement the standard scalar `NULLIF(a, b)` function using SQL equality, returning NULL for equal arguments and preserving the first value otherwise, including NULL comparisons. Reject argument counts other than two and DISTINCT, UNIQUE, or `*` modifiers with SQLState `42000` (#204). NULLIF in HAVING and grouped select lists remains unsupported (#247).
- Window-only function calls without OVER now report that an OVER clause is required, with SQLState `42000`.
- **Behavior change:** JDBC column labels for delimited identifiers no longer include their surrounding quotes (#202). Preserve SQL labels and case-sensitive lookup independently of internal Avro field and record names through CTEs, derived tables, VALUES, joins, and LATERAL. Quoted names containing spaces and unnamed computed outputs no longer fail Avro schema validation; generated internal names are unique even when labels collide and cannot be referenced as SQL aliases. Decode doubled identifier quotes only within complete delimited identifiers, preserving unaliased expression labels such as `"id" + 1`. Cache validated materialized column bindings so getters retain direct record reads. Bind unqualified join predicates by SQL lookup keys, and reject ambiguous projection and predicate references to duplicate SQL names rather than selecting an arbitrary value (also addresses the reviewed cases in #243).
- **Behavior change:** numeric casts reject non-finite inputs (`Infinity`, `-Infinity`, and `NaN`) and invalid numeric text with `SQLDataException` and SQLState `22018`, instead of exposing `NumberFormatException` or an unclassified wrapper (#203). Preserve that classification through derived tables, CTEs, scalar subqueries, and filters, including pushed join predicates; join planning also preserves existing decimal overflow errors (`22003`). Projection evaluation failures, including invalid temporal values, now reach JDBC getters as `SQLException` with their original causes. Overflow from finite inputs narrowed to integer or floating-point targets remains tracked in #238.
- Decode doubled apostrophes in SQL string literals, including JDBC string parameters, JSON_OBJECT keys, and pushed comparison, IN, BETWEEN, and LIKE filters (#199). Literal decoding leaves dollar-quoted contents unchanged; pre-pass failures for dollar-quoted apostrophes and comment markers remain tracked in #226.
- **Behavior change:** JSON_OBJECT preserves evaluated key text, including keys from columns and comma-form arguments. Leading and trailing whitespace and literal quotes are retained; empty and blank keys are included instead of dropping their pairs. A key containing a single apostrophe now works instead of throwing StringIndexOutOfBoundsException (#199).
- Support set operations in derived tables, including joins and LATERAL inputs, instead of failing with an internal ClassCastException; preserve local CTE scope, aliases, ordering, row bounds, and decimal declaration validation (#198).
- Support quantified comparisons over Parquet array columns, including correlated logical arrays (#197).
- **Behavior change:** JDBC array elements now decode dates, decimals, and timestamps to their Java logical types before correlation binding or projection (#219).
  Plain binary elements also follow the scalar UTF-8 text heuristic per element: text can become `String` and other
  values `byte[]`, so a single array can contain both; element schemas marked `jparq.binary=true` stay `byte[]`.
  Nested array elements are recursively returned as `Object[]` instead of `GenericData.Array`.
- **Behavior change:** arrays and structs retain JDBC ARRAY and STRUCT metadata through derived tables, CTEs, joins, and set operations, including empty physical projections with matching source schemas (#201). Array columns read through derived tables and CTEs return `Object[]` instead of VARCHAR identity strings. Computed arrays in non-LATERAL derived tables and CTEs infer element types recursively from returned values, including logical and nested elements; empty or all-NULL elements use a nullable VARCHAR element schema.
- **Behavior change:** `getObject()` on struct columns returns Avro `GenericRecord` values instead of JSON strings, preserving fields for subsequent materialization; `getString()` still returns their textual representation (#201). Fields retain raw Avro values, such as `Utf8`, epoch-day integers, decimal `ByteBuffer`, and timestamp longs; nested fields are not recursively decoded. JSON_OBJECT and JSON_ARRAY continue to embed structs as JSON objects.
- Compare struct and array keys by content for DISTINCT, GROUP BY, and all set operators, ignoring Avro record names and array identity (#201, #218). Retain a set-operation source schema only when every branch reports the same schema; otherwise infer common array element types from the materialized values.
- Compare window PARTITION BY keys by content for structs, arrays, and binary columns, ignoring Avro record names and array identity (#201, #235). Keep each content group contiguous even when distinct keys have identical text representations.
- Evaluate `ANY`, `SOME`, and `ALL` comparisons over arrays instead of silently filtering out matching rows; preserve SQL NULL and empty-array semantics and negated predicates with pushdown enabled, and reject comparisons with two quantified operands or non-scalar array elements (#197).
- Apply derived-table column alias lists by output position across standalone inputs, joins, and LATERAL, including computed and wildcard projections; reject alias-count mismatches even for empty results, and reject duplicate column aliases at parse time with SQLState `42000` (#196).
- Reuse parsed query shapes, validated source schemas, and eligible prepared plans for correlated EXISTS, IN, scalar, quantified, and ARRAY subqueries within one owning execution (#192). Bind current outer values directly, refresh row-dependent derived/CTE/join materializations, and preserve cancellation and uncorrelated-result caching.
- **Behavior change:** correlated outer references retain their original Java types instead of being converted through SQL literals (#192). Scalar projections of an outer INTEGER can therefore return `Integer` rather than the literal-derived `Long`; NULLs and exact decimal and temporal values are preserved.
- **Behavior change:** non-LATERAL derived tables, including joined inputs, infer unknown computed column types from returned values (#191). `getObject()` returns inferred Java values such as `java.sql.Date`, `Long`, and `BigDecimal` instead of strings, preserving exact numeric aggregate behavior. Empty or all-NULL unknown-type columns fall back to VARCHAR; mixed types, such as numbers and strings in a CASE expression, may also resolve to VARCHAR with String values. Known column types remain available for empty results.
- Support `SELECT` without `FROM`, including recursive CTE anchors such as `SELECT 1`; preserve numeric computed values when materializing CTEs (#190).
- Reject missing columns and projection wildcards in SELECTs without FROM. Stream CTE materialization when types are known and release buffered rows during conversion when type inference is needed.
- Reject lossy numeric conversions during CTE materialization and recursive alignment with `SQLException` instead of truncating fractions, overflowing integers, or rounding decimal scale. Validate table-free named WINDOW and DISTINCT ON expressions; document the explicit-FROM requirement for correlated LATERAL inputs and preserve scalar/EXISTS correlations.
- **Behavior change:** FROM-backed recursive CTEs whose recursive term widens an integer anchor to fractional values now fail instead of returning string values. Use an explicitly typed anchor such as `CAST(1 AS DECIMAL(10,1))` for a recursive increment of `0.5`.
- **Behavior change:** SQL syntax rejected by JSqlParser now raises `SQLSyntaxErrorException` with SQLState `42000` instead of an ordinary `SQLException` with no SQLState. When propagating syntax or numeric SQLStates, preparation and nested-subquery context is retained in error messages and cause chains; clients matching exception types or messages should account for this change.
- **Behavior change:** `DECIMAL`, `NUMERIC`, and `NUMBER` casts with declared precision round HALF_UP to the declared scale (defaulting to zero when omitted), so `CAST(0.01 AS NUMERIC(6))` now returns `0`. Values exceeding precision after rounding, such as `CAST(123 AS DECIMAL(2,0))`, fail with SQLState `22003`. Invalid declarations (`p < 1`, `s < 0`, or `s > p`), such as `DECIMAL(2,5)`, fail during preparation with SQLState `42000`, including for NULL inputs and empty results. LATERAL decimal expressions with unknown precision now use VARCHAR to preserve values across correlated executions.
- Preserve decimal precision and scale through CTEs, derived tables, and aggregate materialization; retain exact integer conversion checks without allocating decimal objects for ordinary integer values.
- Include JParq's own LICENSE at the root of both the thin and fat JARs.
- Fix fat-JAR service descriptor merging, preserve dependency license/notice text, remove dependency module descriptors and stale JAR indexes, and preserve multi-release dependency classes. Centralize driver-aware Hadoop configuration so local Parquet reads work in isolated driver loaders with a foreign context class loader. Add packaged-artifact regression probes for discovery, metadata, all fixture values, GZIP/Snappy/ZSTD, and replacement loaders. Java 21 remains the minimum; Java 25 is best effort. Add optional SQL Workbench/J batch integration coverage. Specific SQL-client GUI workflows remain unverified; see the compatibility record.
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
