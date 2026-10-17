# Hugging Face Optimized Parquet Support

## Background

Hugging Face offers dataset downloads in two Parquet flavors: `parquet` and `optimized-parquet`. Optimized files are
plain Parquet files written by PyArrow (>= 21) with two extra writer options:

```python
pq.write_table(table, path, use_content_defined_chunking=True, write_page_index=True)
```

- **Content-defined chunking (CDC):** data page boundaries are chosen by a rolling hash over the column values
  (FastCDC-style gear hash) rather than by a fixed byte size. With PyArrow's defaults the chunks are between
  `min_chunk_size = 256 KiB` and `max_chunk_size = 1024 KiB`, measured before encoding, so pages are not much
  smaller than with the default 1 MiB `data_page_size`. (The ~64 KB figure often quoted is the Xet storage chunk
  size, not the Parquet page size.) Only the writer changes; the output is a standard Parquet file and needs no
  CDC-specific reader support. The purpose is deduplication in Hugging Face's Xet storage backend.
- **Page index:** a ColumnIndex (per-page min/max/null counts) and an OffsetIndex (per-page location and first row
  index). Readers can use it to skip individual pages, not only whole row groups.

For JParq the CDC part is a compatibility concern only. The performance gain comes from the page index, and it is only
realized when JParq attaches a correct filter predicate to the reader.

References:
- https://huggingface.co/docs/hub/datasets-pyarrow
- https://huggingface.co/blog/parquet-cdc
- https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
- https://datafusion.apache.org/user-guide/parquet-content-defined-chunking.html

## Current state of JParq

Verified by running queries through the driver against a small Parquet file with rows `id = 1..4`, nullable `n`
equal to `id` except NULL on row 2, `d date` = epoch day `19000 + id` (2022-01-09 to 2022-01-12) and
`ts timestamp-millis` = `1700000000000 + id` (2023-11-14):

| Query | Result | Expected |
|---|---|---|
| `WHERE n <> 3` | `[1 2 4]` | `[1 4]` |
| `WHERE NOT (n = 3)` | `[1 2 4]` | `[1 4]` |
| `WHERE n NOT IN (3)` | `[1 2 4]` | `[1 4]` |
| `WHERE NOT (n = 1 OR id = 4)` | `[2 3]` | `[3]` |
| `WHERE NOT (n = 1 AND id = 1)` | `[2 3 4]` (correct) | `[2 3 4]` |
| `WHERE NOT (n < 3)` | `[3 4]` (correct, `lt`/`gt` already drop NULLs) | `[3 4]` |
| `WHERE n NOT BETWEEN 3 AND 4` | `[1]` (correct) | `[1]` |
| `WHERE ID = 3` | `[]` | `[3]` |
| `WHERE id < 2.5` | `[1]` | `[1 2]` |
| `WHERE d = DATE '2022-01-08'` | `ClassCastException` (Date -> Number) | `[]` |
| `WHERE ts > '2023-01-01 00:00:00'` | `ClassCastException` (Timestamp -> Number) | `[1 2 3 4]` |
| `WHERE id = n + 0` | `ClassCastException` (String -> Number) | `[1 3 4]` |
| `WHERE id = (2)` | `ClassCastException` (String -> Number) | `[2]` |

| Area | Status | Location |
|---|---|---|
| Reading CDC pages | Expected to work; parquet-java 1.17 reads any page layout | `pom.xml` |
| Page-index filtering | Enabled (`COLUMN_INDEX_FILTERING_ENABLED` together with stats, dictionary and bloom filtering) | `JParqPreparedStatement.configureParquetFiltering` |
| Page index actually used | Only when `ParquetFilterBuilder` produces a predicate | |
| Time zone handling | `coerceLiteral` parses literals with `Timestamp.valueOf`/`Date.valueOf` (JVM default zone), while `unwrap` reads stored values with `Instant.ofEpochMilli`. Micro timestamps are truncated to milliseconds when read. `unwrap` only checks `TimestampMillis`/`TimestampMicros`; Avro's `LocalTimestampMillis`/`LocalTimestampMicros` are separate classes, so local-timestamp columns probably come back as raw `long` (unverified) | `AvroCoercions` |
| **Bug:** DATE / TIMESTAMP filters | Query fails. `buildColOpLit` switches on the Avro physical type (`INT`/`LONG`) and casts to `Number`, but `AvroCoercions.coerceLiteral` returns `java.sql.Date`/`Timestamp`. Probably also `intcol = 'abc'` (unparseable literal returned unchanged) | `ParquetFilterBuilder.buildColOpLit` |
| **Bug:** non-literal operand compared with a column | `comparison()` treats any side that is not a bare `Column` as a literal, and `LiteralConverter.toLiteral` falls back to the expression's SQL text. Numeric columns throw `ClassCastException` (String -> Number) while the query is being planned (`id = n + 0`, `id = ABS(-2)`, `id = 1 + 1`, `id = (2)`, `id > (SELECT MIN(x) FROM t2)`, `n BETWEEN id - 1 AND 5`). STRING columns push down a comparison against the SQL text, and BOOLEAN columns push down `col = false` (`Boolean.parseBoolean` of the text). Because `covers()` is true, no residual runs, so the wrong rows come back silently: `s = LOWER(s)` -> `[]` (expected all rows), `s IN (LOWER('A1'), 'b2')` -> `[2]` (expected `[1 2]`), `active = (n > 1)` -> `[2 4]` (expected `[3]`). Verified on a separate file with `s` STRING and `active` BOOLEAN columns. JDBC `?` parameters are substituted as literals and are not affected | `ParquetFilterBuilder.comparison`, `LiteralConverter.toLiteral` |
| **Bug:** Java-side WHERE evaluation does not handle NULL correctly | `ExpressionEvaluator.eval` returns a plain `boolean`: comparisons and `LIKE` with a NULL operand give `false` and `NOT` flips that to `true`. `WHERE n + 0 <> 3` and `WHERE NOT (n + 0 = 3)` (not pushdownable, so evaluated in Java) -> `[1 2 4]` (expected `[1 4]`); `n NOT IN (3, NULL)` (the NULL literal forces Java evaluation) also returns `[1 2 4]` (expected `[]`). Not specific to pushdown; see [sql-three-valued-logic.md](sql-three-valued-logic.md) | `ExpressionEvaluator` |
| **Bug:** `<>`, `NOT (=)`, `NOT IN`, De Morgan forms of `=` | Return NULL rows. The bug affects only `notEq` leaves: parquet-java `notEq` keeps NULLs (Java semantics), and its `LogicalInverseRewriter` (applied by `FilterCompat.get`) turns `not(eq)` into `notEq` without a NULL guard. When the whole WHERE is pushed down `residual()` is `null`, so nothing removes them. `lt`/`gt`-based negations (`NOT BETWEEN`, `NOT (n < 3)`) are correct | `ParquetFilterBuilder.tryBuild` |
| **Bug:** column name casing | The field is found case-insensitively but the predicate uses the name as typed. Parquet treats an unknown column path as all NULL, so `WHERE ID = 3` matches nothing | `ParquetFilterBuilder.buildColOpLit` |
| **Bug:** non-integer literal on integer column | `id < 2.5` becomes `lt(id, 2)` | `ParquetFilterBuilder.buildColOpLit` |
| Partial pushdown | None. `tryBuild` returns empty for an `AND` if either side is unsupported, and `residual()` is all or nothing (`covers(...) ? null : where`). `id BETWEEN 10 AND 20 AND text LIKE '%x%'` pushes nothing | `ParquetFilterBuilder.residual` |
| Pushdown: `IS NULL` / `IS NOT NULL`, `LIKE 'abc%'` | Not built | `ParquetFilterBuilder` |
| Pushdown with `DISTINCT` | Disabled by two guards: the constructor (`!tmpSelect.distinct()`) and `determinePushdownInfo` ("DISTINCT queries currently use residual filtering only."). No semantic reason found, but the history of the guard has not been traced | `JParqPreparedStatement` |
| Pushdown in joins | Each join table is read in full with no predicate | `JParqPreparedStatement.buildJoinReader` |
| Column pruning with `ORDER BY` | An early return disables projection pushdown whenever there is an `ORDER BY`. It was added as a workaround in commit `0ad2d25` ("Fix test failures for testDepartmentNameSubquery"). `requiredColumns()` already includes order-by columns. The `orderColumns` block after the guard is unreachable | `JParqPreparedStatement.configureProjectionPushdown` |
| Switch to turn pushdown off | Does not exist (only `caseSensitive` is a connection property) | `JParqConnection` |
| PyArrow types: `timestamp[ns]`, `isAdjustedToUTC=false`, `float16` | Unverified how parquet-avro 1.17 maps them to Avro. `AvroCoercions.unwrap` only handles millis and micros, and the micros branch drops the sub-millisecond part | `AvroCoercions`, `JdbcTypeMapper`, `JParqResultSet` |
| Test files written by PyArrow with CDC and a page index | None; all current fixtures are produced with Groovy/parquet-java | `src/test/resources` |
| **Bug:** floating-point semantics (NaN, infinities, signed zero) | Verified on PR #187 merged with #186 (`f90da0d`), since `main` has no pushdown switch. **Java:** any comparison with NaN or ±Infinity is UNKNOWN (`BigDecimal` cannot represent them), so `d > 1e308` returns nothing even for `+Infinity`; `ORDER BY`, `MIN` and `MAX` throw `NumberFormatException`; `-0.0 = 0.0` is TRUE (correct). **Pushdown:** the record-level filter orders NaN above everything, so `dbl > 3` -> `[2 4]` with pushdown on, `[4]` with it off (row 2 is NaN). The same split appears for `>=`, `<>`, `NOT IN`, `NOT BETWEEN` and `NOT (<)`, on FLOAT as well as DOUBLE. PyArrow leaves NaN out of statistics and page-index min/max, so pages whose NaN rows would match are skipped: on a PyArrow file with NaN rows 2, 5 and 6, pushed-down `d > 100` returns `[5 6 7]` (row 2's row group has max 3.0 and is skipped). parquet-java writes no min/max and no page index for pages containing NaN. Signed zero: pushed `d = 0.0` and `d = -0.0` both return only the `0.0` row, and `d < 0.0` includes the `-0.0` row, while Java treats both zeros as equal | `SqlComparisons.typedCompare`, `ParquetFilterBuilder.buildColOpLit` |

The Hugging Face sharded dataset layout (`train-00000-of-00005.parquet`) is handled in a separate follow-up, see
[hf-dataset-layout.md](hf-dataset-layout.md).

## Testing principle

Plain and optimized files go through the same `ParquetFilterBuilder`, so comparing them with each other cannot detect
pushdown bugs. Every correctness test compares against **fixed expected values**, or against the same query run with
pushdown turned off (Phase 1b adds the switch).

Additional rules:
- The `predicatePushdown=false` oracle is **not** valid for NULL handling until
  [sql-three-valued-logic.md](sql-three-valued-logic.md) is implemented: the Java evaluator returns the same wrong
  rows as the pushdown bug. Until then every NULL-related test (negations, `NOT IN`, `NOT LIKE`, nullable columns)
  uses fixed expected values only.
- Tests involving dates or timestamps force a non-UTC default time zone (e.g. `TimeZone.setDefault` in setup,
  restored in teardown). Otherwise pushdown on and off agree only because the build happens to run in UTC.
- Queries with `TABLESAMPLE` follow the rule chosen in Phase 1b item 8.
- The oracle is **not** valid for FLOAT/DOUBLE columns containing NaN, ±Infinity or `-0.0` until Phase 1b item 10
  is implemented; until then those tests use fixed expected values. NaN fixtures must be written by PyArrow, because
  parquet-java omits the statistics that cause the page-skipping bug.

## Plan

### Status

PR #186 ([sql-three-valued-logic.md](sql-three-valued-logic.md)) is merged into `main` (`4acf716`). PR #187
(`feat/optimized-parquet`, head `b254777`, base `main`) implements this plan. GitHub reports it MERGEABLE/CLEAN and
the CI `verify` checks pass. "Current state" above describes `main` before #187.

Done in #187 since the original implementation commit `3e8d634`:
- `9bb6470`: merged `origin/main`, resolving the two expected conflicts:
  - `determinePushdownInfo` messages kept #187's join-pushdown side;
  - the `release.md` 1.4.0 bullets of both PRs were combined, and the "until the optimized Parquet reader ships"
    sentence was dropped.
- `877593e`: Phase 1b item 9 end-to-end tests (`LiteralOperandPushdownTest`). Parenthesized literals such as
  `id = (2)` are now unwrapped and pushed; before, they were evaluated in Java.
- `b254777`: Phase 1b item 10:
  - `SqlComparisons.compareNumbers` is now shared by comparisons, `ORDER BY`, set operations, window ordering and
    `MIN`/`MAX`;
  - FLOAT/DOUBLE pushdown is restricted to NaN-safe operators with non-zero literals that are exactly representable;
  - PyArrow fixtures `float_groups`, `float_special` and `float_pages`, plus `FloatingPointPushdownTest`.

Verified independently on `b254777`:
- `mvn verify` with no skip flags: 994 unit tests and 3 integration tests, no Checkstyle, PMD or Spotless violations;
- every query in the "non-literal operand" and "floating-point semantics" rows returns the expected rows, the same
  with pushdown on and off;
- #186's three-valued-logic, Boolean-predicate, HAVING and window-duplicate probes are unchanged.

Known limitations (listed in the #187 description):
- arithmetic on NaN or infinities (`d + 0`) throws `NumberFormatException`;
- `DISTINCT`/`GROUP BY` keep `-0.0` and `0.0` separate;
- TIME literals are never pushed (`isLiteral` does not accept `TimeValue`);
- `d > x` on FLOAT/DOUBLE columns is no longer pushed, so a filter with only a lower bound loses page skipping (the
  cost of correct NaN results).

Backlog found during review, not covered by #187:
- computed Booleans in derived tables and CTEs (`n > 1 AS c`) are stored as `String`, so `WHERE c` /
  `GROUP BY c HAVING c` are rejected, and JDBC metadata reports `OTHER` for Boolean projections;
- `GROUP BY ROLLUP(...)` / `GROUPING SETS (..., ())` over an empty input return no grand-total row (PostgreSQL
  returns one).

### Phase 0: Real test data (prerequisite)

1. Add `tools/createHfOptimizedData.py`. Pin an exact PyArrow version (e.g. `pyarrow==21.0.0`) in the script
   header, since PyArrow marks CDC as experimental ("The API may change"). It writes the same table twice into
   `src/test/resources/hf/`:
   - `plain.parquet`: default PyArrow writer options apart from the size settings below.
   - `optimized.parquet`: `write_page_index=True` and CDC.
2. Keep the committed files small:
   - pass `use_content_defined_chunking` a dict with small chunk sizes (e.g.
     `{"min_chunk_size": 4096, "max_chunk_size": 16384}`) and a small `data_page_size`, so each column gets many
     pages without megabytes of data;
   - set `row_group_size` so there are at least two row groups;
   - size limit: each fixture at most ~1 MB.
3. The script verifies its own output and fails if it is wrong (it is unverified whether `data_page_size` still
   splits pages when CDC is on):
   - at least two row groups in each file;
   - a ColumnIndex/OffsetIndex is present in `optimized.parquet` and absent in `plain.parquet`
     (`has_column_index`/`has_offset_index`; pass `write_page_index=False` explicitly for the plain file);
   - each file is within the size limit.

   PyArrow's Python metadata API probably does not expose the number of pages per column chunk (unverified), so the
   page count is checked by a Java fixture test instead: `ParquetFileReader.readOffsetIndex(column).getPageCount()`
   on `optimized.parquet` must be above a threshold (e.g. >= 8 per row group for `id`). Together these ensure that a
   failing Phase 2 assertion points to the code, not to a broken fixture.
4. The generated files are committed so the Maven build never requires Python. Document how to regenerate them in
   the script header.
5. Table contents:
   - sorted `id` (int64), nullable `n` (int), `text` (string with nulls), `label` (int)
   - `date32`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`, a timezone-less (local) timestamp, `decimal128`,
     `float16`
   - a list column
   - an `image` struct `{bytes: binary, path: string}` with the `huggingface` schema metadata
     (`{"huggingface": {"dataset_info": {"features": {"image": {"_type": "Image"}}}}}`)
6. Optionally add a small real Hugging Face optimized-parquet download (e.g. a slice of `stanfordnlp/imdb`) if the
   license and size permit.

### Phase 1: Compatibility

Create `src/test/java/jparq/hf/HfOptimizedParquetTest.java`:

- `SELECT *`, `COUNT(*)` and projections on both files return the fixed expected values.
- `DatabaseMetaData.getColumns` and the CLI `/describe` handle the struct, list and binary columns.
- Every column type from Phase 0 can be read with the appropriate `ResultSet` getter, with full precision
  (sub-millisecond parts of micro/nano timestamps are kept).
- Fix whatever fails. Expected work: nanosecond and local timestamps and float16 in `AvroCoercions`,
  `JdbcTypeMapper` and `JParqResultSet`, and the micros precision loss in `AvroCoercions.unwrap`.
- **Decide how local timestamps (`isAdjustedToUTC=false`) are returned**, e.g. `unwrap` returns
  `Timestamp.valueOf(LocalDateTime)` built from the stored wall-clock value with no zone shift, and handle
  `LocalTimestampMillis`/`LocalTimestampMicros`/nanos in `unwrap`. Phase 1b item 2 builds on this decision, so the
  Java comparison and the pushed-down predicate agree.
- **Behavior change:** keeping full precision means `ts = '2023-01-01 00:00:00'` no longer matches a value stored as
  `00:00:00.000500` (today it does, because micros are truncated to millis). Expect some existing tests to need
  updating, and list this as a behavior change in `release.md`.

### Phase 1b: Pushdown correctness (must be done before Phases 3 and 4)

Phases 3 and 4 push predicates into more queries, so these bugs must be fixed first. Each fix gets a test with fixed
expected values (on the Phase 0 fixtures and on a small purpose-built file with NULLs).

Dependency: items 3 and 6 require [sql-three-valued-logic.md](sql-three-valued-logic.md) to be implemented first
(separate PR). With the current two-valued evaluator the negation rewrite also changes the part left for Java:
`NOT (CAST(n AS VARCHAR) LIKE '3%')` returns `[1 2 4]` today, but the rewritten `CAST(n AS VARCHAR) NOT LIKE '3%'`
returns `[1 4]` (derived from `evalLike`, not run), so pushdown on and off would disagree even for parts that are
never pushed. Items 1, 2, 4, 5, 8 and 9 can be done before it. Item 10 builds on `SqlComparisons` from
[sql-three-valued-logic.md](sql-three-valued-logic.md) (PR #186).

Order: items 2-5, 9 and 10 make each pushed predicate correct and must be done **before** item 6 (AND splitting), because
splitting makes more of each query rely on the pushed predicate alone. At runtime the negation rewrite (item 3) is
applied **before** AND splitting (item 6): `NOT (a = 1 OR b LIKE '%x')` becomes `a <> 1 AND b NOT LIKE '%x'`, so
`a <> 1` can then be pushed on its own.

1. **Pushdown switches:** add two connection properties, both default `true`:
   - `predicatePushdown=false`: no Parquet filter predicate is attached; the whole WHERE is evaluated in Java;
   - `projectionPushdown=false`: no Avro projection is set; all columns are read (escape hatch for the Phase 4
     `ORDER BY` change). Implement it by skipping only `AvroReadSupport.setRequestedProjection`/`setAvroReadSchema`
     in `configureProjectionPushdown`, **not** as an early return, so `validateRequiredColumns(...)` still reports
     unknown columns.
   List them in `JParqDriver.getPropertyInfo` and the README, print them in the CLI `/info` output next to
   `Case sensitive:`, and make `PushdownInfo` report "disabled by connection property" when they are off.
   `predicatePushdown=false` is the test oracle for correctness tests that do not involve NULLs (see "Testing
   principle" for the NULL exception).
2. **Logical-type literals:** in `buildColOpLit`, convert coerced values to the physical representation, using the
   same time zone rules as the Java-side evaluation (`AvroCoercions.unwrap`), so pushdown on and off agree:
   - date: `date.toLocalDate().toEpochDay()` (`int`). Do not use `getTime() / 86_400_000`: `Date.valueOf` gives
     local midnight, so in UTC+2 the division lands on the previous day;
   - timestamp-millis/micros/nanos (`isAdjustedToUTC=true`): build the epoch value in the column's unit from
     `toInstant()` (epoch seconds plus nanos), not `getTime()`, which drops the sub-millisecond part. The zone used
     to interpret the literal must match how `unwrap` presents stored values;
   - local-timestamp-millis/micros/nanos (`isAdjustedToUTC=false`): treat the literal as a wall-clock
     `LocalDateTime` with no zone and convert via `toEpochSecond(ZoneOffset.UTC)` plus nanos, consistent with the
     representation chosen for `unwrap` in Phase 1;
   - a literal more precise than the column unit uses the same rounding rule as item 5;
   - decimal: only push down when the physical type and scale can be represented exactly, otherwise skip.
   If the coerced value is not of the expected Java type (e.g. `intcol = 'abc'`), return `Optional.empty()` instead
   of casting, so the expression is evaluated as a residual. This check covers literals that cannot be parsed; item 9
   rejects non-literal operands before coercion. The type check alone does not stop item 9's bug, because
   `coerceLiteral` always returns a `String` for STRING columns and a `Boolean` for BOOLEAN columns.
3. **SQL NULL semantics for negations:** rewrite the expression to negation normal form (push `NOT` down through
   `AND`/`OR` with De Morgan, `NOT (a = v)` -> `a <> v`, `NOT (a < v)` -> `a >= v`, `NOT IN` -> `<>` conjunction,
   `NOT BETWEEN` -> `<` OR `>`), then guard every leaf `notEq(c, v)` (with `v` non-null) as
   `and(notEq(c, v), notEq(c, null))`. No `not()` may remain in the emitted predicate. JParq must do this rewrite
   itself: parquet-java's `LogicalInverseRewriter` also pushes `not()` down, but leaves `notEq` without the NULL
   guard. Do **not** add a NULL guard per referenced column: that would break `NOT (n = 1 AND id = 1)`, which is
   correct today (for a NULL `n`, `NULL AND FALSE` is `FALSE`, so the row belongs in the result).
   **NULL literals:** a comparison with a SQL NULL literal (`= NULL`, `<> NULL`, NULL inside an `IN`/`NOT IN` list)
   is never pushed down; that AND part (or the enclosing `OR`) is evaluated in Java. Today this happens by accident:
   `coerceLiteral(null)` returns `null` and `buildColOpLit` returns empty. `notEq(c, null)` is emitted only for
   `IS NOT NULL` (Phase 3). A naive rewrite of `a NOT IN (1, NULL)` to `notEq(a, 1) AND notEq(a, null)` would return
   every non-NULL row except 1, while SQL returns no rows. Tests include `n NOT IN (3, NULL)` -> `[]`; this test only
   passes once [sql-three-valued-logic.md](sql-three-valued-logic.md) is implemented, since Java returns `[1 2 4]`
   today. Tests also cover nested cases such as `NOT (n = 1 OR id = 4)` -> `[3]` (row 2: `NOT (NULL OR FALSE)` is
   NULL, so excluded; today it returns `[2 3]`) and `NOT (n = 1 AND id = 1)` -> `[2 3 4]`, with NULLs in either
   column.
4. **Column names:** build column references from the resolved field name (`f.name()`), not the name as typed.
5. **Numeric literals that do not fit the column type:**
   - compare the literal as a `BigDecimal` against the column's range (`Integer`/`Long` `MIN_VALUE`/`MAX_VALUE`)
     before converting. Out-of-range literals are not pushed down; Java evaluates them. Never call
     `intValue()`/`longValue()` on an out-of-range value;
   - non-whole literals on integer columns: for `<`, `<=`, `>`, `>=` round the bound correctly (`<`/`>=` use
     `ceil`, `<=`/`>` use `floor`). `=` and `<>` with a non-whole literal are not pushed down: the results would be
     constant (nothing / all non-NULL rows), and `FilterApi` has no constant predicates;
   - optional: after AND splitting, a part that is known to be constant false may return an empty result
     immediately without reading the file;
   - double literals above 2^53 on LONG columns lose precision; convert from the original literal text via
     `BigDecimal`, or skip pushdown.
6. **Partial pushdown (AND splitting):** split the WHERE into its top-level `AND` parts, push the supported parts and
   keep the unsupported ones as the residual. Produce the predicate and the residual in **one** call so they cannot
   drift apart (a part ending up in both or neither), e.g.
   `record PushdownSplit(Optional<FilterPredicate> predicate, Expression residual)` returned by
   `ParquetFilterBuilder.split(Schema, Expression)`. Replace the separate `build()`/`residual()` calls in
   `JParqPreparedStatement` and remove `covers()` when it becomes unused. `OR` stays all or nothing. This is the
   biggest single win for page-index use.
7. Make sure `PushdownInfo` reports the pushed predicate and the remaining residual correctly after the split.
8. **TABLESAMPLE:** today the sample is taken from the already filtered reader (`applyTableSample(..., reader)`), so
   the same `REPEATABLE` sample probably differs between predicate pushdown on and off (not verified). Choose one
   approach, document it, and test it: either skip predicate pushdown when a `TABLESAMPLE` is present, or keep the
   current behavior and exclude sampled queries from the on/off comparison tests. Phase 4's join pushdown must
   follow the same rule.
9. **Literal-only operands** (**done** in PR #187: `isLiteral` in `buildColOpLit`, end-to-end tests in
   `LiteralOperandPushdownTest` (`877593e`), and parenthesized literals are pushed. Open: `isLiteral` does not accept
   `TimeValue`, so TIME literals are never pushed): in
   `ParquetFilterBuilder.comparison` (which also handles `IN` list items and `BETWEEN`
   bounds), push down a column comparison only when the other side is a constant literal. Check this **before**
   calling `LiteralConverter.toLiteral`; anything else returns `Optional.empty()` and is evaluated in Java.
   - Allowed: `StringValue`, `LongValue`, `DoubleValue`, `BooleanValue`, `DateValue`, `TimestampValue`,
     `TimeValue`, `SignedExpression` over an allowed literal, and `CastExpression` of an allowed
     literal. Unwrap parentheses first, so `id = (2)` is pushed: use the existing `unwrapParenthesis` helper (it
     reaches the deprecated `Parenthesis` class only through `LegacyParenthesisSupport`) plus single-element
     `ParenthesedExpressionList`.
   - Rejected: arithmetic, string concatenation, functions, scalar subqueries, Boolean expressions (comparisons,
     `NOT`, `AND`/`OR`) and column references on both sides. `NULL` literals stay unpushed, as item 3 requires.
   - Do not rely on the text fallback in `toLiteral` (`new BigDecimal(e.toString())` / `e.toString()`) anywhere in
     the pushdown path.
   - Tests, on a purpose-built file with INT, nullable INT, DOUBLE, STRING and BOOLEAN columns, comparing pushdown on
     against pushdown off:
     - STRING compared with a function, a scalar subquery and a function inside an `IN` list;
     - BOOLEAN compared with a comparison and with `NOT col`;
     - INT compared with arithmetic, a function, a parenthesized literal and a scalar subquery;
     - an expression as a `BETWEEN` bound;
     - the expected values from the "non-literal operand" row in "Current state".
     The tests also check that `PushdownInfo` reports the rejected comparisons as residuals, and that `id = (2)`
     is pushed.
10. **Floating-point semantics (NaN, infinities, signed zero):** use one ordering in the Java engine and in pushdown,
    matching PostgreSQL and DuckDB (chosen option): `-Infinity` < every finite value < `+Infinity` < `NaN`, with
    `NaN = NaN`, `+Infinity = +Infinity` and `-0.0 = 0.0`. On `main` before #187 the two sides disagree; see the
    "floating-point" row in "Current state". **Done** in PR #187 (`b254777`), see "Status"; beyond this spec, a
    FLOAT/DOUBLE literal is pushed only if it is exactly representable in the column type.
    - **Java engine:** in `SqlComparisons.typedCompare`, when either operand is a `Double`/`Float` that is NaN or
      infinite, compare by rank (`-Infinity`, finite, `+Infinity`, `NaN`) instead of converting to `BigDecimal`
      (which throws, so the comparison is UNKNOWN today). Keep the `BigDecimal` comparison for finite values: do
      **not** use `Double.compare`, which orders `-0.0` below `0.0`. This applies to comparisons, `IN`, `BETWEEN`,
      ANY/ALL and `IS [NOT] DISTINCT FROM`. It also fixes `ORDER BY`, `MIN` and `MAX` on columns containing NaN or
      infinities, which throw `NumberFormatException` today. NaN sorts last in ascending order. Update #186's NaN
      tests in `SqlTruthTest` (`anyAllComparisonFailuresAreUnknown`, `nanComparisonsAreUnknownAcrossAllPredicateKinds`):
      for example `ACOS(2) NOT IN (1.0, 2.0)` and `ACOS(2) NOT BETWEEN 0 AND 5` become TRUE, and the NaN row then
      passes `> ALL (1.0)`.
    - **Pushdown, NaN:** PyArrow leaves NaN out of row-group statistics and page-index min/max, so a pushed `gt`
      skips pages whose only matching rows are NaN. On FLOAT/DOUBLE columns, push down only comparisons that NaN
      can never satisfy after negation normal form: `<`, `<=`, `=` with a non-NaN literal, `IN` and `BETWEEN`.
      Keep `>`, `>=`, `<>` and `NOT IN` as Java residuals, and `NOT BETWEEN` too, since its `>` half makes the
      whole `OR` a residual. AND splitting can still push the other parts of the AND.
    - **Pushdown, signed zero:** parquet-java's record-level filter treats `-0.0` as below and not equal to `0.0`,
      and a `-0.0` literal becomes `0.0` through `BigDecimal`. Do not push down FLOAT/DOUBLE comparisons with a zero
      literal (or rewrite the bounds to cover both zeros, e.g. `=` 0 as `eq(0.0) OR eq(-0.0)`).
    - **Out of scope, noted:** arithmetic on NaN or infinities (`d + 0`) throws `NumberFormatException` in Java, so
      tests must use bare columns or literals. `DISTINCT`/`GROUP BY` keep `-0.0` and `0.0` as separate values
      (PostgreSQL merges them).
    - **Tests:** use a PyArrow fixture (parquet-java writes no min/max and no page index for a page that contains
      NaN, which hides the statistics problem). The fixture needs row groups and pages that mix NaN with values
      below a bound, and FLOAT and DOUBLE columns holding `-0.0`, `0.0`, `NaN`, `+Infinity`, `-Infinity` and `1.0`.
      Compare pushdown on against pushdown off and against fixed values for every operator above, including
      `ORDER BY`, `MIN`/`MAX`, a split AND such as `d > 3 AND id > 0`, and a check that the NaN-safe operators still
      skip pages.
    - **Release note:** behavior change. Comparisons with NaN and infinities return TRUE/FALSE instead of UNKNOWN,
      and results no longer depend on the `predicatePushdown` setting.

### Phase 2: Prove that the page index is used

- Add a small engine helper (e.g. `se.alipsa.jparq.engine.ParquetFileInfo`) that reports, per file, row groups,
  row counts and whether a ColumnIndex/OffsetIndex is present. Keep it out of `JParqDatabaseMetaData`; the helper
  will also serve the sharded layout in [hf-dataset-layout.md](hf-dataset-layout.md). Surface the information in the
  CLI `/describe <table>` output (page-index presence is per table; `/info` only shows connection-level details).
- Add a test helper that opens a `ParquetFileReader` with JParq's configuration and the predicate produced by
  `ParquetFilterBuilder`, and for a narrow `id` range asserts:
  - `plain.parquet`: `getFilteredRecordCount()` equals the row count of the row groups whose statistics match
    (row-group statistics already drop the others, since `id` is sorted);
  - `optimized.parquet`: `getFilteredRecordCount()` is strictly less than for `plain.parquet`.

### Phase 3: Push more of the WHERE clause to the page index (`ParquetFilterBuilder`)

Depends on Phase 1b.

- `IS NULL` / `IS NOT NULL` -> `eq(col, null)` / `notEq(col, null)`.
- `LIKE 'prefix%'` -> `gtEq(prefix) AND lt(upperBound)`, keeping the exact `LIKE` as a residual (possible thanks to
  the AND splitting in Phase 1b):
  - the upper bound uses unsigned UTF-8 byte order: drop trailing `0xFF` bytes and increment the last remaining byte;
    if nothing remains, use only the lower bound;
  - push down **only** when all of these hold (allowlist, since `ExpressionEvaluator.evalLike` also handles other
    keywords such as `SIMILAR_TO`):
    - the keyword is plain `LIKE` (or `null`) and `isNot()` is false;
    - the left side is a bare column of Avro type `STRING` (`evalLike` calls `toString()` on its operand, so
      `intcol LIKE '1%'` matches 1, 10 and 100, and a binary range would be wrong);
    - the pattern is a string literal with a non-empty prefix before the first `%`, no `_` in that prefix, and no
      `ESCAPE` clause.
  - plain `LIKE` is case-sensitive (`caseInsensitive` is set only for `ILIKE`), so a byte-range bound is correct.
- DATE, TIMESTAMP (millis/micros/nanos) and DECIMAL predicates in `BETWEEN` and `IN` as well, reusing the Phase 1b
  conversions.
- Enable pushdown with `DISTINCT`: remove **both** guards (the constructor and `determinePushdownInfo`) and update
  the tests that check the "DISTINCT queries currently use residual filtering only." message. Trace the history of
  the guard first to confirm there was no reason for it.
- One test per operator checking the result against fixed expected values (or pushdown off) and that rows were
  actually skipped (Phase 2 helper).

### Phase 4: Read fewer columns and pages elsewhere

Depends on Phase 1b.

- **ORDER BY projection:** find the root cause of the `testDepartmentNameSubquery` failure that the guard from
  commit `0ad2d25` works around, fix it, and then delete the guard. Also delete the unreachable `orderColumns` block
  (it compares `key.column()`, which may be an alias or an ordinal, with the select columns).
  `testDepartmentNameSubquery` and the rest of the suite must stay green. This matters a lot for datasets with
  embedded media.
- **Join pushdown:** push single-table conditions down to each join table's reader. `JParqResultSet.pruneExpression`
  cannot be reused unchanged:
  - it modifies the tree it is given, so it must work on a copy (or be rewritten to be non-destructive), leaving
    the full WHERE intact for evaluation after the join;
  - predicates without a table qualifier must only be pushed when the column resolves unambiguously to one table;
  - outer joins: only push null-rejecting predicates into the null-supplying side of a `LEFT`/`RIGHT`/`FULL` join
    (e.g. never `b.x IS NULL`), and always evaluate the full WHERE after the join;
  - `USING`/`NATURAL` joins: in a `FULL JOIN ... USING (id)` an unqualified `id` is effectively
    `COALESCE(a.id, b.id)`, so pushing `id = 5` into either side alone changes the result. Exclude merged
    `USING`/`NATURAL` columns from pushdown in `FULL` joins;
  - `TABLESAMPLE` on a join table follows the rule chosen in Phase 1b item 8;
  - tests for inner and outer joins, with and without NULLs, against fixed expected values.
- Optional: let `LIMIT`/`OFFSET` skip whole row groups using the footer row counts. Only valid with no WHERE,
  `ORDER BY`, `DISTINCT`, aggregates, `GROUP BY` or window functions, since the footer counts are taken before
  filtering.

### Phase 5: Docs and release

- Add a README section on Hugging Face datasets: CDC files are supported, the page index is used automatically,
  which WHERE clauses can use it, and the `predicatePushdown`/`projectionPushdown` connection properties (also
  under "Connection options").
- `release.md`: add a new version section at the top (e.g. `### 1.4.0, <date>`, leaving the date out until release)
  with bullets for the user-visible changes: support for Hugging Face optimized Parquet (CDC + page index), the
  pushdown correctness fixes (NULL handling for `<>`/`NOT`, date/timestamp filters, column name casing,
  non-integer literals on integer columns), partial pushdown, the new pushdown operators, `DISTINCT`/join
  pushdown, the `ORDER BY` projection fix, newly supported column types, the
  `predicatePushdown`/`projectionPushdown` connection properties, and any new CLI output.
  - Mark as a **behavior change**: micro/nano timestamps keep full precision, so equality and range comparisons
    that matched because of millisecond truncation may now return different rows.
  - If TABLESAMPLE semantics change (Phase 1b item 8), mark that as a behavior change too.
- Javadoc on all new public helpers (all params, return and throws).
- `mvn verify` passes with no Checkstyle, PMD or Spotless violations.
