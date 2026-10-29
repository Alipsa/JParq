[![Maven Central](https://img.shields.io/maven-central/v/se.alipsa/jparq)](https://mvnrepository.com/artifact/se.alipsa/jparq)
[![javadoc](https://javadoc.io/badge2/se.alipsa/jparq/javadoc.svg)](https://javadoc.io/doc/se.alipsa/jparq)

# JParq

JParq is a JDBC driver for Apache Parquet files. It treats a directory as a database and every `.parquet` file in that
directory as a table. The table name is the filename without the `.parquet` extension. 
Sub directories are treated as schemas. 
For example, given the following directory structure:

```
/data
  ├── employees.parquet
  ├── departments.parquet
  └── sales
      ├── orders.parquet
      └── customers.parquet   
```
The tables `employees` and `departments` would be in the default PUBLIC schema, while `orders` and `customers` would be in the `sales` schema.

JParq uses Apache Arrow and Apache Parquet for efficient columnar reads and jsqlparser to parse SQL statements. 
It aims to be 100% compliant with the read part of the SQL standard. There are a few common extensions supported as well e.g. LIMIT, ASCII, REPEAT, DIFFERENCE.

Note: When common implementations differ from the SQL standard, we stick to the standard. An example of this is
convert which in many databases is used for data type casting, but in the SQL standard is used for character set
conversion. If you want to convert data types, use CAST instead.

> **Note**
> The majority of the code was created in collaboration with (vibe coded with) ChatGPT Codex with Copilot and 
> myself (Per Nyfelt) as code reviewers.

## Requirements

- Java 21. This matches the client runtime supported by
  [Hadoop 3.5.0](https://hadoop.apache.org/docs/r3.5.0/index.html). Java 25 passes JParq's test suite on macOS ARM64 and can be
  used on a best-effort basis, but Hadoop 3.5.0 does not officially support it.
- Parquet files stored in a directory accessible from the JVM running the driver

## Installation

Add the dependency to your build. Replace `x.y.z` with the latest version number from the Maven Central badge above.

```xml
<dependency>
  <groupId>se.alipsa</groupId>
  <artifactId>jparq</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Usage

The driver registers itself when the `se.alipsa.jparq.JParqDriver` class is loaded, so simply placing the JAR on the
classpath is normally enough. If your runtime requires explicit registration you can call
`Class.forName("se.alipsa.jparq.JParqDriver")` before obtaining a connection.

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import se.alipsa.jparq.JParqSql;

public class JParqExample {

  // Standard JDBC
  void selectMtcarsLimit() throws SQLException {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM mtcars LIMIT 5")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
      }
    }
  }

  // Using the JParqSql helper
  void selectMtcarsToyotas() {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars WHERE model LIKE 'Toyota%'", rs -> {
      try {
        while (rs.next()) {
          System.out.println(rs.getString(1) + ", " + rs.getInt(2) + ", " + rs.getDouble(3));
        }
      } catch (SQLException e) {
        System.out.println("Query failed: " + e);
      }
    });
  }
}
```

Subqueries resolve unqualified column names in their inner scopes first, then in the outer row or grouping
context when the inner inputs have no matching column. This also applies to correlations in `HAVING`.
Virtual information_schema tables use their fixed schemas for name binding. If an inner input's columns
cannot be determined, unqualified names remain inner; use an explicit qualifier for outer references.
WITH declarations inside subqueries, including derived tables, keep their local scope and override physical
tables or inherited CTEs with the same name.

Scalar subqueries in grouped SELECT lists can reference outer grouping columns, including grouping columns
omitted from the output. Each subquery uses the current group's values; NULL grouping keys and keys suppressed
by grouping sets bind as NULL. Inner names take precedence over outer names. References to ungrouped outer
columns are rejected, including when the outer input is empty.
HAVING filters groups before select-list scalar subqueries run; a subquery alias referenced by HAVING is
evaluated on demand and reused for that group's output. Only surviving groups contribute to inferred scalar
result types. COUNT subqueries retain BIGINT metadata when no groups survive; other unknown scalar types
continue to depend on returned values.
Aggregate `FILTER (WHERE ...)` clauses are currently unsupported and raise `SQLSyntaxErrorException`
with SQLState `42000` during preparation, including when combined with `OVER`. Use a `WHERE` clause
when all aggregates share one filter, or conditional arguments such as
`COUNT(CASE WHEN cyl IN (4,6) THEN 1 END)` for separate aggregate filters.
HAVING supports quantified comparisons with `ANY`, `SOME`, and `ALL` over subqueries and arrays,
including aggregate values and correlated grouping columns. These comparisons use SQL three-valued logic for
NULLs; empty inputs yield FALSE for ANY/SOME and TRUE for ALL.
Quantified comparisons in WHERE and HAVING require scalar candidates; array, map, and struct candidates
from either arrays or subqueries raise an error.
HAVING evaluates AND/OR operands from left to right: AND stops after FALSE, and OR stops after TRUE.
Skipped operands are not evaluated, so their errors are suppressed; reordering conditions can expose an error.
Scalar subqueries must return exactly one column and at most one row. Violations raise SQLSyntaxErrorException
with SQLState `42000` consistently in scalar expressions and HAVING, including empty results with multiple
columns. An empty one-column result evaluates to NULL.

Correlated subqueries reuse parsed query shapes and validated source schemas within one outer execution.
Outer values are bound with their Java types and NULL values intact, and each evaluation reads fresh results.
Simple physical-table subqueries also reuse their prepared plans; derived tables, joins, and CTEs rebuild
row-dependent materializations. Eligible correlated predicates are rebuilt from the current bindings for
predicate pushdown on every evaluation; the Java residual filter still checks current outer values.
Caches are local to the owning execution, and uncorrelated subqueries continue to reuse their results.

Delimited lowercase column references resolve to unquoted lowercase columns: `SELECT "id" + 0 FROM optimized
WHERE "id" = 1`. This also applies to GROUP BY, derived tables, CTEs, VALUES inputs, and computed
join expressions with qualified duplicate column names. Lowercase CTE names can be declared or referenced
with quotes (`WITH "c" AS (SELECT 1 AS x) SELECT x FROM c`), including in correlated subqueries.
Equivalent CTE declarations such as `c` and `"c"` in the same WITH list are rejected with SQLState `42000`;
a nested WITH shadows an outer declaration using either form, including in scalar subqueries
and subsequent CTE declarations in that nested scope.
Quoted mixed-case names still require exact case. Unquoted column references to explicitly quoted lowercase
materialized aliases, such as `SELECT x FROM (SELECT 1 AS "x") t`, remain unsupported
([#338](https://github.com/Alipsa/JParq/issues/338)).

Delimited column aliases retain their case and appear in JDBC column labels without the surrounding
identifier quotes. Derived tables and CTEs preserve these labels, including spaces and unnamed computed
expressions such as `id + 1`; internal Avro names do not restrict SQL output labels. Quoted table aliases
such as `"My T"` are supported at these materialization boundaries. Doubled quotes in delimited identifiers
are decoded (`"a""b"` has label `a"b`). Unaliased expression labels keep their SQL text, including
quoted operands such as `"id" + 1`. Generated internal names are not SQL column aliases, and ambiguous
projection, predicate, and ORDER BY references to duplicate output names raise an SQL exception
with SQLState `42702` naming the ambiguous column, including for empty inputs. Qualify join columns
with a table alias to resolve ambiguity; duplicate outputs remain available by position through `SELECT *`
and positional ORDER BY. Repeated SELECT labels may be used by ORDER BY when they denote the same
expression; different expressions with the same label are ambiguous, including columns contributed by
qualified wildcards. Column equivalence uses the bound input field, so an unqualified reference and its
qualified form can share a label. Function names and unquoted identifiers compare without regard to case;
quoted identifiers and string literal contents retain their case. Unused duplicate output labels do not
invalidate ordering by another column.

Derived tables and CTEs validate column references against their visible output names, including for empty
inputs. Unknown names and columns replaced by a column alias list raise `SQLSyntaxErrorException` with
SQLState `42000`, naming the missing column; generated internal Avro names are not visible SQL columns.
CTE reference alias lists bind by output position in both `FROM c AS x(a, b)` and `FROM (c) x(a, b)`,
including wildcard output, grouping, recursive CTEs, joins, and nested queries.
Column alias lists on physical Parquet tables are currently rejected with SQLState `42000`; use a derived
table or CTE instead (tracked in [#263](https://github.com/Alipsa/JParq/issues/263)).
Alias lists on `information_schema.tables` and `information_schema.columns` are also explicitly rejected;
wrap these inputs in a derived table or CTE to rename their columns.
Nonparameterized single-input queries validate during preparation; joins validate when their inputs are
built during execution. ON conditions can reference only columns from inputs joined so far.
In ON conditions, a table alias hides the original table or CTE name: use `e.id`, rather than `employees.id`,
after `FROM employees e`. Other clauses retain their existing qualifier resolution; consistency is tracked
in [#261](https://github.com/Alipsa/JParq/issues/261).

Recursive CTEs retain the anchor's numeric types and decimal precision and scale. Recursive values that require
fractional truncation or decimal rounding, or exceed integer ranges or decimal precision, fail with `SQLException`
(SQLState `22003`); recursive schemas are not widened across iterations. For example, an integer anchor cannot
accept a recursive step of `0.5`. A decimal literal also infers precision from its value: an anchor of `0.5`
becomes `DECIMAL(1,1)` and cannot grow to `1.5`. Use an explicit cast to reserve both precision and scale:

```sql
WITH RECURSIVE r(n) AS (
  SELECT CAST(0.5 AS DECIMAL(10,1))
  UNION ALL SELECT n + 1 FROM r WHERE n < 29.5
)
SELECT n FROM r ORDER BY n
```

Computed values with unknown types, including dates and window results, are typed from the returned values
through CTEs and non-LATERAL derived tables, including when used in joins. Inferred metadata depends on
which rows survive filtering: empty or all-NULL unknown-type columns use VARCHAR, and mixed types may
also resolve to VARCHAR. Known column types remain available for empty results.
For inferred columns, `getObject()` returns the corresponding Java type, such as `java.sql.Date`, `Long`,
or `BigDecimal`.
LATERAL derived tables bind outer values with their Java types intact. For example,
`FROM arr a CROSS JOIN LATERAL (SELECT u.t FROM UNNEST(a.dates) AS u(t)) x`
expands the current row's array. When the UNNEST argument is a direct outer column reference,
declared date and nested-array element types are preserved, including metadata when the outer input is empty.
Wrapping the argument in an expression such as COALESCE currently loses its declared element schema;
LATERAL output types can then fall back to VARCHAR during schema discovery.
Null and empty arrays produce no inner rows; a LEFT JOIN LATERAL retains their outer rows with NULL inner columns.
Standalone `UNNEST(NULL)` also returns no rows and uses VARCHAR metadata because its element type is unknown.

LATERAL derived tables may still expose numeric values as VARCHAR. In particular, LATERAL decimal
expressions with unknown precision use VARCHAR to keep one stable schema across correlated executions.

The numeric ResultSet getters (`getByte`, `getShort`, `getInt`, `getLong`, `getFloat`, and `getDouble`)
convert numeric text through both column indexes and labels. They return zero for SQL NULL, with
`wasNull()` returning true. For text conversions, they follow numeric CAST rules: truncate integer
fractions toward zero, reject invalid or non-finite text with SQLState `22018`, and reject range
overflow with `22003`. Native numeric values retain Java numeric conversion behavior, including
NaN and infinity for floating-point getters. BOOLEAN values convert to one for TRUE and zero for FALSE.

`DECIMAL(p,s)`, `NUMERIC(p,s)`, and `NUMBER(p,s)` casts round to the declared scale with HALF_UP and then
reject precision overflow with SQLState `22003`. Omitting scale while specifying precision defaults to scale
zero. Bounds must satisfy `1 <= p` and `0 <= s <= p`; invalid declarations fail during statement preparation
with SQLState `42000`, including for NULL inputs and empty results. Casts without precision preserve the decimal value.

Invalid DATE, TIME, and TIMESTAMP formats raise `SQLDataException` with SQLState `22007`;
out-of-range calendar and clock fields use `22008`. Non-Boolean predicates, such as `CASE WHEN cyl`
or `NOT model`, raise `SQLSyntaxErrorException` with `42000`. These classifications and their causes
are preserved through getters, WHERE/HAVING filters, and nested query evaluation. Errors can be deferred until
iteration or value access.

Numeric arithmetic derives JDBC types before reading rows, including through derived tables and CTEs.
Whole-number `+`, `-`, `*`, `%`, and unary signs return BIGINT (`Long`); BIGINT overflow raises
SQLState `22003`. REAL or DOUBLE operands produce DOUBLE (`Double`) using double arithmetic.
Exact decimal operands produce DECIMAL (`BigDecimal`), with declared bounds reserving carry and
product digits. Non-approximate division retains `MathContext.DECIMAL64`, so `7 / 2` returns `3.5`;
its precision and scale are unknown until materialization infers them from the values, preserving
value scales such as `3.5`. NULL operands produce NULL, and
division or remainder by zero continues to return NULL. Integer literals fitting INTEGER return
`Integer`; larger literals use `Long` or exact decimal values. Unary signs promote whole-number
literals to BIGINT (`Long`), so `-1` and `+1` return `Long`. Whole-number grouping and DISTINCT
keys compare equally across these Java representations. JDBC precision is 10 for INTEGER and 19
for BIGINT; literal-specific digit bounds are retained internally for decimal arithmetic.
Decimal literals such as `1.5` are exact.
Decimal casts without declared bounds can report unknown precision until materialization infers it.

`DATE - DATE` returns the signed calendar-day difference as a Java `Long` (JDBC `BIGINT`),
including through derived tables and CTEs. For example, `DATE '2024-03-01' - DATE '2024-02-28'`
returns `2`. NULL operands produce NULL. Day counts cannot be compared with or added to
`INTERVAL` values; these operations raise SQLState `42000`. Compare against numeric days instead,
for example `d - DATE '2024-02-01' > 7`. Date/time offsets require an explicit `INTERVAL`:
`DATE '2024-01-01' + INTERVAL '1 day'` is supported; `DATE '2024-01-01' + 1` and other
unsupported temporal arithmetic raise `SQLException` with SQLState `42000` when evaluated.

Numeric casts reject non-finite inputs such as `'Infinity'`, `'-Infinity'`, and `'NaN'`, as well as invalid numeric
text, with `SQLDataException` and SQLState `22018`. Projection evaluation failures are reported as
`SQLException` with their original causes, including when retrieving values through result-set getters.
Numeric casts reject integer range overflow and floating-point overflow to infinity with
`SQLDataException` and SQLState `22003`. Integer casts truncate fractions toward zero before checking
the target range; floating-point rounding and underflow to zero retain Java conversion behavior.
`FLOAT`, including `FLOAT(p)` for any precision such as `FLOAT(53)`, always uses single precision
(Java `Float`, JDBC `REAL`). The precision argument does not select double precision. Use `DOUBLE`,
`DOUBLE PRECISION`, or `FLOAT8` for double precision; for example, cast `1e300` to `DOUBLE` rather than `FLOAT(53)`.

Unknown or unsupported scalar functions and unsupported expression types raise `SQLSyntaxErrorException`
with SQLState `42000`
when evaluated, instead of returning their SQL text as a value. Evaluation can occur during query
execution, materialization, or a result-set getter; function names are not checked by a preparation-time
allowlist. Aggregate results can be operands of arithmetic and scalar functions in SELECT, HAVING,
and ORDER BY, with or without GROUP BY. For example, `SUM(cyl) * 2`, `COALESCE(MAX(cyl), 0)`,
and `ROUND(AVG(mpg), 1)` evaluate once per group. ORDER BY and HAVING may use aggregates omitted
from SELECT. This includes aggregates in the standard `SUBSTRING(... FROM ... FOR ...)`,
`POSITION(... IN ...)`, and `TRIM(... FROM ...)` forms. Grouped `IS [NOT] DISTINCT FROM` predicates
bind grouping columns, aggregates, and subqueries with their NULL semantics intact. Grouped arithmetic
uses the same numeric and temporal rules as row expressions.
Columns outside aggregates must be grouping expressions. Scalar functions use the shared
value evaluator; remaining HAVING predicate classification differences are tracked in
[#247](https://github.com/Alipsa/JParq/issues/247). Ungrouped column references in HAVING are rejected
with SQLState `42000`, including for empty inputs. As a deliberate non-standard extension, HAVING
also accepts unqualified SELECT aliases: `SELECT SUM(mpg) AS mpg FROM mtcars m GROUP BY cyl
HAVING mpg > 200` filters on the aggregate alias. A qualified name such as `m.mpg` refers to the
input column and must satisfy the grouping rules; replacing `mpg` with `m.mpg` in that HAVING clause
raises SQLState `42000`. For standard SQL, repeat the aggregate expression: `HAVING SUM(mpg) > 200`.
Grouped ORDER BY supports scalar subqueries and
evaluates sort keys once per group. Predicate subqueries using IN, EXISTS, ANY, SOME, or ALL retain
set semantics and SQL three-valued logic inside CASE and other scalar expressions in SELECT, HAVING,
and ORDER BY. Subquery correlations in all three clauses reject ungrouped outer columns with
SQLState `42000` before reading groups, including when the outer input is empty.
Window functions in grouped queries are rejected during preparation
with SQLState `42000`; separate inner queries retain their own window-function scope.
Direct correlated scalar subqueries used as aggregate arguments still have a separate binding limitation
([#307](https://github.com/Alipsa/JParq/issues/307)).
Unary signs convert numeric text and report invalid numeric text as `SQLDataException` with SQLState `22018`;
unsupported operand types raise SQLState `42000`. Expression diagnostics preserve the SQL structure while replacing internal correlated bindings with
`<outer value>`, including bindings inside nested subqueries.
Hex literals (`X'41'`, `0x41`), including JDBC byte parameters, still return their SQL text rather than
binary values; this legacy behavior is tracked in [#250](https://github.com/Alipsa/JParq/issues/250).
Expressions skipped by filtering or CASE branch selection are not evaluated.

SQL syntax rejected by JSqlParser raises `SQLSyntaxErrorException` with SQLState `42000`. Syntax and numeric
errors retain preparation and nested-query context in their messages and cause chains.
Select-list scalar subquery failures also retain this classification when reported by `ResultSet.next()` or
projection getters. Evaluation can be deferred until iteration or value access; `executeQuery()` succeeding
does not guarantee that a scalar subquery is valid.

Escape an apostrophe in a SQL string literal by doubling it: `'O''Brien'` evaluates to `O'Brien`.
JDBC `setString` values are escaped automatically. Only untagged `$$...$$` dollar-quoted strings are supported;
tagged forms such as `$tag$...$tag$` are not enabled. Untagged strings preserve their contents literally,
including apostrophes, identifier quotes, and comment markers: `$$it's$$` returns `it's`, and
`$$a--b$$` returns `a--b`.
JDBC parameter scanning does not yet recognize dollar quotes: parameter markers inside their contents and
real parameters following quoted contents can be misidentified (tracked in [#296](https://github.com/Alipsa/JParq/issues/296)).

### Connection options

- `caseSensitive` — defaults to `false`. Set to `true` to make table-name resolution case-sensitive.
  ```text
  jdbc:jparq:/home/user/data?caseSensitive=true
  ```
- `datasetLayout` — defaults to `none` (one physical file per table). Set to `hf` to group Hugging Face shards
  and numeric directory layouts into logical tables. Values are case-insensitive (`HF` and `None` work);
  empty, whitespace-padded, and unknown values fail connection creation with the allowed values.
  ```text
  jdbc:jparq:/datasets/example?datasetLayout=hf
  ```
- Paths may be specified directly or using the `file://` prefix.
- `predicatePushdown` — defaults to `true`. Set to `false` to evaluate the entire `WHERE` clause in Java.
- `projectionPushdown` — defaults to `true`. Set to `false` to read every column, while still validating column names.
  ```text
  jdbc:jparq:/home/user/data?predicatePushdown=false&projectionPushdown=false
  ```

### Hugging Face optimized Parquet

JParq reads ordinary and Hugging Face optimized Parquet files, including files written by PyArrow with
content-defined chunking (CDC). CDC changes page boundaries; it does not require a special reader. When a file
contains a page index, JParq uses it automatically with supported predicates to skip pages within row groups.
The CLI `/describe <table>` reports row groups, row counts, and page-index availability. `/info` reports the
connection's pushdown settings.

Pushdown supports column-to-literal comparisons, `BETWEEN`, `IN` (up to 20 literals), `IS NULL`, `IS NOT NULL`,
and supported `AND`/`OR`/`NOT` combinations. Supported terms of an `AND` are pushed independently; an `OR` must
be fully supported. A case-sensitive string `LIKE 'prefix%'` can use a UTF-8 prefix range, with the exact pattern
still evaluated in Java. `ILIKE`, escaped patterns, and prefixes containing `_` remain in Java. Date, timestamp,
and exactly representable decimal literals are supported; incompatible or out-of-range bounds remain in Java.
`DISTINCT` queries can use filters too.

Floating-point values use one order everywhere, with and without pushdown, matching PostgreSQL and DuckDB:
`-Infinity` < every finite value < `+Infinity` < `NaN`, where `NaN = NaN` and `-0.0 = 0.0`. Comparisons with
`NaN` and infinities are therefore TRUE or FALSE rather than UNKNOWN, and `ORDER BY`, `MIN` and `MAX` place `NaN`
last. PyArrow leaves `NaN` out of Parquet statistics, so on `FLOAT` and `DOUBLE` columns only `<`, `<=`, `=`,
`IN` and `BETWEEN` against a non-zero literal are pushed down; `>`, `>=`, `<>`, `NOT IN` and `NOT BETWEEN` are
evaluated in Java. With Parquet 1.18, floating-point page indexes without NaN counts retain all pages for range
predicates; equality and `IN` can still prune pages, as can eligible predicates on other columns. Arithmetic on
`NaN` or infinities and `DISTINCT`/`GROUP BY` of `-0.0` versus `0.0` are not yet covered.

Single-table conditions in a join's `WHERE` can filter individual inputs. Unqualified columns must resolve to
one input. Outer-join filtering preserves NULL-extended rows, and merged `FULL JOIN ... USING` columns remain
in the final filter. The complete `WHERE` is always evaluated after the join. Predicate pushdown is disabled
for sampled inputs: `TABLESAMPLE` chooses from the unfiltered input before `WHERE`, so `REPEATABLE` samples
are independent of the pushdown setting. `ORDER BY` projections retain sorting and correlated-subquery inputs
while excluding unrelated columns such as embedded media.

Parenthesized join groups currently materialize their inputs eagerly. Outer `WHERE` predicates are evaluated
after the grouped join and are not pushed into its physical tables, so grouping can increase reads and memory
use for large inputs. Pushdown into nested groups is not yet implemented.

Nanosecond timestamps, local timestamps, float16, lists, and image structs are readable. UTC-adjusted timestamps
are returned as `java.sql.Timestamp` instants; local timestamps retain their stored wall-clock values in the
JVM's default time zone. Microsecond and nanosecond precision is preserved. List (array) columns are available
through `ResultSet.getObject` as `Object[]` values, and structs through their JSON string representation.
Shards remain separate tables by default; opt into logical multi-file tables with `datasetLayout=hf` as described
below.

Small PyArrow fixtures are committed, so Maven does not need Python. Regeneration instructions and the exact
PyArrow dependency are in `tools/createHfOptimizedData.py`.

### Hugging Face dataset layouts

The detailed requirements and verification record are in [hf-dataset-layout.md](requirements/done/hf-dataset-layout.md).

With `datasetLayout=hf`, point the URL at the dataset root to expose these layouts:

| Relative files | Logical table |
| --- | --- |
| `data/train-0-of-2.parquet`, `data/train-1-of-2.parquet` | `data.train` |
| `default/train/0000.parquet`, `default/train/0001.parquet` | `default.train` |
| `train-0-of-1-a1b2.parquet` | `PUBLIC.train` |

Direct base files belong to `PUBLIC`. An immediate child containing only numeric Parquet files and no visible
child directories becomes a `PUBLIC` table, rather than a schema. Otherwise it remains a schema, whose direct
files and eligible immediate numeric directories become tables. README files do not disqualify a directory;
a visible `partial-train/` child or nonnumeric Parquet file does. Empty directories are schemas, not tables.
All dot-prefixed entries are ignored in HF mode, including `.cache/` and `.ipynb_checkpoints/`. The explicitly
chosen base is honored even if its own name starts with a dot. The default `none` layout retains its existing behavior.

Pointing at `default/` in the example exposes `PUBLIC.train` without a `train` schema. Pointing at `data/`
exposes named splits as `PUBLIC` tables. Dataset-root `data/config/train-*.parquet` is too deep and is not
discovered; point at `data/` to expose `config.train`. There are no nested schemas or deeper traversal.
Pointing at `default/train/` itself exposes its individual `PUBLIC.0000`/`PUBLIC.0001` files.

Named shards match this exact regex (optional hexadecimal hash suffix):

```regex
^(?<split>[A-Za-z0-9_][A-Za-z0-9_.-]*)-(?<index>[0-9]+)-of-(?<total>[0-9]+)(?:-(?<hash>[0-9a-fA-F]+))?\.(?i:parquet)$
```

A group requires a positive common total and exactly one index from zero through total minus one. Missing,
duplicate (including padded/hash variants), out-of-range, and disagreeing-total shards fail discovery.
Nonmatching filenames remain ordinary single-file tables. Directory-table names match `^[0-9]+\.(?i:parquet)$`;
gaps are allowed but duplicate numeric indices are errors. Both layouts scan in numeric order (`9` before `10`).
SQL row order still requires `ORDER BY`. Bounds, DISTINCT, aggregation, windows, joins, and repeatable sampling
operate over the whole logical input. Each shard retains predicate and projection pushdown, its own physical
read schema and enriched logical types, and at most one shard reader is open per logical input.
`Statement.cancel()` applies while `executeQuery()` is aggregating, sorting, loading joins, or executing nested
queries, as well as after a result has been returned. It requests closure without waiting for an active physical
read. The executing `executeQuery()` or in-flight `next()` throws `SQLException` when cancellation is observed,
instead of returning a partial aggregate or silently reporting end of results. Cancellation exceptions use
SQLState `57014`, including cancellation inside a correlated subquery evaluated during iteration. Nested queries
share the outer cancellation state and preserve the outer result returned by `Statement.getResultSet()`, so
cancellation between subquery calls also stops iteration. The reading thread
closes its input before opening another shard. A pushed-down record filter can make the current physical read
span the rest of a shard; cancellation does not interrupt a decoder in use.

File and directory symlinks are followed, including visible `.parquet` links to extensionless cache blobs outside
the base. Names and discovery depth come from the visible paths. Dot-prefixed links are ignored; other dangling
non-Parquet links such as README are ignored unless they resolve to directories. Visible invalid `.parquet`
links fail discovery. Unlistable discovered directories fail too. HF mode rejects table-source collisions
(single file, group, or directory) and normalized schema collisions, even for empty/README-only schema directories.
Case variants collide with `caseSensitive=false`; they remain distinct with `true` on supporting filesystems.
A base directory table is assigned only the table role, so its own name does not also create a schema.

Discovery reads no footers and is catalog-wide: one incomplete group, naming collision, or broken Parquet link
fails even an unrelated table query or filtered catalog listing. Repairing it lets the next discovery succeed.
Schema validation is lazy and compares field names/order, types, nullability, containers, enum symbols, fixed
sizes, and logical-type parameters recursively. Union branch order, record names/namespaces, docs, defaults,
and nonsemantic writer metadata may differ. There is no schema widening or evolution. Each query input is
fully validated before returning rows. A prepared statement reuses its validated schemas across executions;
validation is not shared between prepared statements. An incompatible group does not prevent a healthy
physical-table query.

`getTables`, CLI `/list`, and `information_schema.tables` read remarks from the first file only, without full
shard validation. First-file footer I/O errors can still fail a listing. `getColumns` validates every shard
of every matching table; `/describe` and queries validate their selected inputs. Every `information_schema.columns`
query materializes all catalog columns before SQL `WHERE`: even a filter naming a healthy table fails if another
table is incompatible. Wildcard `getColumns(null, null, "%", "%")` has the same scope. Column requests cost a
footer read per included shard, repeated for new statements; no shared validation cache is used. A direct
`getColumns` call restricted to a healthy table remains independent. Table/column remarks come from the first
numeric file. `/describe` sums files, row groups, and unfiltered row counts and reports page indexes as `present`
(all chunks have both indexes), `absent` (none have either, including no chunks), or `partial` (anything else).

Dots and hyphens can occur in table names. Use schema-qualified SQL for dotted names:
`data."train.v2"` or `PUBLIC."train.v2"`. The existing parser does not resolve unqualified `"train.v2"`, even
with an alias. Unqualified `"train-extra"` and `"train-extra" t` work. With the default `caseSensitive=false`,
lowercase both schema and table names before quoting names obtained from listings: listed `Train-X` becomes
`data."train-x"`. Leave `PUBLIC` unquoted or use `"public"`; quoted `"PUBLIC"` does not resolve in this mode.
With `caseSensitive=true`, use exact listed spelling, such as `data."Train-X"` or `"PUBLIC"."train.v2"`.
Blindly quoting original mixed-case listed names is therefore unsuitable with the default setting.

CLI `/describe` takes literal names and escapes JDBC metadata wildcards (`_`, `%`, and the escape character)
before requesting columns. It describes one exact table, using `PUBLIC` when no schema is given. Its plain
command syntax splits at the first dot: `/describe data.train.v2` describes table `train.v2` in schema `data`.
For a dotted PUBLIC table use `/describe PUBLIC.train.v2`; `/describe train.v2` means schema `train`, table
`v2`. These command arguments use no SQL quotes. `/describe train-extra` needs no schema for a PUBLIC table.

`TableLocation` now has an immutable nonempty `List<File> files` record component. Its original single-file
constructor and `file()` accessor remain available; `file()` returns only the first file. Code inspecting record
components must account for this change, and logical-table consumers should use `files()`.

### Fat Jar

A "fat" jar, which includes all dependencies, is also created. This is useful when you want to add the driver to the classpath of a tool. The fat jar is named `jparq-x.y.z-fat.jar` i.e. fat is the maven classifier. It is available in maven central and in the [releases](https://github.com/Alipsa/JParq/releases) section.

## SQL clients

Use `jparq-x.y.z-fat.jar` as a custom/generic JDBC driver. The ordinary JAR requires all runtime dependencies;
adding that small JAR alone is insufficient for connections and queries.

| Setting | Value |
| --- | --- |
| Driver class | `se.alipsa.jparq.JParqDriver` |
| JDBC URL | `jdbc:jparq:/absolute/path/to/parquet-directory` |
| Windows URL example | `jdbc:jparq:C:/data/parquet` |
| Credentials | Leave username and password empty |
| Client JVM | Java 21 supported; Java 25 best effort; Java 17 cannot load the driver |

Check the Java runtime running the client, including any bundled JVM. JParq remains compiled for Java 21;
a scanner running on Java 17 can reject its classes before offering a driver selection. Java 25 results do not
extend Hadoop's official Java support. Use read-only, forward-only result sets when the client offers that choice.

These setup recipes use the clients' custom-driver facilities; their GUI workflows have **not** been verified.
SQL Workbench/J build 133 also has a tested headless batch integration (see the verification record).

- **DBeaver:** in Database → Driver Manager, create a generic driver named JParq. Add the fat JAR under Libraries,
  set the driver class above, and use the complete JDBC URL when creating a connection.
  See [DBeaver's driver manager documentation](https://dbeaver.com/docs/dbeaver/Driver-Manager/).
- **DbVisualizer:** create a user driver from the Custom template in Driver Manager, add the fat JAR, set Driver
  Class, and configure a connection with the JDBC URL above.
  See [DbVisualizer's installation guide](https://www.dbvis.com/docs/ug/reference-material/installing-a-jdbc-driver/).
- **SQuirreL SQL:** create a driver definition with the fat JAR on its extra classpath and the class above;
  create an alias using that driver and the complete JDBC URL.
- **SQL Workbench/J:** add a driver definition with the fat JAR and class above, then select it in a connection
  profile and enter the complete JDBC URL.

Browse schemas, tables, and columns, then try `SELECT * FROM salary LIMIT 5` when the directory contains
`salary.parquet`. Close and reconnect to check the client's full workflow. If discovery or browsing fails,
record the client version, its actual Java version, the JAR version, and the full exception. A successful
isolated JDBC probe does not establish compatibility with a specific GUI client's generated SQL or scanner.

The [compatibility verification record](requirements/done/sql-client-compatibility.md) lists tested runtimes,
platforms, loader scenarios, and remaining limits, including x64 coverage.

SQL Workbench/J can run scripts without a GUI. To include its real client loader and batch execution in the
packaged-driver tests, install it separately from the
[official download](https://sql-workbench.eu/downloads.html), build JParq, then run:

```sh
mvn -Dit.test=JParqPackagedDriverIT -Djparq.it.sqlWorkbenchJar=/path/to/sqlworkbench.jar failsafe:integration-test failsafe:verify
```

The optional test lists schemas/tables, describes columns, queries all fixture codecs, opens a second batch
session, and checks that a deliberate query failure aborts the script. Without this property it is reported
as skipped. It uses the selected `jparq.it.javaHome` runtime and needs Java 21 or later for build 133.
The client is not bundled, downloaded during Maven builds, or added to JParq's application classpath.

## JParq CLI
An interactive command line interface is available for querying Parquet files using JParq. The CLI supports command
history and basic line editing features. It also includes several commands starting with a slash (/) for controlling the CLI itself.
To start the CLI, run the following command, replacing `path/to/jparq.jar` with the actual path to the JParq fat jar and the {path to jparq dir} with the path the directory where your parquet files reside. 

```shell
java -jar path/to/jparq.jar {path to jparq dir}
```

To download the fat jar, you can either download it from [releases](https://github.com/Alipsa/JParq/releases) or use the following command:

```shell
LATEST_VERSION=$(curl -s https://repo1.maven.org/maven2/se/alipsa/jparq/maven-metadata.xml | sed -nE 's/.*<release>(.*)<\/release>.*/\1/p') && \
curl -o jparq.jar https://repo1.maven.org/maven2/se/alipsa/jparq/$LATEST_VERSION/jparq-$LATEST_VERSION-fat.jar
```
For simpler access you can create a script to it (e.g called jparq) that you put in your ~/bin dir

```shell
#!/usr/bin/env bash
java -jar /path/to/jparq.jar $@
```

Once the CLI is started, you can use the following commands:
- /connect <base directory path> - Connect to a Parquet database directory (dir with parquet files)
- /close - Close the current connection
- /list - List available tables
- /describe <table name> - Show column definitions for a table
- /info - Show information about the current connection
- /help - Display this help text
- /exit - Exit the CLI


> Anything else will be treated as a SQL query and executed against the connected dir.

Example session:
```
$ java -jar ~/lib/jparq.jar target/test-classes/acme
JParq CLI version 1.4.0
Connected to /home/user1/project/JParq/target/test-classes/acme
jparq(acme)> select * from employees e left join salary s on e.id = salary.employee
e__id | first_name | last_name  | s__id | employee | salary   | change_date
------+------------+------------+-------+----------+----------+------------
1     | Per        | Andersson  | 1     | 1        | 150000.0 | 2020-03-01 
1     | Per        | Andersson  | 6     | 1        | 160000.0 | 2021-01-01 
1     | Per        | Andersson  | 8     | 1        | 165000.0 | 2021-08-01 
2     | Karin      | Pettersson | 2     | 2        | 180000.0 | 2020-03-01 
3     | Tage       | Lundström  | 3     | 3        | 130000.0 | 2021-01-01 
3     | Tage       | Lundström  | 7     | 3        | 140000.0 | 2021-12-01 
4     | Arne       | Larsson    | 4     | 4        | 195000.0 | 2020-10-01 
5     | Sixten     | Svensson   | 5     | 5        | 230000.0 | 2020-12-15 
jparq(acme)> /exit
$
```

## JDBC support

- **Driver scope** — Read-only. Only `SELECT` statements are supported; `executeUpdate`/DML, generated keys, and updatable result sets are not implemented. Result sets are `TYPE_FORWARD_ONLY`, `CONCUR_READ_ONLY` with `CLOSE_CURSORS_AT_COMMIT` holdability.
- **Prepared statements** — SQL is planned when the `PreparedStatement` is created. Positional `?` parameters are bound via the standard `setXxx` methods and rendered as safe SQL literals (named parameters are also supported). Parameter metadata is available but reports generic `VARCHAR`/`IN` characteristics because actual typing occurs at execution. Only forward-only/read-only statements with `CLOSE_CURSORS_AT_COMMIT` holdability are accepted; overloads requesting other result-set types or generated keys/column-based key selection throw `SQLFeatureNotSupportedException`. Prepared-statement batching without a SQL argument is supported for convenience and reports `SUCCESS_NO_INFO` counts per entry. `Statement` batching and `addBatch(String)` remain unsupported.
- **Parquet predicate pushdown** — JParq attaches supported `WHERE` predicates to Parquet readers and enables statistics, column-index, dictionary, and bloom-filter pruning. Supported `AND` terms can be pushed independently, with unsupported terms evaluated in Java. See the Hugging Face optimized Parquet section above for supported operators and join/sample behavior. `JParqPreparedStatement.getPushdownInfo()` reports the attached predicate and the remaining residual filter.
- **Transactions and sessions** — The connection is always read-only with `TRANSACTION_NONE`; `commit`, `rollback`, and savepoints are no-ops. `getAutoCommit()` always returns `true`, and changing isolation level, holdability, or poolability is not supported.
- **Metadata** — `DatabaseMetaData` exposes tables and columns derived from the base directory and subdirectories (schemas); the catalog is the base directory name. Callable statements, stored procedures, and custom type mappings are not supported.
- **Other JDBC surfaces** — `CallableStatement`, `createClob`/`createBlob`/`createArrayOf`, network timeout, and client-info features are not implemented. Connection validation via `isValid()` returns `true` while the connection is open and `false` after it is closed.

### Non standard JDBC extensions
- Named parameters i.e. :paramName syntax for prepared statements

## SQL Support
JParq aims to be fully compliant with the read part of the SQL standard (SQL:2016 and earlier). 
As of version 1.1.1, the support for the standard is complete (as far as I know). The following is a detailed list of supported features and functions.

The following SQL statements are supported:
- `SELECT` with support for
  - Omitted `FROM` for literal and computed expressions, including recursive CTE anchors. The implicit input is one row; `WHERE` and row limits still apply.
    Missing input columns and projection wildcards (`*` and `table.*`) are rejected when `FROM` is omitted.
    Correlated scalar and `EXISTS` subqueries can omit `FROM`. LATERAL inputs referencing outer columns still require
    an explicit input, such as `LATERAL (SELECT e.id * 2 AS d FROM (VALUES (1)) v(z)) x`.
  - `*` to select all columns
  - Qualified wildcard projections (table.*).
  - alias support for columns and tables
  - Support computed expressions with aliases (e.g. SELECT mpg*2 AS double_mpg)
  - Quoted identifiers using "double quotes" as per the SQL standard.
  - `CASE` support. Numeric result branches built from literals, columns, numeric casts, arithmetic,
    aggregates, COALESCE, ABS, and ROUND use a common type derived before rows are read; untyped NULL
    branches do not constrain it. Selected values use that type consistently for JDBC metadata and
    `getObject()`, including grouped aggregate branches. Decimal branches share a scale when their
    bounds are known; unbounded expressions retain value-specific scales. Decimal equality keys ignore
    scale for DISTINCT, GROUP BY, and window PARTITION BY. Unselected branches are not evaluated.
    Scalar subqueries and other functions without numeric type derivation still leave CASE untyped;
    mixed numeric values can still split keys in those cases (see [#319](https://github.com/Alipsa/JParq/issues/319)).
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators
  - `AND`, `OR`, `NOT` logical operators
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Null checks: `IS NULL`, `IS NOT NULL`
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options
  - support for optional explicit NULLS FIRST and NULLS LAST
- `OFFSET` and `LIMIT` support
  - Also support the PostgreSQL shorthand `LIMIT n OFFSET m` syntax
- Standard row-limiting syntax (FETCH FIRST / OFFSET … FETCH)
- `DISTINCT` support
- Functions support
  - Date functions
    - `TIMESTAMPADD` and `TIMESTAMPDIFF` accept all nine JDBC `SQL_TSI_*` interval constants,
      including native calls and `{fn ...}` escapes, with or without `FROM`.
      `SQL_TSI_FRAC_SECOND` counts nanoseconds; other units range from seconds through years.
  - Aggregate functions (count, sum, avg, max, min)
  - CAST support
  - CONVERT support (for both character set conversion with USING and data type conversion without USING, as per SQL standard and common extensions)
  - coalesce (The COALESCE() function returns the first non-null value in a list.)
  - NULLIF(a, b) returns NULL when its arguments compare equal, otherwise the first argument, including
    when the comparison is UNKNOWN because an argument is NULL.
  - String functions (all SQL standard string function supported)
    - also support || operator for string concatenations
  - Numeric functions (abs, ceil/ceiling, floor, round, sqrt, truncate/trunc, mod, power/pow, exp, log, log10,
    rand/random, sign, sin, cos, tan, cot, asin, acos, atan, atan2, degrees, radians, pi)
  - ARRAY constructor function
    - Non-NULL scalar and array elements must share a common type and nesting depth. Numeric elements, including
      leaves across sibling nested arrays, are promoted to a common numeric type;
      mixing numbers with nested arrays, strings, or other incompatible types raises `SQLSyntaxErrorException`
      with SQLState `42000` when evaluated. NULL elements are preserved. Stored array columns and ARRAY
      constructors can be combined; nested reference arrays are normalized to lists, including in ARRAY subqueries.
    - MAP key/value types and STRUCT field schemas are not validated.
- comments (line --) and block (/* */)
- Subquery support
  - In the SELECT Clause : Used to return a single value or a set of values. e.g.
    SELECT first_name, (
    SELECT department_name FROM departments WHERE departments.department_id = employees.department_id
    ) AS department_name
    FROM employees;
  - In the FROM Clause : Treated as a derived table or inline view. E.g:
    SELECT *
    FROM (SELECT first_name, salary FROM employees WHERE salary > 5000) AS "high_salaried"
  - Derived-table column alias lists rename every output by position, including computed columns and expanded
    wildcards: `SELECT x.a, x.b FROM (SELECT id, id + 1 FROM optimized) x(a, b)`. This also applies to joins and
    LATERAL inputs. The alias count must equal the expanded output column count, including for empty results.
  - Derived tables can contain `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT`, including their `ALL` variants,
    nested set operations, and LATERAL inputs. Inner ordering and row bounds apply before the consuming query.
    Table-free SELECT and set-operation derived tables may omit a table alias in joins; reference their output
    columns by unqualified name when unambiguous. Multiple VALUES inputs still require aliases.
  - In the WHERE Clause : Used to filter the results. e.g
    SELECT first_name
    FROM employees
    WHERE department_id IN (SELECT department_id FROM departments WHERE location_id>1500);
  - In the HAVING Clause : Used to filter groups. E.g:
    SELECT department_id, AVG(salary)
    FROM employees
    GROUP BY department_id
    HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);
- `GROUP BY` support
  - `COUNT(*)` aggregation
  - `HAVING` clause with conditions
  - support aggregation functions and case statements in the `GROUP BY` and `SELECT` clause
- `EXISTS` support
- `ANY`, `SOME`, and `ALL` comparisons over subqueries and arrays, such as
  `cyl = ANY(ARRAY[4, 6])`. `SOME` is a synonym for `ANY`. Empty arrays yield FALSE for
  `ANY`/`SOME` and TRUE for `ALL`; NULL arrays or undecided comparisons with NULL elements yield UNKNOWN.
  Array-column values decode scalar element logical types, including dates, decimals, and timestamps,
  before comparison or correlation binding. Physical array and struct columns retain their types through derived tables, CTEs, and set operations,
  with matching source schemas available even for empty results. Computed arrays and arrays with differing
  set-branch schemas in non-LATERAL derived tables and CTEs infer element types
  from returned values, recursively; empty or all-NULL elements use a nullable VARCHAR element schema.
  JDBC `getObject()` returns every ARRAY value, whether from a stored column or a computed constructor, as
  `Object[]` (including mixed set-operation results), and struct columns as Avro `GenericRecord` values. Struct
  fields retain their raw Avro representations:
  strings can be `Utf8`, dates epoch-day integers, decimals `ByteBuffer`, and timestamps stored longs.
  The record's schema describes those field types; nested struct fields are not recursively decoded.
  Predicates compare ARRAY values by content, element by element, treating stored and computed representations
  alike; a NULL element orders before non-NULL elements, and a shared prefix orders the shorter array first.
  DISTINCT, GROUP BY, set-operation keys, and window PARTITION BY compare struct fields and array elements by content,
  independently of Avro record names. JSON_OBJECT and JSON_ARRAY embed struct values as JSON objects and
  SQL ARRAY values (stored columns or computed constructors) as JSON arrays, recursively. NULL elements become
  JSON `null`, and empty arrays become `[]`. Strings inside arrays and maps remain JSON strings, even when
  their text looks like JSON. This also applies to strings returned by JSON constructors inside an SQL ARRAY:
  `JSON_ARRAY(ARRAY[JSON_OBJECT(KEY 'a' VALUE 1)])` returns `[["{\"a\":1}"]]`.
  Direct JSON constructor arguments retain the existing heuristic that embeds JSON-looking text as JSON.
  Struct fields use their declared Avro logical types, including numeric decimals and date/timestamp strings.
  This schema-aware conversion includes maps nested within struct fields. Top-level map columns and maps
  inside stored arrays currently retain raw Avro logical values, such as epoch-day integers for dates;
  consistent map decoding is tracked in [#313](https://github.com/Alipsa/JParq/issues/313).
  Raw BYTES fields within structs use base64 strings. Top-level columns and stored-array elements pass through
  JDBC coercion first, so BYTES may become UTF-8 text unless `jparq.binary` prefers binary values. For example,
  the bytes of `hello` can render as `"hello"` in a column or stored array and `"aGVsbG8="` in a struct field.
  Non-finite floating-point values retain their string representations, such as `"NaN"`.
  Array columns are read correctly from both the legacy 2-level and the standard 3-level Parquet LIST layout,
  including with projection and predicate pushdown enabled.
  Quantified comparisons in `HAVING` are not currently supported; see [#214](https://github.com/Alipsa/JParq/issues/214).
- Join support: INNER, LEFT, RIGHT, FULL, CROSS, and Self Join, join ... using syntax
  - Parenthesized join groups preserve nested join order, including outer-join NULL extension.
    Unaliased groups expose their inputs' qualifiers; a group alias exposes the group's output columns instead.
    For aliased inputs, groups expose only the aliases, not the real table names.
    Qualified wildcards such as `g.*` currently require distinct output labels for any aliased join input,
    including groups, derived tables, and CTEs. Duplicate labels raise
    SQLState `42000`; assign unique positional column aliases to the input, such as `g(x, y)`, to expand them.
- `UNION` and `UNION ALL` support
- Complete set-operation coverage.
  Numeric branches use a common output type: INTEGER with BIGINT becomes BIGINT, two REAL inputs
  retain REAL and Float values, other approximate combinations use DOUBLE, and DECIMAL with exact numeric inputs retains the largest
  integer-digit count and scale. Values are converted before set comparison, and metadata preserves
  the common type and decimal bounds, including empty results. At the top level, untyped NULL adopts
  the other branch's type, and wholly untyped NULL results retain OTHER metadata. NULL typing across
  CTE and derived-table boundaries is tracked in [#288](https://github.com/Alipsa/JParq/issues/288).
  Numeric and nonnumeric branches remain incompatible. Integer literals currently use BIGINT;
  general expression typing is tracked in [#232](https://github.com/Alipsa/JParq/issues/232).
  - `EXCEPT`
  - `INTERSECT`
  - `INTERSECT ALL`
  - `EXCEPT ALL`
  - nesting of set operations
- CTE (Common Table Expressions) support
- Windowing
  - Ranking functions
    - `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE`
  -  Aggregate window functions
    - `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`
  - Analytic Value/Navigation Functions
    -  `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`
- Advanced GROUP BY constructs i.e:
  - `GROUPING SETS`
  - `ROLLUP`
  - `CUBE`
- Derived Tables: `UNNEST` with or without a table wrapper, `LATERAL` derived tables, `VALUES` table constructors
- `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.TABLES`

#### String functions support details
##### Character Length and Position
- CHAR_LENGTH(string) or CHARACTER_LENGTH(string)       Returns number of characters in a string.       CHAR_LENGTH('hello') → 5
- OCTET_LENGTH(string)  Returns number of bytes in the string (depends on encoding).    OCTET_LENGTH('Å') → 2 (in UTF-8)
- POSITION(substring IN string) Finds the position (1-based) of substring in string.    POSITION('l' IN 'hello') → 3

##### Substrings and Extraction
- SUBSTRING(string FROM start [FOR length])     Extracts substring starting at start, optionally limited by length.     SUBSTRING('abcdef' FROM 2 FOR 3) → 'bcd'
- LEFT(string, count) (optional extension)      Leftmost characters.    LEFT('abcdef', 3) → 'abc'
- RIGHT(string, count) (optional extension)     Rightmost characters.   RIGHT('abcdef', 2) → 'ef'

##### Concatenation
- CONCAT(string1, string2, …)   Concatenates two or more strings (SQL:2016 added variadic support).     CONCAT('a','b','c') → 'abc'
- Support for the || operator, covering both binary and text concatenation, as well as null propagation.

##### Case Conversion
- UPPER(string) Converts to uppercase.  UPPER('sql') → 'SQL'
- LOWER(string) Converts to lowercase.  LOWER('SQL') → 'sql'

##### Trimming and Padding
- TRIM([LEADING TRAILING        BOTH] [characters] FROM string)
- LTRIM(string) (extension)     Trims leading spaces.   LTRIM(' hi') → 'hi'
- RTRIM(string) (extension)     Trims trailing spaces.  RTRIM('hi ') → 'hi'
- LPAD(string, length [, fill]) (SQL:2008 optional)     Pads string on the left.        LPAD('42', 5, '0') → '00042'
- RPAD(string, length [, fill]) (SQL:2008 optional)     Pads string on the right.       RPAD('42', 5, '0') → '42000'

##### Searching and Replacing
- OVERLAY(string PLACING replacement FROM start [FOR length])   Replaces part of string starting at start with replacement.    OVERLAY('abcdef' PLACING 'xyz' FROM 3 FOR 2) → 'abxyze f'
- REPLACE(string, search, replace) (SQL:2008)   Replaces all occurrences of search with replace.        REPLACE('banana', 'na', 'xy') → 'baxyxy'

##### Collation and Comparison
- COLLATE(string, collation_name)       Applies a specific collation to a string.       'abc' COLLATE "sv_SE"
- SIMILAR TO e.g.
  'cat' SIMILAR TO '(cat|dog)'      → TRUE
  'cab' SIMILAR TO 'c(a|o)b'        → TRUE
  'cab' SIMILAR TO 'c(a|e)b'        → FALSE
  'abc' SIMILAR TO 'a%'             → TRUE
- REGEXP_LIKE (pattern matching operators) e.g:
  REGEXP_LIKE('abc123', '^[a-z]+[0-9]+$')   → TRUE
  REGEXP_LIKE('AbC', 'abc', 'i')            → TRUE  -- 'i' = case-insensitive
  REGEXP_LIKE('cat', 'dog|cat')             → TRUE

##### Unicode and Codepoints
- CHAR(code)    Returns the character corresponding to a code point.    CHAR(65) → 'A'
- UNICODE(string)       Returns Unicode code point of first character.  UNICODE('A') → 65

##### SQL:2016–2023 Additions
- NORMALIZE(string [USING form])        Normalizes Unicode text (SQL:2016).     NORMALIZE('é') → 'é'
- STRING_AGG(expression, separator)     Aggregates values into a single string with a separator.        STRING_AGG(name, ', ') → 'Alice, Bob, Carol'
- JSON_VALUE, JSON_QUERY, JSON_OBJECT, JSON_ARRAY       JSON construction/extraction—technically not core string functions but string-returning functions standardized in SQL:2016–2023.

`JSON_VALUE` and `JSON_QUERY` support double-quoted path member names, including spaces,
dots, brackets, and JSON string escapes such as `\"`, `\\`, and `\u0061`.
For example, `JSON_VALUE('{"a b":1}', '$."a b"')` returns `1`.
Quoted members require a leading dot: use `$."a"`, while `$"a"` raises an error.
Plain spaces (U+0020) after quoted members are accepted, for example `$."a" [1]` and `$."a" .b`.
Missing members return NULL for supported paths. Malformed quoted members and unsupported array
subscripts raise `SQLException` when evaluated, even after a missing member. Subscripts must be
zero-based integer indexes. An optional `+` sign is accepted (`[+1]` selects the second element).
Negative indexes such as `[-1]` always return NULL; they do not count backward from the end of the array.
Subscripts trim surrounding characters U+0000–U+0020, including tabs and newlines;
tabs and newlines after quoted members are rejected.
`[*]`, `[last]`, and ranges remain unsupported
([#303](https://github.com/Alipsa/JParq/issues/303)). For example, `$.missing[*]` now raises
`Unsupported array subscript in JSON path: [*]` instead of returning NULL.

**Behavior change:** `$."a"` now matches the key `a`, rather than a key containing literal quotes
(`"a"`). To select a key containing literal quotes, escape them in the path: `$."\"a\""`.

##### Non-standard extensions
- ASCII(string)    Returns the ASCII code of the first character.    ASCII('A') → 65
- LOCATE(substring, string[, start])    Locates substring in string with optional start position.    LOCATE('c', 'abcabc', 3) → 3
- REPEAT(string, count)    Repeats the string N times.    REPEAT('a', 4) → 'aaaa'
- SPACE(count)    Creates a string of N space characters.    SPACE(5) → '     '
- INSERT(string, start, length, replacement)    Inserts replacement at position after removing length characters.    INSERT('abcdef', 3, 2, 'XYZ') → 'abXYZef'
- SOUNDEX(string)    Computes the Soundex phonetic code.    SOUNDEX('Robert') → 'R163'
- DIFFERENCE(string1, string2)    Calculates similarity based on Soundex codes (0-4).    DIFFERENCE('Smith', 'Smyth') → 4

### Build and test

```bash
mvn verify
```
- To run without spotless, add -Dspotless.check.skip=true
- To skip unit tests, add -DskipTests
- To skip integration tests add -DskipITs=true

This project uses Checkstyle, PMD, and Spotless. The checks run automatically as part of the Maven build. Use the
`spotless:apply` goal before committing if you need to fix formatting issues.

### Release notes

See [release.md](release.md) for the full version history and work in progress.

## Roadmap: _Might_ be implemented in the future

### Non standard extensions
- Support for variable assignment and use within SQL scripts.
  - @variable_name syntax to define a variable that exists for the duration of the connection
    - Example (direct assignment, connection scope):
      declare @myVar INT = 10;
      SELECT * FROM myTable WHERE myColumn > @myVar;
      SELECT * FROM anotherTable LIMIT @myVar;
- PIVOT and UNPIVOT operators.
- TABLESAMPLE clause for sampling rows from a table.
- Support for modular encryption via a keystore and/or a Key Management Service

## Out of scope (will not be supported, at least not in the foreseeable future)
- Data modification statements (INSERT, UPDATE, DELETE, MERGE)
- Transaction control (COMMIT, ROLLBACK, SAVEPOINT)
- Data definition statements (CREATE, ALTER, DROP, TRUNCATE)
- User management and security (GRANT, REVOKE, CREATE USER, etc.)
- Stored procedures and functions (CREATE PROCEDURE, CREATE FUNCTION)
- Triggers (CREATE TRIGGER, DROP TRIGGER)
- Advanced indexing and optimization hints
- Full-text search capabilities
- TEMPORARY TABLES, you need to use CTE's or value tables instead.
