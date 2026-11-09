# SQL reference

This page describes the SQL that JParq supports and how it behaves in detail. For an overview, see the
[README](../README.md). For Parquet-specific behavior such as predicate pushdown, see
[Parquet files and Hugging Face datasets](parquet-and-hugging-face.md).

JParq follows the SQL standard (SQL:2016 and earlier) for read queries. Where it deviates, the deviation is a bug and
is listed under [known deviations and open issues](#known-deviations-and-open-issues).

## Contents

- [Supported statements and clauses](#supported-statements-and-clauses)
- [Functions](#functions)
- [Name resolution](#name-resolution)
- [Subqueries](#subqueries)
- [Grouping and aggregation](#grouping-and-aggregation)
- [CTEs, derived tables, LATERAL, and UNNEST](#ctes-derived-tables-lateral-and-unnest)
- [Set operations](#set-operations)
- [Data types](#data-types)
- [Literals and parameters](#literals-and-parameters)
- [JDBC result values](#jdbc-result-values)
- [Statement cancellation](#statement-cancellation)
- [Errors and SQLStates](#errors-and-sqlstates)
- [Known deviations and open issues](#known-deviations-and-open-issues)

---

## Supported statements and clauses

### SELECT

- `*` and qualified wildcards (`table.*`)
- Column and table aliases, including computed expressions with aliases (`SELECT mpg * 2 AS double_mpg`)
- Quoted identifiers using "double quotes", as the SQL standard specifies
- `DISTINCT`
- `CASE` expressions; see [CASE](#case)
- Line (`--`) and block (`/* */`) comments

**Omitted `FROM`.** Literal and computed expressions can omit `FROM`, including recursive CTE anchors. The implicit
input is one row; `WHERE` and row limits still apply.

- Column references and wildcards (`*` and `table.*`) are rejected when `FROM` is omitted.
- Correlated scalar and `EXISTS` subqueries can omit `FROM`.
- `LATERAL` inputs that reference outer columns still require an explicit input, such as
  `LATERAL (SELECT e.id * 2 AS d FROM (VALUES (1)) v(z)) x`.

### WHERE

- `BETWEEN`, `IN`, `LIKE`, and `SIMILAR TO`
- `AND`, `OR`, and `NOT`
- Comparison operators `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- `IS NULL` and `IS NOT NULL`
- `EXISTS`, `IN`, and quantified comparisons (`ANY`, `SOME`, `ALL`) over subqueries and arrays; see
  [quantified comparisons](#quantified-comparisons)

Expressions skipped by filtering or `CASE` branch selection are not evaluated.

### ORDER BY

Multiple sort keys with `ASC`/`DESC` and optional `NULLS FIRST`/`NULLS LAST`. A bare output name resolves
against the SELECT list first. Inside an expression, input columns take precedence, including when a SELECT
alias has the same name. As an extension, an otherwise unresolved name may refer to a deterministic SELECT
alias, including grouped aggregates (for example,
`SELECT cyl, SUM(mpg) AS s FROM mtcars GROUP BY cyl ORDER BY s * 2`).
Expression references to aliases containing window functions, RAND/RANDOM, or subqueries are rejected with
SQLState `42000`; use a derived table or CTE to expose their computed values as input columns. See
[column labels and ambiguity](#column-labels-and-ambiguity) for how output names are resolved.

### Row limiting

- Standard `OFFSET ... FETCH FIRST/NEXT ... ROWS ONLY`
- `LIMIT` and `OFFSET`, including the PostgreSQL shorthand `LIMIT n OFFSET m`
- MySQL-style `LIMIT offset, count` is rejected with SQLState `0A000`; use `OFFSET` and `LIMIT`.

Row counts accept non-negative integer literals and bound JDBC parameters up to `2147483647`.

| Row count | Result |
| --- | --- |
| Integral numeric parameters such as `2.0` and `BigDecimal("2.00")` | Accepted |
| Character parameters such as `"2"` or `'2'` | Accepted |
| Invalid numeric strings | SQLState `22018` |
| Other non-numeric parameters | `2201X` for `OFFSET`, `2201W` for `LIMIT`/`FETCH` |
| Values above `2147483647` | `22003` |
| Negative counts, and NULL, fractional, or non-finite numeric parameters | `SQLDataException`: `2201X` for `OFFSET`, `2201W` for `LIMIT`/`FETCH` |
| Other expressions (bitwise NOT, arithmetic, subqueries, literal `NULL`) | `42000` |

### Joins

`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, and self joins, with `ON` or `USING`. `ON` conditions can reference only
columns from inputs joined so far.

**`JOIN ... USING (x)`**

- Unqualified `x` and `SELECT *` expose one merged key.
- Qualified references such as `a.x` and `b.x`, including `a.*` and `b.*`, keep their input values and return NULL
  when that input is absent from an outer-join row.
- Later joins using `x` match against the accumulated merged key.
- Grouping by merged `x` does not group the separate input keys `a.x` or `b.x`; select those only when they are
  also grouped or aggregated.
- `SELECT x, a.x ... ORDER BY x` has an ambiguous output label; use distinct aliases or a positional reference.

**Parenthesized join groups**

- Nested join order is preserved, including outer-join NULL extension.
- Unaliased groups expose their inputs' qualifiers. A group alias exposes the group's output columns instead.
- For aliased inputs, groups expose only the aliases, not the real table names.
- Qualified wildcards such as `g.*` currently require distinct output labels for any aliased join input, including
  groups, derived tables, and CTEs. Duplicate labels raise SQLState `42000`; give the input unique positional column
  aliases, such as `g(x, y)`, to expand them.
- See [predicate pushdown](parquet-and-hugging-face.md#joins-sampling-and-projections) for performance notes.

### Subquery support

Subqueries are supported in the `SELECT`, `FROM`, `WHERE`, and `HAVING` clauses:

```sql
-- SELECT clause: returns a single value
SELECT first_name,
       (SELECT department_name FROM departments
        WHERE departments.department_id = employees.department_id) AS department_name
FROM employees;

-- FROM clause: a derived table
SELECT *
FROM (SELECT first_name, salary FROM employees WHERE salary > 5000) AS "high_salaried";

-- WHERE clause: filters rows
SELECT first_name
FROM employees
WHERE department_id IN (SELECT department_id FROM departments WHERE location_id > 1500);

-- HAVING clause: filters groups
SELECT department_id, AVG(salary)
FROM employees
GROUP BY department_id
HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);
```

Derived tables can contain `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` (including `ALL` variants), nested set
operations, and `LATERAL` inputs. Inner ordering and row bounds apply before the consuming query. Table-free
`SELECT` and set-operation derived tables may omit a table alias in joins; reference their output columns by
unqualified name when unambiguous. Multiple `VALUES` inputs still require aliases.

See [Subqueries](#subqueries) for scoping and evaluation rules.

### GROUP BY and HAVING

- Aggregates, `COUNT(*)`, and `CASE` expressions in `GROUP BY` and `SELECT`
- `HAVING` with conditions, subqueries, and quantified comparisons
- `GROUPING SETS`, `ROLLUP`, and `CUBE`

See [Grouping and aggregation](#grouping-and-aggregation).

### Set operation support

`UNION`, `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`, and nested set operations. See
[Set operations](#set-operations).

### Common table expressions

`WITH` and `WITH RECURSIVE`. See [CTEs, derived tables, LATERAL, and UNNEST](#ctes-derived-tables-lateral-and-unnest).

### Window functions

| Kind | Functions |
| --- | --- |
| Ranking | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE` |
| Aggregate | `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` |
| Navigation | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` |

- Unsupported function names used with `OVER` in `SELECT` lists raise `SQLSyntaxErrorException` with SQLState
  `42000`, naming the function. Validation outside `SELECT` lists is tracked in
  [#350](https://github.com/Alipsa/JParq/issues/350).
- `WITHIN GROUP` clauses in `SELECT` lists are unsupported, with or without `OVER`, and raise SQLState `42000`
  ([#473](https://github.com/Alipsa/JParq/issues/473)).
- Window functions in grouped queries are rejected during preparation with SQLState `42000`; separate inner queries
  keep their own window function scope.

### Table sources

- Parquet tables, with optional schema qualification
- Derived tables, `LATERAL` derived tables, `UNNEST` (with or without a table wrapper), and `VALUES` table
  constructors
- `TABLESAMPLE ... REPEATABLE (...)`; see [predicate pushdown](parquet-and-hugging-face.md#joins-sampling-and-projections)
- `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`

---

## Functions

| Category | Functions |
| --- | --- |
| Aggregate | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG` |
| Numeric | `ABS`, `CEIL`/`CEILING`, `FLOOR`, `ROUND`, `SQRT`, `TRUNCATE`/`TRUNC`, `MOD`, `POWER`/`POW`, `EXP`, `LOG`, `LOG10`, `RAND`/`RANDOM`, `SIGN`, `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`, `PI` |
| Conditional | `CASE`, `COALESCE`, `NULLIF` |
| Conversion | `CAST`, `CONVERT` |
| Date and time | `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `EXTRACT`, `TIMESTAMPADD`, `TIMESTAMPDIFF` |
| Constructors | `ARRAY`, `JSON_OBJECT`, `JSON_ARRAY` |
| JSON | `JSON_VALUE`, `JSON_QUERY` |
| String | See [string functions](#string-functions) |

Unknown or unsupported scalar functions and expression types raise `SQLSyntaxErrorException` with SQLState `42000`
when evaluated, instead of returning their SQL text as a value. Function names are not checked against an allow
list during preparation, so the error can occur during execution, materialization, or a result set getter.

### NULLIF and COALESCE

- `COALESCE(a, b, ...)` returns the first non-null argument.
- `NULLIF(a, b)` returns NULL when its arguments compare equal, otherwise the first argument. This includes the case
  where the comparison is UNKNOWN because an argument is NULL.

### CAST and CONVERT

- `CONVERT(... USING charset)` performs character set conversion, as in the SQL standard.
- Without `USING`, cast-style `CONVERT` performs data type conversion (a common extension) and shares `CAST`'s
  declaration checks. Use target-first syntax for NULL inputs; see
  [#465](https://github.com/Alipsa/JParq/issues/465).
- See [Data types](#data-types) for cast rules per type.

### TIMESTAMPADD and TIMESTAMPDIFF

Both accept all nine JDBC `SQL_TSI_*` interval constants, as native calls or `{fn ...}` escapes, with or without
`FROM`. `SQL_TSI_FRAC_SECOND` counts nanoseconds; the other units range from seconds through years.

### String functions

`||` concatenates strings and binary values, with NULL propagation.

| Function | Description | Example |
| --- | --- | --- |
| `CHAR_LENGTH(s)`, `CHARACTER_LENGTH(s)` | Number of characters | `CHAR_LENGTH('hello')` → `5` |
| `OCTET_LENGTH(s)` | Number of bytes (UTF-8 for text) | `OCTET_LENGTH('Å')` → `2` |
| `POSITION(sub IN s)` | 1-based position of `sub` in `s` | `POSITION('l' IN 'hello')` → `3` |
| `SUBSTRING(s FROM start [FOR length])` | Substring | `SUBSTRING('abcdef' FROM 2 FOR 3)` → `'bcd'` |
| `CONCAT(s1, s2, ...)` | Concatenates two or more strings | `CONCAT('a', 'b', 'c')` → `'abc'` |
| `UPPER(s)`, `LOWER(s)` | Case conversion | `UPPER('sql')` → `'SQL'` |
| `TRIM([LEADING \| TRAILING \| BOTH] [chars] FROM s)` | Removes characters from the ends | `TRIM(BOTH 'x' FROM 'xaxx')` → `'a'` |
| `LPAD(s, length [, fill])` | Pads on the left (SQL:2008 optional) | `LPAD('42', 5, '0')` → `'00042'` |
| `RPAD(s, length [, fill])` | Pads on the right (SQL:2008 optional) | `RPAD('42', 5, '0')` → `'42000'` |
| `OVERLAY(s PLACING r FROM start [FOR length])` | Replaces part of `s` with `r` | `OVERLAY('abcdef' PLACING 'xyz' FROM 3 FOR 2)` → `'abxyzef'` |
| `REPLACE(s, search, replacement)` | Replaces all occurrences (SQL:2008) | `REPLACE('banana', 'na', 'xy')` → `'baxyxy'` |
| `s COLLATE collation` | Applies a collation | `'abc' COLLATE "sv_SE"` |
| `s SIMILAR TO pattern` | SQL regular expression match | `'cab' SIMILAR TO 'c(a\|o)b'` → `TRUE` |
| `REGEXP_LIKE(s, pattern [, flags])` | Regular expression match; flag `'i'` is case-insensitive | `REGEXP_LIKE('AbC', 'abc', 'i')` → `TRUE` |
| `NORMALIZE(s [USING form])` | Unicode normalization (SQL:2016) | `NORMALIZE('é')` → `'é'` |
| `STRING_AGG(expr, separator)` | Aggregates values into one string | `STRING_AGG(name, ', ')` → `'Alice, Bob, Carol'` |
| `UNICODE(s)` | Code point of the first character | `UNICODE('A')` → `65` |
| `CHAR(code)` | Character for a code point | `CHAR(65)` → `'A'` |

More `SIMILAR TO` and `REGEXP_LIKE` examples:

```sql
'cat' SIMILAR TO '(cat|dog)'              -- TRUE
'cab' SIMILAR TO 'c(a|e)b'                -- FALSE
'abc' SIMILAR TO 'a%'                     -- TRUE
REGEXP_LIKE('abc123', '^[a-z]+[0-9]+$')   -- TRUE
REGEXP_LIKE('cat', 'dog|cat')             -- TRUE
```

`STRING_AGG ... WITHIN GROUP (ORDER BY ...)` is unsupported and raises SQLState `42000`
([#473](https://github.com/Alipsa/JParq/issues/473)).

`LENGTH` is an alias of the character-only `CHAR_LENGTH`. For binary operands, see
[character and binary types](#character-and-binary-types).

**Non-standard extensions:**

| Function | Description | Example |
| --- | --- | --- |
| `LEFT(s, n)` | Leftmost characters | `LEFT('abcdef', 3)` → `'abc'` |
| `RIGHT(s, n)` | Rightmost characters | `RIGHT('abcdef', 2)` → `'ef'` |
| `LTRIM(s)` | Trims leading spaces | `LTRIM(' hi')` → `'hi'` |
| `RTRIM(s)` | Trims trailing spaces | `RTRIM('hi ')` → `'hi'` |
| `ASCII(s)` | ASCII code of the first character | `ASCII('A')` → `65` |
| `LOCATE(sub, s [, start])` | Position of `sub`, from an optional start | `LOCATE('c', 'abcabc', 3)` → `3` |
| `REPEAT(s, n)` | Repeats a string | `REPEAT('a', 4)` → `'aaaa'` |
| `SPACE(n)` | String of `n` spaces | `SPACE(5)` → `'     '` |
| `INSERT(s, start, length, r)` | Replaces `length` characters at `start` with `r` | `INSERT('abcdef', 3, 2, 'XYZ')` → `'abXYZef'` |
| `SOUNDEX(s)` | Soundex phonetic code | `SOUNDEX('Robert')` → `'R163'` |
| `DIFFERENCE(s1, s2)` | Soundex similarity (0–4) | `DIFFERENCE('Smith', 'Smyth')` → `4` |

### JSON functions

`JSON_VALUE`, `JSON_QUERY`, `JSON_OBJECT`, and `JSON_ARRAY` are string-returning functions standardized in
SQL:2016–2023.

**Paths in `JSON_VALUE` and `JSON_QUERY`:**

- Double-quoted member names are supported, including spaces, dots, brackets, and JSON string escapes such as `\"`,
  `\\`, and `\u0061`. For example, `JSON_VALUE('{"a b":1}', '$."a b"')` returns `1`.
- All member steps require a leading dot: use `$.a` or `$."a"`; `$a`, `$ a`, `$"a"`,
  and `$.a[0]b` raise errors. Whitespace between steps does not replace the dot.
- Leading and trailing whitespace and whitespace between path tokens are ignored. The parser uses ECMAScript
  [WhiteSpace](https://262.ecma-international.org/15.0/#sec-white-space) and
  [LineTerminator](https://262.ecma-international.org/15.0/#sec-line-terminators) characters:
  Unicode space separators (including NBSP), tab, vertical tab,
  form feed, LF, CR, U+2028, U+2029, and U+FEFF. Other control characters are not whitespace. For example,
  `$.a .b`, `$.a [1]`, and `$. "a"` work like their compact forms. Whitespace inside quoted names is preserved;
  unquoted names containing whitespace must be quoted (`$."a b"`, not `$.a b`).
- `$."a"` matches the key `a`. To select a key containing literal quotes, escape them: `$."\"a\""`.
  (**Behavior change:** earlier versions matched a key containing the literal quotes.)
- A dot must be followed by a member name; empty names must be quoted (`$.""`).
- Missing members return NULL for supported paths.
- Numeric array indexes are zero-based integers. An optional `+` sign is accepted (`[+1]` selects the second
  element). Negative indexes such as `[-1]` always return NULL; they do not count from the end of the array.
  Subscripts ignore the same surrounding whitespace as other path tokens.
- In the default lax behavior, array steps wrap non-array values as one-element arrays. For example,
  `$.a[*]`, `$.a[0]`, `$.a[last]`, and `$.a[0 to 2]` all select `1` from `{"a":1}`.
- Array steps support `[*]` (all elements), `[last]` (the final element), offsets such as `[last-1]`,
  inclusive ranges such as `[0 to 2]` and `[last-1 to last]`, and comma-separated selections such as `[0,last]`.
  Numeric literals of any length are accepted and parsed with saturating arithmetic; endpoints outside the array
  are clipped, and descending ranges have no matches. Whitespace is allowed
  between subscript tokens, including around the sign in `last - 1`.
- Subsequent steps apply to every selected item. Both functions return NULL if the complete path selects zero
  or multiple items (the default SQL/JSON behavior without an array wrapper). A single selected JSON array
  remains one item and is returned intact by `JSON_QUERY`.
- Intermediate selections are limited to 100,000 items per path step, including duplicates. From the first
  nonterminal array step onward, item/range evaluations share one cumulative budget over the complete path,
  including terminal steps and ranges that select nothing. The cumulative budget defaults to 100,000.
  For example, `$[*][0].x` on 100,000 elements exceeds the default work budget: `[*]` costs 1 and `[0]` costs 100,000.
  Set the JVM system property `-Djparq.json.maxIntermediateItems=200000` to change both limits to a positive
  integer. The property is read only at the first nonterminal array step and reused for the remainder of that
  evaluation. Paths with no intermediate array step do not consult the property. This process-wide budget
  covers JDBC and direct JSON function calls; it is not a connection property.
  Exceeding either limit raises `SQLException` with SQLState `54000`; an invalid setting uses `HY024`. Both
  diagnostics include the property name. Each range's size is checked before its items are collected;
  nonterminal item/range combinations are checked against the remaining cumulative budget before iteration.
  A terminal array step needs at most two items to determine whether the result is a singleton and does not
  expand the rest of the selection.
- Numeric subscript literals accept ASCII digits only. Leading-zero validation remains tracked in
  [#496](https://github.com/Alipsa/JParq/issues/496).
- Malformed quoted members and invalid subscripts raise `SQLException` when evaluated, even after a missing
  member. Invalid subscripts include the bracket contents in the diagnostic.

**Constructors (`JSON_OBJECT`, `JSON_ARRAY`):**

- Struct values are embedded as JSON objects, and SQL `ARRAY` values (stored or constructed) as JSON arrays,
  recursively. NULL elements become JSON `null`, and empty arrays become `[]`.
- Strings inside arrays and maps remain JSON strings, even when their text looks like JSON. This includes strings
  returned by JSON constructors inside an SQL `ARRAY`: `JSON_ARRAY(ARRAY[JSON_OBJECT(KEY 'a' VALUE 1)])` returns
  `[["{\"a\":1}"]]`.
- Direct constructor arguments keep the heuristic that embeds JSON-looking text as JSON.
- Struct fields use their declared Avro logical types, including numeric decimals and date/timestamp strings. This
  includes maps nested within struct fields. Top-level map columns and maps inside stored arrays currently keep raw
  Avro logical values, such as epoch-day integers for dates
  ([#313](https://github.com/Alipsa/JParq/issues/313)).
- Raw `BYTES` fields within structs become base64 strings. Top-level columns and stored array elements pass through
  JDBC coercion first, so `BYTES` may become UTF-8 text unless `jparq.binary` prefers binary values. For example,
  the bytes of `hello` render as `"hello"` in a column or stored array and as `"aGVsbG8="` in a struct field.
- Non-finite floating-point values keep their string representations, such as `"NaN"`.

---

## Name resolution

### Identifiers and case

- Function names and unquoted identifiers are case-insensitive. Quoted identifiers and string literal contents keep
  their case.
- Quoted lowercase column references resolve to unquoted lowercase columns:
  `SELECT "id" + 0 FROM optimized WHERE "id" = 1`. This also applies to `GROUP BY`, derived tables, CTEs, `VALUES`
  inputs, and computed join expressions with qualified duplicate column names.
- Quoted mixed-case names require an exact case match.
- Unquoted references to explicitly quoted lowercase materialized aliases, such as
  `SELECT x FROM (SELECT 1 AS "x") t`, are not supported yet ([#338](https://github.com/Alipsa/JParq/issues/338)).
- Lowercase CTE names can be declared or referenced with quotes, including in correlated subqueries:
  `WITH "c" AS (SELECT 1 AS x) SELECT x FROM c`.
- Equivalent CTE names such as `c` and `"c"` in the same `WITH` list are rejected with SQLState `42000`. A nested
  `WITH` shadows an outer declaration in either form, including in scalar subqueries and later CTE declarations in
  that nested scope.
- Doubled quotes in quoted identifiers are decoded: `"a""b"` has the label `a"b`.
- Table name case sensitivity is controlled by the `caseSensitive` connection option.

### Column labels and ambiguity

- Quoted column aliases keep their case and appear in JDBC column labels without the surrounding quotes. Derived
  tables and CTEs preserve these labels, including spaces.
- Unaliased expressions are labeled with their SQL text, including quoted operands such as `"id" + 1`. Derived
  tables and CTEs preserve these labels too (for example `id + 1`). Internal Avro names do not restrict SQL output
  labels, and generated internal names are not SQL column aliases.
- Ambiguous references to duplicate output names in projections, predicates, and `ORDER BY` raise an exception with
  SQLState `42702` naming the column, even for empty inputs. Qualify join columns with a table alias to resolve
  them; duplicate outputs remain available by position through `SELECT *` and positional `ORDER BY`.
- `ORDER BY` can use a repeated `SELECT` label when every occurrence denotes the same expression. Different
  expressions with the same label are ambiguous, including columns contributed by qualified wildcards.
- Column equivalence uses the bound input field, so an unqualified reference and its qualified form can share a
  label.
- Unused duplicate output labels do not prevent ordering by another column.

### Table aliases and qualifiers

- A table alias hides the original table or CTE name everywhere: `SELECT` (including `alias.*`), `WHERE`,
  `GROUP BY`, `HAVING`, `ORDER BY`, `DISTINCT ON`, `WINDOW`, `QUALIFY`, `ON` conditions, and correlated subqueries.
  After `FROM employees e`, write `e.id`.
- Quoted table aliases such as `"My T"` are supported, including at derived table and CTE boundaries. Quoted aliases
  containing dots remain single identifiers, such as `"a.b".id` after `FROM employees "a.b"`.
- Schema prefixes are checked against unaliased inputs: `acme.employees.id` is valid after `FROM acme.employees`,
  while `nosuch.employees.id` and schema prefixes on aliases are rejected.
- Unknown or hidden relation qualifiers raise `SQLSyntaxErrorException` with SQLState `42000`, even when the input is
  empty or predicates are pushed down.
- Qualifiers in nested queries are validated during preparation against local and enclosing inputs, even when the
  outer query returns no rows or a scalar subquery result is never read.

### Column validation in derived tables and CTEs

- Column references are validated against the visible output names, even for empty inputs.
- Unknown names, and columns replaced by a column alias list, raise `SQLSyntaxErrorException` with SQLState `42000`
  naming the missing column. Generated internal Avro names are not visible SQL columns.
- Non-parameterized single-input queries are validated during preparation; joins are validated when their inputs are
  built during execution.

### Column alias lists

Column alias lists rename outputs by position:

```sql
SELECT x.a, x.b FROM (SELECT id, id + 1 FROM optimized) x(a, b);  -- derived table
SELECT * FROM c AS x(a, b);                                         -- CTE reference
SELECT * FROM (c) x(a, b);                                          -- parenthesized CTE reference
SELECT a FROM employees e(a, b, c);                                 -- Parquet table
```

- Derived table lists cover every output, including computed columns and expanded wildcards, and also work in joins
  and `LATERAL` inputs.
- CTE reference lists work with wildcard output, grouping, recursive CTEs, joins, and nested queries.
- On a Parquet table, `employees e(a, b, c)` exposes `id, first_name, last_name` as `a, b, c` and hides the
  original names, in plain queries and joins.
- The alias count must equal the output (or table) column count, including for empty results. Mismatches raise
  `SQLSyntaxErrorException` with SQLState `42000`.
- Alias lists on `information_schema.tables` and `information_schema.columns` are rejected; wrap them in a derived
  table or CTE to rename their columns.

---

## Subqueries

### Scope

- Unqualified column names resolve in the subquery's inner scope first, then in the outer row or grouping context
  if no inner input has a matching column. This also applies to correlations in `HAVING`.
- Virtual `information_schema` tables use their fixed schemas for name binding.
- If an inner input's columns cannot be determined, unqualified names stay inner; use an explicit qualifier for
  outer references.
- `WITH` declarations inside subqueries, including derived tables, have local scope and override physical tables or
  inherited CTEs with the same name.

### Scalar subqueries

- A scalar subquery must return exactly one column and at most one row. An empty one-column result is NULL.
- Violations raise `SQLSyntaxErrorException` with SQLState `42000` in scalar expressions and `HAVING`, including
  empty results with multiple columns.
- Failures in select-list scalar subqueries keep this classification when reported by `ResultSet.next()` or
  getters. Evaluation can be deferred until iteration or value access, so a successful `executeQuery()` does not
  guarantee that a scalar subquery is valid.

### Predicate subqueries

- Single-value `IN`/`NOT IN` and `ANY`/`SOME`/`ALL` subqueries require exactly one column, even for empty results.
  Extra columns raise `SQLSyntaxErrorException` with SQLState `42000` in `WHERE` and `HAVING`.
- Column counts are validated during planning, before predicate pushdown or empty grouping can skip evaluation.
- These predicates allow multiple rows. `EXISTS` allows multiple columns.
- `EXISTS` and `NOT EXISTS` test row existence, including in correlated subqueries, `HAVING` predicates, and set
  operations.
- `IN`, `EXISTS`, `ANY`, `SOME`, and `ALL` keep set semantics and SQL three-valued logic inside `CASE` and other
  scalar expressions in `SELECT`, `HAVING`, and `ORDER BY`.

### Quantified comparisons

`ANY`, `SOME`, and `ALL` work over subqueries and arrays, such as `cyl = ANY(ARRAY[4, 6])`, in `WHERE` and `HAVING`
(including aggregate values and correlated grouping columns).

- `SOME` is a synonym for `ANY`.
- Empty inputs yield FALSE for `ANY`/`SOME` and TRUE for `ALL`.
- NULL arrays, and undecided comparisons involving NULL elements, yield UNKNOWN.
- Candidates must be scalar; array, map, and struct candidates from arrays or subqueries raise an error.
- Array column values decode scalar element logical types, including dates, decimals, and timestamps, before
  comparison or correlation binding.

### Correlated subqueries

- Within one outer execution, parsed query shapes and validated source schemas are reused. Outer values are bound
  with their Java types and NULL values intact, and each evaluation reads fresh results.
- Simple physical-table subqueries also reuse their prepared plans; derived tables, joins, and CTEs rebuild
  row-dependent materializations.
- Eligible correlated predicates are rebuilt from the current bindings for predicate pushdown on every evaluation;
  the Java residual filter still checks the current outer values.
- Caches are local to the owning execution. Uncorrelated subqueries reuse their results.
- Direct correlated scalar subqueries used as aggregate arguments have a binding limitation
  ([#307](https://github.com/Alipsa/JParq/issues/307)).

---

## Grouping and aggregation

### Expressions in grouped queries

- Aggregate results can be operands of arithmetic and scalar functions in `SELECT`, `HAVING`, and `ORDER BY`, with
  or without `GROUP BY`. For example, `SUM(cyl) * 2`, `COALESCE(MAX(cyl), 0)`, and `ROUND(AVG(mpg), 1)` are
  evaluated once per group. This includes aggregates inside `SUBSTRING(... FROM ... FOR ...)`,
  `POSITION(... IN ...)`, and `TRIM(... FROM ...)`.
- `ORDER BY` and `HAVING` may use aggregates that are not in `SELECT`.
- Scalar expressions outside aggregates may use grouping expressions and constants, evaluated with each group's
  keys, including NULL keys and keys suppressed by grouping sets. For example,
  `SELECT cyl, ABS(cyl) FROM mtcars GROUP BY cyl`.
- Grouping keys used by `HAVING` need not appear in `SELECT`:
  `SELECT cyl FROM mtcars GROUP BY cyl HAVING NULLIF(cyl, 4) IS NOT NULL`.
- Grouped `IS [NOT] DISTINCT FROM` predicates bind grouping columns, aggregates, and subqueries with their NULL
  semantics intact.
- Grouped arithmetic follows the same numeric and temporal rules as row expressions.
- Ungrouped column references in `HAVING` are rejected with SQLState `42000`, even for empty inputs.
- Unknown functions in grouped `SELECT` and `HAVING` raise `SQLSyntaxErrorException` with SQLState `42000` when
  evaluated.
- `SUM` over `REAL` or `DOUBLE` inputs reports JDBC `DOUBLE` metadata and returns `Double`.

### DISTINCT and row limits on grouped results

- Grouped and global aggregate queries apply `DISTINCT` to their projected result tuples, and apply `OFFSET`,
  `LIMIT`, and `FETCH ... ROWS ONLY` after grouping, `HAVING`, and ordering.
- Value-inferred decimal bounds and interval declarations use the full aggregate result after `DISTINCT` and before
  row limits, so metadata is consistent across pages. After inference, the result set keeps only the requested rows.

### HAVING

- `HAVING` filters groups before select-list scalar subqueries run. A subquery alias referenced by `HAVING` is
  evaluated on demand and reused for that group's output.
- `AND`/`OR` operands are evaluated left to right: `AND` stops after FALSE, and `OR` stops after TRUE. Skipped
  operands are not evaluated, so their errors are suppressed; reordering conditions can expose an error.
- Quantified comparisons use SQL three-valued logic; see [quantified comparisons](#quantified-comparisons).

### Subqueries in grouped queries

- Scalar subqueries in grouped `SELECT` lists can reference outer grouping columns, including grouping columns not
  in the output. Each subquery uses the current group's values; NULL grouping keys and keys suppressed by grouping
  sets bind as NULL. Inner names take precedence over outer names.
- References to ungrouped outer columns are rejected with SQLState `42000` in `SELECT`, `HAVING`, and `ORDER BY`,
  before reading groups and even when the outer input is empty.
- Grouped `ORDER BY` supports scalar subqueries and evaluates sort keys once per group.
- Only surviving groups contribute to inferred scalar result types. `COUNT` subqueries keep `BIGINT` metadata when
  no groups survive; other unknown scalar types depend on the returned values.

### Aggregate FILTER clause

Aggregate `FILTER (WHERE ...)` clauses are not supported yet ([#438](https://github.com/Alipsa/JParq/issues/438)).
They raise `SQLSyntaxErrorException` with SQLState `42000` during preparation, including when combined with `OVER`.
Workarounds: use a `WHERE` clause when all aggregates share one filter, or conditional arguments such as
`COUNT(CASE WHEN cyl IN (4, 6) THEN 1 END)`.

### Non-standard extensions: SELECT aliases in HAVING and GROUP BY

**HAVING** accepts unqualified `SELECT` aliases:

```sql
SELECT SUM(mpg) AS mpg FROM mtcars m GROUP BY cyl HAVING mpg > 200  -- filters on the aggregate alias
```

A qualified name such as `m.mpg` refers to the input column and must satisfy the grouping rules, so `HAVING m.mpg >
200` raises SQLState `42000`. In standard SQL, repeat the aggregate: `HAVING SUM(mpg) > 200`.

**GROUP BY** accepts unqualified `SELECT` aliases:

```sql
SELECT d.department AS dep, COUNT(*)
FROM employee_department ed JOIN departments d ON ed.department = d.id
GROUP BY dep
```

Unlike in `HAVING`, `GROUP BY` names refer to input columns in standard SQL. When a name matches both an input
column and a select-list alias, **the input column wins**; the alias applies only when no input column has that
name.

- Column matching honors quoted names, including the quoted-lowercase fallback, so the rule also holds for derived
  tables and CTEs whose internal field names differ from their SQL names.
- Aliases of aggregate expressions cannot be grouping expressions.
- A name shared by more than one select-list alias with different expressions is ambiguous (SQLState `42702`).

---

## CTEs, derived tables, LATERAL, and UNNEST

### Recursive CTEs

Recursive CTEs keep the anchor's numeric types, decimal precision, and scale; recursive schemas are not widened
across iterations. Recursive values that need fractional truncation or decimal rounding, or that exceed integer
ranges or decimal precision, fail with `SQLException` (SQLState `22003`).

For example, an integer anchor cannot accept a recursive step of `0.5`. A decimal literal infers its precision from
its value, so an anchor of `0.5` becomes `DECIMAL(1,1)` and cannot grow to `1.5`. Use an explicit cast to reserve
precision and scale:

```sql
WITH RECURSIVE r(n) AS (
  SELECT CAST(0.5 AS DECIMAL(10,1))
  UNION ALL SELECT n + 1 FROM r WHERE n < 29.5
)
SELECT n FROM r ORDER BY n
```

### Result types through CTEs and derived tables

- Known column types are kept, including for empty results.
- Computed values with unknown types, including dates and window results, are typed from the returned values through
  CTEs and non-`LATERAL` derived tables, including in joins. Inferred metadata therefore depends on which rows
  survive filtering: empty or all-NULL unknown-type columns use `VARCHAR`, and mixed types may also resolve to
  `VARCHAR`.
- For inferred columns, `getObject()` returns the matching Java type, such as `java.sql.Date`, `Long`, or
  `BigDecimal`.

### LATERAL

- `LATERAL` derived tables bind outer values with their Java types intact. For example,
  `FROM arr a CROSS JOIN LATERAL (SELECT u.t FROM UNNEST(a.dates) AS u(t)) x` expands the current row's array.
- `LATERAL` derived tables currently expose decimal expressions as `VARCHAR`, even when their bounds are known,
  to keep one stable schema across correlated executions ([#475](https://github.com/Alipsa/JParq/issues/475)).

### UNNEST

- NULL and empty arrays produce no rows; `LEFT JOIN LATERAL` keeps their outer rows with NULL inner columns.
- Standalone `UNNEST(NULL)` returns no rows and uses `VARCHAR` metadata because its element type is unknown.
- When the argument is a direct outer column reference, declared date and nested-array element types are preserved,
  including metadata when the outer input is empty.
- Declared element types are also propagated through `ARRAY` constructors, `COALESCE`, `NULLIF` (using its first
  argument's type), `CASE`, arithmetic, and sign expressions, including for empty outer inputs. Other function
  wrappers can still lose their type ([#424](https://github.com/Alipsa/JParq/issues/424)).
- Arguments can contain scalar subqueries, such as
  `UNNEST(ARRAY[(SELECT COUNT(*) FROM mtcars WHERE cyl = 4)]) AS t(x)`.
- Joined `UNNEST` expressions can reference preceding inputs, including through correlated scalar subqueries, with
  implicit or explicit `LATERAL`. Subqueries use the enclosing CTEs and current parameter bindings; unqualified outer
  names must resolve uniquely.
- Column arguments honor quoted lowercase names such as `UNNEST("n"."a")`, including references into parenthesized
  join groups. Lowercase relation aliases can be declared or referenced with or without quotes.
- Quoted mixed-case aliases require an exact match. Quoted mixed-case physical Parquet columns still fail to bind
  ([#479](https://github.com/Alipsa/JParq/issues/479)).
- References to equivalent relation qualifiers that identify conflicting inputs raise SQLState `42702` in flat and
  grouped joins. Statement-wide rejection of equivalent correlation names is tracked in
  [#482](https://github.com/Alipsa/JParq/issues/482).
- Independent joined `UNNEST` arguments are evaluated once per execution and support `RIGHT` and `FULL` joins. A
  `RIGHT` or `FULL JOIN UNNEST` argument cannot reference its preceding join inputs.
- `COUNT(*)` and ordinality-only queries over `UNNEST` do not convert interval elements to text.

| Error | SQLState |
| --- | --- |
| Scalar subquery returns several rows | `21000` |
| Invalid scalar subquery column count, or wrong alias count | `42000` |
| Unknown column | `42000` |
| Ambiguous column or conflicting equivalent qualifier | `42702` |
| `RIGHT`/`FULL JOIN UNNEST` references a preceding input | `42000` |

---

## Set operations

- Trailing `LIMIT`, `OFFSET`, and `FETCH` apply to the combined result, with or without `ORDER BY`.
  Parenthesized branches keep their own row bounds.
- Values are converted to the common type before set comparison. Metadata reports the common type and decimal
  bounds, including for empty results.

### Numeric branch types

| Branches | Result |
| --- | --- |
| `INTEGER` and `BIGINT` | `BIGINT` |
| `REAL` and `REAL` | `REAL` (`Float` values) |
| Other approximate combinations | `DOUBLE` |
| `DECIMAL` and exact numeric | `DECIMAL` with the largest integer digit count and scale |
| Numeric and non-numeric | Incompatible |

Integer literals currently use `BIGINT`.

### NULL and declared types

- At the top level, an untyped NULL branch adopts the other branch's type.
- Untyped NULL projections report `OTHER` metadata in plain `SELECT`s, grouped results, set operations, CTEs, and
  derived tables. Their internal schemas stay untyped through CTEs and derived tables (including empty results), so
  an enclosing set operation can adopt a numeric or non-numeric branch's type.
- String literals (including national `N'...'`), `DATE`/`TIME`/`TIMESTAMP` literals, character casts, `DATE` casts,
  and `TIME`/`TIMESTAMP WITHOUT TIME ZONE` casts report their declared JDBC types even for empty or all-NULL
  results.
- Declared LOB and binary casts stay typed for set compatibility, even for all-NULL or empty results; incompatible
  numeric, character, binary, or datetime families raise SQLState `42000`. These declared `CAST` NULLs are distinct
  from untyped NULL projections.
- Compatible `CHAR` and `VARCHAR` branches produce `VARCHAR`.
- Compatible local and offset times or timestamps are promoted to the corresponding `WITH TIME ZONE` type using the
  session offset. This also applies to `VALUES`.

Set-operation branch declaration checks and cross-branch type compatibility run during execution, including for
empty inputs ([#464](https://github.com/Alipsa/JParq/issues/464)).

---

## Data types

### Numeric arithmetic

JDBC types for arithmetic are derived before rows are read, including through derived tables and CTEs.

| Operands | Result |
| --- | --- |
| Whole numbers with `+`, `-`, `*`, `%`, unary sign | `BIGINT` (`Long`); overflow raises `22003` |
| `REAL` or `DOUBLE` | `DOUBLE` (`Double`), using double arithmetic |
| Exact decimals | `DECIMAL` (`BigDecimal`); declared bounds reserve carry and product digits |
| Non-approximate division | `MathContext.DECIMAL64`, so `7 / 2` returns `3.5`. Precision and scale are unknown until materialization infers them from the values, keeping value scales such as `3.5`. |
| NULL operand | NULL |
| Division or remainder by zero | NULL |

- Integer literals that fit `INTEGER` return `Integer`; larger literals use `Long` or exact decimals. Decimal
  literals such as `1.5` are exact.
- Unary signs promote whole-number literals to `BIGINT`, so `-1` and `+1` return `Long`.
- Whole-number grouping and `DISTINCT` keys compare equal across these Java representations.
- JDBC precision is 10 for `INTEGER` and 19 for `BIGINT`; literal-specific digit bounds are kept internally for
  decimal arithmetic.
- Unary signs and binary arithmetic accept numeric text and trim surrounding whitespace, as numeric `CAST` does.
  Invalid or non-finite text raises `SQLDataException` with SQLState `22018`. Text outside the `DOUBLE` range raises
  `22003` when converted for floating-point arithmetic. Unsupported operand types for unary signs raise `42000`.

### Numeric casts

- `DECIMAL(p,s)`, `NUMERIC(p,s)`, and `NUMBER(p,s)` round to the declared scale with `HALF_UP`, then reject
  precision overflow with SQLState `22003`.
  - Omitting the scale while specifying precision means scale zero.
  - Bounds must satisfy `1 <= p` and `0 <= s <= p`. Invalid declarations fail during preparation with SQLState
    `42000`, including for NULL inputs and empty results.
  - Casts without precision keep the decimal value and can report unknown precision until materialization infers
    it.
- Integer casts truncate fractions toward zero before checking the target range.
- Integer range overflow and floating-point overflow to infinity raise `SQLDataException` with SQLState `22003`.
  Floating-point rounding and underflow to zero follow Java conversion behavior.
- Non-finite text such as `'Infinity'`, `'-Infinity'`, and `'NaN'`, and invalid numeric text, raise
  `SQLDataException` with SQLState `22018`.
- `FLOAT`, including `FLOAT(p)` for any precision such as `FLOAT(53)`, is always single precision (Java `Float`,
  JDBC `REAL`). Use `DOUBLE`, `DOUBLE PRECISION`, or `FLOAT8` for double precision; for example, cast `1e300` to
  `DOUBLE`, not `FLOAT(53)`.

### Floating-point values

Floating-point values use one ordering everywhere, with and without predicate pushdown, matching PostgreSQL and
DuckDB:

`-Infinity` < every finite value < `+Infinity` < `NaN`, where `NaN = NaN` and `-0.0 = 0.0`.

Comparisons with `NaN` and infinities are therefore TRUE or FALSE rather than UNKNOWN, and `ORDER BY`, `MIN`, and
`MAX` place `NaN` last. Arithmetic follows IEEE 754: for example, `Infinity * 0` and `NaN + 1` return `NaN`.
`DISTINCT` and `GROUP BY` currently keep `-0.0` and `0.0` apart
([#477](https://github.com/Alipsa/JParq/issues/477)).

### CASE

Numeric result branches built from literals, columns, numeric casts, arithmetic, aggregates, `COALESCE`, `ABS`, and
`ROUND` use a common type derived before rows are read. Untyped NULL branches do not constrain it.

- Selected values use that type consistently for JDBC metadata and `getObject()`, including grouped aggregate
  branches.
- Decimal branches share a scale when their bounds are known; unbounded expressions keep value-specific scales.
- Decimal equality keys ignore scale for `DISTINCT`, `GROUP BY`, and window `PARTITION BY`.
- Unselected branches are not evaluated.
- Scalar subqueries and other functions without numeric type derivation leave `CASE` untyped, so mixed numeric
  values can still split keys ([#319](https://github.com/Alipsa/JParq/issues/319)).

### Character and binary types

**Character casts**

- Casting numeric, datetime, or interval values to a character type raises `SQLDataException` with SQLState `22001`
  when the text does not fit the requested length.
- `CHAR`, `CHARACTER`, and `NCHAR` without a length mean length 1.
- Character-source casts keep their truncation behavior.
- The standard `NATIONAL CHARACTER`, `NATIONAL CHAR`, and `NATIONAL CHARACTER VARYING` spellings behave like
  `NCHAR`/`NVARCHAR`.

**Binary values**

- Hex literals (`X'41'`, `0x41`) and JDBC `setBytes`/`setObject(ByteBuffer)` parameters return `byte[]`. Hex
  digits must come in pairs; `X''` is an empty byte array. `ByteBuffer` parameters use the remaining bytes without
  changing the buffer's position or limit.
- Binary casts and cast-style `CONVERT` return `byte[]` and keep `BINARY`, `VARBINARY`, or `BLOB` metadata through
  CTEs and derived tables, including all-NULL and empty results.
- Character/binary casts use UTF-8. Malformed UTF-8 raises SQLState `22021` instead of replacing bytes.
- `BLOB` lengths accept `K`/`M`/`G`/`T`/`P` multipliers using overflow-checked long bounds. Plain LOB lengths above
  `Integer.MAX_VALUE` hit a parser restriction ([#462](https://github.com/Alipsa/JParq/issues/462)).
- `||` concatenation preserves bytes. `OCTET_LENGTH` counts binary bytes.
- Binary arguments to `CHAR_LENGTH`, `LENGTH`, `UPPER`, `LOWER`, and `CONCAT`, binary/non-binary comparisons,
  invalid lengths, and known incompatible binary cast sources (including `INTERVAL`) raise `42000` during
  preparation in plain `SELECT`s. Preparation checks include `IN`, `BETWEEN`, quantified comparisons, mixed
  binary/character `LIKE`, simple `CASE`, and `NULLIF`, even for NULL and empty inputs. Operands of unknown type are
  validated at run time.
- Standard binary `SUBSTRING`, `POSITION`, `TRIM`, `OVERLAY`, and binary-pattern `LIKE` are not supported yet
  ([#461](https://github.com/Alipsa/JParq/issues/461)).
- Both `getBlob` overloads return detached BLOBs, or null for SQL NULL.

**Large objects**

- `CHARACTER LARGE OBJECT`, `NCLOB`, `NATIONAL CHARACTER LARGE OBJECT`, and `BINARY LARGE OBJECT` share the
  corresponding LOB compatibility checks.
- `CLOB`/`NCLOB` use interim character storage. Full character LOB metadata and LOB array elements are tracked in
  [#452](https://github.com/Alipsa/JParq/issues/452).

**Implicit conversion**

Implicit interval-to-text coercion is a type error (`SQLSyntaxErrorException`, SQLState `42000`), including in
comparisons with `VARCHAR` values, concatenation, `LIKE`, and string functions. Use an explicit cast when text is
intended, such as `CAST(end_ts - start_ts AS VARCHAR)`. NULL operands still produce NULL.

### Date and time

**Datetime value functions** (`CURRENT_TIMESTAMP`, `CURRENT_TIME`, `CURRENT_DATE`, `LOCALTIMESTAMP`, `LOCALTIME`)

- All share one snapshot per statement execution, including nested queries and lazy result reads. Each execution of
  a prepared statement takes a fresh snapshot.
- They keep their Java time types, fractional seconds, UTC offsets, and JDBC metadata through derived tables and
  CTEs, including empty results. `MIN`/`MAX` over these derived columns keep the temporal metadata.

**Parsing errors**

- Invalid `DATE`, `TIME`, and `TIMESTAMP` formats raise `SQLDataException` with SQLState `22007`.
- Out-of-range calendar and clock fields raise `22008`.
- These classifications and their causes are preserved through getters, `WHERE`/`HAVING` filters, and nested query
  evaluation. Errors can be deferred until iteration or value access.

**TIME values**

- `TIME` literals, parameters, and stored Avro `time-millis`/`time-micros` columns keep milliseconds in
  `java.sql.Time`. Digits below one millisecond are truncated, including in comparisons, sorting, grouping,
  `DISTINCT`, aggregates, and set operations; values that differ only below a millisecond compare equal.
- `TIME` conversions, including `CAST(timestamp AS TIME)` and JDBC `getTime()`, use the local date 1970-01-01, so
  equality, hashing, and ordering depend on clock time alone.
- Interval addition and subtraction keep milliseconds and wrap at midnight.
- Stored `TIME` columns report JDBC `TIME` metadata and return `java.sql.Time` from `getObject()` and `getTime()`.
  `CAST(t AS VARCHAR)` returns `10:00:00` for a stored 10:00 clock value. `java.sql.Time.toString()` shows whole
  seconds even when the value has milliseconds.
- Stored `TIME` values outside the half-open range 00:00:00 to 24:00:00 raise `SQLDataException` with SQLState
  `22008`.
- `CAST` defaults to six fractional seconds for `TIMESTAMP` and zero for `TIME`; an explicit precision overrides the
  default. Casts truncate excess fractional digits before arithmetic.

**Parameters**

JDBC date, time, and timestamp parameters, including their `java.time` equivalents, keep their JDBC types through
typed SQL literals. Timestamps keep nanoseconds. `LocalDateTime` timestamp parameters before year 1 are rejected
with SQLState `22008` because their era cannot round-trip through JDBC.

**Date arithmetic**

- `DATE - DATE` returns the signed number of days as `Long` (JDBC `BIGINT`), including through derived tables and
  CTEs: `DATE '2024-03-01' - DATE '2024-02-28'` returns `2`.
- Day counts cannot be compared with or added to `INTERVAL` values (SQLState `42000`). Compare against numbers
  instead: `d - DATE '2024-02-01' > 7`.
- Date and time offsets require an explicit `INTERVAL`: `DATE '2024-01-01' + INTERVAL '1 day'` works, while
  `DATE '2024-01-01' + 1` and other unsupported temporal arithmetic raise `SQLException` with SQLState `42000` when
  evaluated.
- Subtracting `TIME` from `DATE` or `TIMESTAMP`, or the reverse, raises `SQLSyntaxErrorException` with SQLState
  `42000`, including within predicates.
- Differences between `TIMESTAMP` values, between `TIME` values, or between mixed `DATE`/`TIMESTAMP` operands return
  intervals; see below.
- `VALUES` rejects datetime inputs with incompatible fields.

### Intervals

**Literals**

- A separate unit: `INTERVAL '1' DAY`, for `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `SECOND`. `SECOND` accepts
  fractional values.
- A single string: `INTERVAL '1 day'`.
- `DAY TO SECOND`: `INTERVAL '1 02:03:04' DAY TO SECOND`. Fractional seconds are kept, and a leading sign applies to
  the whole interval. The day field must contain digits after the optional sign.
- Values may have an embedded sign or a unary sign (`-INTERVAL '1' DAY`).
- Qualifier precision such as `DAY(3)` and `SECOND(2,3)` is accepted. Fractional-second precision is kept, but
  leading-field precision limits are not enforced yet ([#474](https://github.com/Alipsa/JParq/issues/474)).

| Problem | SQLState |
| --- | --- |
| Invalid interval text, including unknown unit words in the single-string form (such as `lightyear`), and decimal, exponent, or repeated-sign day fields | `22007` (`SQLDataException`) |
| Numeric values that cannot be represented, including digit-only day values outside the supported range | `22003` |
| Unsupported qualifiers (`YEAR TO MONTH`, `WEEK`, `QUARTER`, `MILLISECOND`, `MICROSECOND`, including plurals, in both literal forms) and expression operands such as `INTERVAL (1+1) DAY` | `0A000` (`SQLFeatureNotSupportedException`); see [#352](https://github.com/Alipsa/JParq/issues/352) |

**Values and JDBC**

- Intervals are returned as immutable `se.alipsa.jparq.helper.TemporalInterval` objects from `getObject()` and
  `getObject(column, TemporalInterval.class)`. Plain interval literals use the same representation.
- Interval columns report JDBC `Types.OTHER`, type name `INTERVAL`, and class `TemporalInterval` in metadata.
- `getString()` and explicit character casts use the result column's common interval declaration. The `qualifier()`
  and `fractionalPrecision()` accessors describe that declaration.
- `CASE`, `COALESCE`, `VALUES`, arrays, and set operations combine the widest field range and the largest
  fractional-second precision of their inputs, even inputs that produce no rows. For example, combining `1 DAY`
  and `36 HOUR` prints `1 00` and `1 12`, and all rows use the same fractional digits. A standalone
  `INTERVAL '90' MINUTE` still prints `90`. Positive values omit the optional plus sign. Nanoseconds are kept
  through materialization.
- Parentheses, `CASE`, `COALESCE`, `NULLIF`, aggregates, navigation window functions, and scalar subqueries keep
  interval metadata even for empty inputs or all-NULL results. Datetime operands in those expressions also keep
  their declared type and precision when used in subtraction.
- Intervals keep their types through derived tables, CTEs, `VALUES`, `UNNEST`, `LATERAL` inputs, set operations,
  and scalar or predicate subqueries. Comparisons with intervals and `EXTRACT` work after materialization.
- Implicit conversion of intervals to text is an error; see [implicit conversion](#character-and-binary-types).

### Arrays

**Constructing arrays**

- Non-NULL scalar and array elements must share a common type and nesting depth. NULL elements are kept.
- Numeric elements, including leaves across sibling nested arrays, are promoted to a common numeric type. Mixing
  numbers with nested arrays, strings, or other incompatible types raises `SQLSyntaxErrorException` with SQLState
  `42000` when evaluated.
- Stored array columns and `ARRAY` constructors can be combined; nested reference arrays are normalized to lists,
  including in `ARRAY` subqueries.
- Computed arrays keep declared element types even when every element is NULL, including fixed-length character and
  varying binary declarations. Incompatible declared element types are rejected during planning with SQLState
  `42000`, including NULL elements in predicates, grouping, ordering, join conditions, and skipped subqueries.

**Temporal elements**

- Elements must have the same datetime fields (`DATE`, `TIME`, or `TIMESTAMP`), including across sibling nested
  arrays. `DATE`/`TIMESTAMP`, `DATE`/`TIME`, and `TIMESTAMP`/`TIME` combinations raise SQLState `42000`. Cast `DATE`
  elements to combine them with timestamps:
  `ARRAY[CAST(DATE '2020-01-01' AS TIMESTAMP), TIMESTAMP '2020-01-01 12:00:00']`.
- Compatible local and zoned timestamps or times are promoted to the zoned type. Promoted local values receive the
  SQL session time zone displacement (the JVM time zone's current offset), regardless of their own date, without
  adjusting local timestamp fields in daylight-saving gaps.
- Values are normalized to `java.sql.Date`, `java.sql.Time` (millisecond precision), `java.sql.Timestamp`,
  `OffsetTime`, or `OffsetDateTime`, keeping timestamp fractions and explicit offsets.

**Types through queries**

- Physical array and struct columns keep their types through derived tables, CTEs, and set operations, with matching
  source schemas available even for empty results.
- Computed arrays with unknown element declarations, and arrays with differing set-branch schemas in non-`LATERAL`
  derived tables and CTEs, infer element types from the returned values, recursively. Empty or all-NULL elements use
  a nullable `VARCHAR` element schema.

**Comparison and grouping**

- Predicates compare arrays by content, element by element, treating stored and computed arrays alike. A NULL
  element sorts before non-NULL elements, and when one array is a prefix of the other, the shorter array sorts first.
- `DISTINCT`, `GROUP BY`, set-operation keys, and window `PARTITION BY` compare struct fields and array elements by
  content, independently of Avro record names.

**JDBC access**

- `getObject()` returns every array value, stored or computed, as `Object[]`, including mixed set-operation results.
- `getArray(index)` and `getArray(label)` return detached JDBC `Array` objects; SQL NULL returns null and sets
  `wasNull()`. JDBC arrays expose element type metadata, `Object[]` contents, one-based slices, and INDEX/VALUE
  element result sets. `free()` releases the array; later reads raise `SQLException`.
- `CLOB` and `BLOB` array elements raise `SQLFeatureNotSupportedException` with SQLState `0A000`
  ([#452](https://github.com/Alipsa/JParq/issues/452)). Custom user-defined element mappings raise
  `SQLFeatureNotSupportedException` when requested.

### Maps

- Map keys and values are checked recursively for a common type, including numeric and temporal promotion across
  sibling maps. NULL values and empty maps do not constrain inferred types. Incompatible map contents raise SQLState
  `42000`.
- Top-level map columns and maps inside stored arrays keep raw Avro logical values
  ([#313](https://github.com/Alipsa/JParq/issues/313)).

### Structs

- `getObject()` returns struct columns as Avro `GenericRecord` values. Fields keep their raw Avro representations:
  strings can be `Utf8`, dates epoch-day integers, decimals `ByteBuffer`, and timestamps stored longs. The record's
  schema describes the field types; nested struct fields are not decoded recursively.
- Struct elements in arrays must have matching field names and order, with compatible SQL field types resolved
  recursively. Numeric fields are promoted to a common type. Nullability, Avro type names, and storage properties do
  not distinguish SQL types. Incompatible struct schemas raise SQLState `42000`.
- Temporal fields keep the finer precision when units differ. Mixed nanosecond and coarser timestamp fields use
  lossless internal datetime text to preserve the full date range and nanosecond fractions:
  - UTC-only combinations keep their instants and decode as `Timestamp`, preserving the standalone JVM-zone view,
    even during daylight-saving overlaps.
  - Combinations that include local timestamps keep wall-clock fields and decode as `LocalDateTime`.
  - Both keep JDBC type `TIMESTAMP`. Timestamp promotion overflow in other physical encodings raises SQLState
    `22008`.

---

## Literals and parameters

**String literals**

- Escape an apostrophe by doubling it: `'O''Brien'` evaluates to `O'Brien`. JDBC `setString` values are escaped
  automatically.
- Untagged dollar-quoted strings (`$$...$$`) are supported and keep their contents literally, including apostrophes,
  identifier quotes, and comment markers: `$$it's$$` returns `it's`, and `$$a--b$$` returns `a--b`.
- Tagged forms such as `$tag$...$tag$` are not supported ([#476](https://github.com/Alipsa/JParq/issues/476)).
- JDBC parameter scanning does not yet recognize dollar quotes: parameter markers inside their contents, and real
  parameters after them, can be misidentified ([#296](https://github.com/Alipsa/JParq/issues/296)).

**Parameters**

- Positional `?` and named `:paramName` parameters; see the [JDBC support](jdbc.md#prepared-statements).
- For binary and datetime parameters, see [binary values](#character-and-binary-types) and
  [date and time](#date-and-time).

---

## JDBC result values

**Numeric getters** (`getByte`, `getShort`, `getInt`, `getLong`, `getFloat`, `getDouble`)

- Convert numeric text by column index and label.
- Return zero for SQL NULL, with `wasNull()` returning true.
- For text, follow numeric `CAST` rules: truncate integer fractions toward zero, reject invalid or non-finite text
  with SQLState `22018`, and reject range overflow with `22003`.
- Native numeric values keep Java numeric conversion behavior, including NaN and infinity for floating-point
  getters.
- `BOOLEAN` values convert to 1 for TRUE and 0 for FALSE.

**Typed `getObject(column, Class)`**

- Supports numeric conversions, `BigDecimal`, `LocalDate`, `LocalTime`, `LocalDateTime`, `OffsetTime`, and
  `OffsetDateTime`.
- `TIME` conversions keep milliseconds; `TIMESTAMP` conversions keep nanoseconds.
- Datetime targets accept datetime and character sources. Numeric and Boolean sources raise `22005`.
- `OffsetTime` applies the JVM's current time zone offset before normalizing to UTC. `OffsetDateTime` converts the
  timestamp's instant to UTC. Existing offset values keep their offsets.
- SQL NULL returns null and sets `wasNull()`.

**Other values:** see [arrays](#arrays), [structs](#structs), [intervals](#intervals), and
[binary values](#character-and-binary-types).

---

## Statement cancellation

`Statement.cancel()` works while `executeQuery()` is aggregating, sorting, loading joins, or running nested queries,
and after a result has been returned.

- It requests closure without waiting for an active physical read. A pushed-down record filter can make the current
  read span the rest of a shard; cancellation does not interrupt a decoder in use.
- The running `executeQuery()` or in-flight `next()` throws `SQLException` with SQLState `57014` when it observes the
  cancellation, instead of returning a partial aggregate or reporting the end of results. This includes cancellation
  inside a correlated subquery evaluated during iteration.
- Nested queries share the outer cancellation state and preserve the outer result returned by
  `Statement.getResultSet()`, so cancellation between subquery calls also stops iteration.

---

## Errors and SQLStates

- SQL that JSqlParser rejects raises `SQLSyntaxErrorException` with SQLState `42000`.
- Syntax and numeric errors keep their preparation and nested query context in messages and cause chains.
- Projection evaluation failures are reported as `SQLException` with their original causes, including through result
  set getters.
- Non-Boolean predicates, such as `CASE WHEN cyl` or `NOT model`, raise `SQLSyntaxErrorException` with `42000`.
- Expression diagnostics keep the SQL structure but replace internal correlated bindings with `<outer value>`,
  including inside nested subqueries.
- Many errors are raised only when a value is evaluated, which can be during `next()` or a getter call.

| SQLState | Meaning in JParq |
| --- | --- |
| `0A000` | Feature not supported (some interval qualifiers, `LIMIT offset, count`, LOB array elements) |
| `21000` | Scalar subquery returned more than one row |
| `22001` | Cast result does not fit the character length |
| `22003` | Numeric value out of range or overflow |
| `22005` | Incompatible source type for typed `getObject` |
| `22007` | Invalid datetime or interval format |
| `22008` | Datetime field out of range |
| `22018` | Invalid character value for a numeric conversion |
| `22021` | Malformed UTF-8 in a character/binary conversion |
| `2201W` | Invalid `LIMIT`/`FETCH` row count |
| `2201X` | Invalid `OFFSET` row count |
| `42000` | Syntax error or invalid query (unknown names, type errors, unsupported constructs) |
| `42702` | Ambiguous column reference |
| `57014` | Statement cancelled |

---

## Known deviations and open issues

These are known gaps relative to the SQL standard or expected behavior. Each is a bug to be fixed.

| Area | Issue |
| --- | --- |
| Aggregate `FILTER (WHERE ...)` is not supported | [#438](https://github.com/Alipsa/JParq/issues/438) |
| Unquoted references do not resolve quoted lowercase materialized aliases | [#338](https://github.com/Alipsa/JParq/issues/338) |
| Correlated scalar subqueries used directly as aggregate arguments lose outer bindings | [#307](https://github.com/Alipsa/JParq/issues/307) |
| `CASE` with scalar subqueries or untyped functions does not derive a common numeric type | [#319](https://github.com/Alipsa/JParq/issues/319) |
| Window functions outside `SELECT` lists are not validated | [#350](https://github.com/Alipsa/JParq/issues/350) |
| Scalar function result types (and `UNNEST` type propagation through them) | [#424](https://github.com/Alipsa/JParq/issues/424) |
| Character metadata for concatenation and `CASE` | [#460](https://github.com/Alipsa/JParq/issues/460) |
| `TIME` predicates are not pushed down | [#359](https://github.com/Alipsa/JParq/issues/359) |
| Row value `IN`/`ANY`/`ALL` comparisons | [#388](https://github.com/Alipsa/JParq/issues/388) |
| Some type checks run at execution instead of preparation | [#464](https://github.com/Alipsa/JParq/issues/464) |
| Source-first `CONVERT` rejects NULL inputs and parameterized decimal targets | [#465](https://github.com/Alipsa/JParq/issues/465) |
| Standard binary `SUBSTRING`, `POSITION`, `TRIM`, `OVERLAY`, and binary `LIKE` | [#461](https://github.com/Alipsa/JParq/issues/461) |
| `CLOB`/`BLOB` support, including array elements | [#452](https://github.com/Alipsa/JParq/issues/452) |
| Plain LOB lengths above `Integer.MAX_VALUE` | [#462](https://github.com/Alipsa/JParq/issues/462) |
| Map values outside struct fields keep raw Avro logical values | [#313](https://github.com/Alipsa/JParq/issues/313) |
| JSON path `[*]`, `[last]`, and ranges | [#303](https://github.com/Alipsa/JParq/issues/303) |
| JDBC parameter scanning inside dollar-quoted strings | [#296](https://github.com/Alipsa/JParq/issues/296) |
| Ordered-set aggregates with `WITHIN GROUP` (`PERCENTILE_CONT`, `LISTAGG`, ...) | [#473](https://github.com/Alipsa/JParq/issues/473) |
| Multi-field interval qualifiers (`YEAR TO MONTH`, ...) and `INTERVAL (expr) unit` | [#352](https://github.com/Alipsa/JParq/issues/352) |
| Interval leading-field precision is not enforced | [#474](https://github.com/Alipsa/JParq/issues/474) |
| `LATERAL` derived tables expose decimal expressions as `VARCHAR` | [#475](https://github.com/Alipsa/JParq/issues/475) |
| Tagged dollar-quoted strings fail with an unclassified error | [#476](https://github.com/Alipsa/JParq/issues/476) |
| `GROUP BY` and `DISTINCT` treat `-0.0` and `0.0` as different values | [#477](https://github.com/Alipsa/JParq/issues/477) |
| `WHERE` predicates are not pushed into parenthesized join groups (performance only) | [#478](https://github.com/Alipsa/JParq/issues/478) |
| Quoted mixed-case physical Parquet columns fail to bind | [#479](https://github.com/Alipsa/JParq/issues/479) |
| Equivalent correlation names are not rejected throughout the FROM clause | [#482](https://github.com/Alipsa/JParq/issues/482) |
