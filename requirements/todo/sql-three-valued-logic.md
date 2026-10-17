# SQL Three-Valued Logic in Expression Evaluation

Prerequisite for Phase 1b items 3 and 6 of [optimized-parquet.md](optimized-parquet.md).

## Background

SQL predicates evaluate to TRUE, FALSE or UNKNOWN. A comparison with a NULL operand is UNKNOWN, `NOT UNKNOWN` is
UNKNOWN, and WHERE/ON/HAVING keep only rows where the condition is TRUE.

JParq's `ExpressionEvaluator.eval(Expression, GenericRecord)` returns a plain `boolean`, so UNKNOWN collapses to
`false` and `NOT` then flips it to `true`:

```java
private boolean evalNot(NotExpression not, GenericRecord rec) {
  return !eval(not.getExpression(), rec);
}
case "<>", "!=" -> cmp != 0;                    // evalBinary
if (left == null || pat == null) { return false; } // evalLike
```

Results reported in plan review for a table where `n = id` except NULL on row 2 (`id = 1..4`), Java-side
evaluation (the `n + 0` forms and the NULL literal prevent pushdown). Re-run them as the first step of the
implementation:

| Query | Today | Correct |
|---|---|---|
| `WHERE n + 0 <> 3` | `[1 2 4]` | `[1 4]` |
| `WHERE NOT (n + 0 = 3)` | `[1 2 4]` | `[1 4]` |
| `WHERE NOT (n + 0 = 1 OR id = 4)` | `[2 3]` | `[3]` |
| `WHERE NOT (n + 0 IN (1, 3))` | `[2 4]` | `[4]` |
| `WHERE n NOT IN (3, NULL)` | `[1 2 4]` | `[]` |
| `WHERE NOT (CAST(n AS VARCHAR) LIKE '3%')` | `[1 2 4]` | `[1 4]` |
| `WHERE CAST(n AS VARCHAR) NOT LIKE '3%'` | `[1 4]` (derived from code, not run) | `[1 4]` |

Consequences:
- the bug is not specific to Parquet pushdown; it affects every WHERE evaluated in Java;
- logically equivalent rewrites (e.g. `NOT (x LIKE p)` vs `x NOT LIKE p`) give different results, so the negation
  normal form rewrite planned in optimized-parquet Phase 1b item 3 is not safe until this is fixed;
- the `predicatePushdown=false` oracle returns the same wrong rows as the pushdown bug, so it cannot validate NULL
  handling.

## Where the evaluator is used

Mapped call sites of the boolean `ExpressionEvaluator.eval` (verify and complete while implementing):

- WHERE: `QueryProcessor` (residual filter), `JParqResultSet` (residual filter), `AggregateFunctions` (WHERE before
  grouping)
- JOIN ON: `JoinRecordReader`
- CASE WHEN conditions and boolean expressions: `ValueExpressionEvaluator` via its `conditionEvaluator`
- Subquery predicates handled inside `ExpressionEvaluator` (`IN (subquery)`, `EXISTS`, `ANY`/`SOME`/`ALL`)
- HAVING uses a separate `HavingEvaluator` in `AggregateFunctions`; check it for the same two-valued behavior

## Requirements

1. Introduce a three-valued result (e.g. an enum `SqlBoolean { TRUE, FALSE, UNKNOWN }` or `Boolean` with `null` for
   UNKNOWN) for predicate evaluation in `ExpressionEvaluator`.
2. Rules:
   - `NOT UNKNOWN` = UNKNOWN;
   - `AND`: FALSE if either side is FALSE, otherwise UNKNOWN if either side is UNKNOWN, otherwise TRUE;
   - `OR`: TRUE if either side is TRUE, otherwise UNKNOWN if either side is UNKNOWN, otherwise FALSE;
   - any comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), `LIKE`/`ILIKE`/`SIMILAR TO` (including the `NOT` forms) and
     `BETWEEN` with a NULL operand gives UNKNOWN;
   - `x IN (list)`: TRUE if any element equals `x`; otherwise UNKNOWN if `x` is NULL or the list contains NULL;
     otherwise FALSE. `NOT IN` is the negation of that;
   - `IN (subquery)`, `ANY`/`SOME`/`ALL` follow the same rules over the subquery rows;
   - `IS NULL`, `IS NOT NULL`, `EXISTS` and `IS [NOT] DISTINCT FROM` never return UNKNOWN.
3. The WHERE, ON and HAVING boundaries treat UNKNOWN as FALSE (row excluded).
4. CASE WHEN treats UNKNOWN as not matched. A boolean expression used as a value (e.g. in a projection) yields SQL
   NULL for UNKNOWN.
5. Keep a `boolean` convenience method for the WHERE/ON boundary so existing callers change minimally, and keep the
   public API documented (Javadoc for all params, return and throws).

## Tests

- Every row of the table above, plus `AND`/`OR`/`NOT` truth tables with NULL operands, for WHERE, JOIN ON, HAVING
  and CASE WHEN.
- `NOT (x LIKE p)` and `x NOT LIKE p` return the same rows; likewise for `NOT (a = b)` vs `a <> b` and
  `NOT (a IN ...)` vs `a NOT IN ...`.
- Existing tests that relied on the two-valued behavior are updated, and each such change is listed.

## Documentation

- `release.md`: mark as a **behavior change**: predicates involving NULL now follow SQL three-valued logic, so
  queries using `<>`, `NOT`, `NOT IN` or `NOT LIKE` on nullable columns may return fewer rows.
