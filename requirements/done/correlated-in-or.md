# Correlated IN combined with OR

Fixed during review of PR #189. The defect was reported as already present on main.
A Parquet-backed regression reproduced the failure before the fix with predicate pushdown and projection
pushdown independently enabled and disabled.

## Reproducer

Both tables have INTEGER columns `id` and `g`:

- `big`: 100 rows, `id` from 0 through 99 and `g = id % 5`.
- `small`: 5 rows, `id` from 0 through 4 and `g = id % 5`.

```sql
SELECT b.id FROM big b
WHERE b.id IN (SELECT s2.id FROM small s2 WHERE s2.g = b.g)
   OR b.id > 10;
```

Expected: 94 identifiers, 0–4 and 11–99. Before the fix, the query returned zero rows. The IN predicate alone
returned 5 rows, and the range predicate alone returned 89 rows.

## Cause and implementation

JSqlParser 5.3 can parse this as an `InExpression` whose right operand is an `OrExpression`, absorbing the
Boolean tail outside the parenthesized subquery. The driver now restores IN's precedence before predicate
execution, including AND/OR chains, negation, and parentheses. Boolean expressions inside an IN value list
retain their existing grouping.

The predicate evaluator also rewrites correlated IN subqueries with the current outer row's correlation
context, using the same helper as EXISTS, rather than executing them as uncorrelated cached queries.

## Verification

`src/test/java/jparq/CorrelatedInOrTest.java` checks the exact 94-row result, independent predicate controls,
reversed OR operands, parentheses, AND/OR precedence, negation, NOT IN, and uncorrelated IN. A second inner
table uses `g = (id + 1) % 5`, making the correlated IN result empty while its disjunction still returns the
89 range matches. Every case runs with all four predicate/projection pushdown combinations.
