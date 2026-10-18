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
Boolean tail outside the parenthesized subquery or value list. `SqlExpressionNormalizer` now restores IN's
precedence throughout the parsed statement before CTEs, derived tables, or set-operation branches are
extracted. It writes replacement roots back to WHERE, HAVING, JOIN ON, and SELECT-list owners, including
CASE/function arguments, AND/OR chains, negation, and parentheses. Boolean expressions actually inside an IN
value list retain their existing grouping. JOIN ON evaluation receives a subquery executor. Unqualified
inner columns are not replaced merely because the outer join or grouping context contains the same name.
Unqualified names absent from every inner scope resolve to available outer row or grouping columns.
`SubqueryExecutor` resolves and retains each inner scope's source columns lazily within the statement, using
physical schemas retained by the prepared plan, CTE columns, derived projections, and fixed
information_schema schemas. Resolving virtual column names does not materialize or scan the catalog.
Unknown inputs are distinct from known inputs with no columns. Unknown table/function inputs, unresolved
schemas, and uncertainty within joins or derived wildcards keep unqualified references inner, while
explicit outer qualifiers still resolve. Both known and unknown results are retained within the statement. The shared rewrite
path applies this inner-first rule to IN, EXISTS, scalar subqueries, and ANY/ALL. Projection planning uses
the same resolver to retain unqualified outer dependencies before reading data.

The predicate evaluator also rewrites correlated IN subqueries with the current outer row's correlation
context, using the same helper as EXISTS, rather than executing them as uncorrelated cached queries.
Whether a predicate subquery is uncorrelated is retained per AST node so subsequent rows reuse its cached
result without repeating correlation analysis. Nested statements preserve the outer JDBC result and inherit
its cancellation state even when started during iteration.

## Verification

`src/test/java/jparq/CorrelatedInOrTest.java` checks the exact 94-row result, independent predicate controls,
reversed OR operands, parentheses, AND/OR precedence, negation, NOT IN, and uncorrelated IN. A second inner
table uses `g = (id + 1) % 5`, making the correlated IN result empty while its disjunction still returns the
89 range matches. Coverage includes derived tables, CTEs, UNION ALL multiplicities, JOIN WHERE/ON, HAVING,
SUM/CASE, literal IN lists, and an IN list containing a Boolean expression. Every SQL case runs with all four
predicate/projection pushdown combinations. `SubqueryCorrelationCachingTest` asserts that 1,000 outer rows
require one uncorrelated-IN analysis. `StatementCancellationTest` checks outer result identity and SQLState
57014 when EXISTS and correlated IN/OR are canceled between `next()` calls.
`UnqualifiedSubqueryCorrelationTest` checks the HAVING regression, the formerly unsupported WHERE forms,
inner-column shadowing, derived/CTE inputs, scalar and quantified subqueries under all four pushdown
combinations, plus both information_schema views with colliding outer names, prepared execution, and
qualified/correlated controls. `SubqueryColumnResolutionTest` verifies scan-free virtual binding,
conservative handling and caching of unknown inputs, and outer fallback for known inputs without the name.
The analysis-count test also checks that source columns are resolved once for 1,000 rows.
