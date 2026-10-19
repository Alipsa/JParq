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

A separate pre-existing CTE scoping defect was fixed during review: the correlation rewriter retained WITH,
but parsing a parenthesized SELECT discarded declarations attached to its inner SELECT. The parser now
enters each SELECT's WITH scope before unwrapping or parsing its body. Local CTEs override physical tables
and inherited CTE names; duplicate declarations within a single WITH scope remain errors. Derived FROM
inputs use the same scope-aware parser and are identified by their own SQL, since a CTE reference also
stores a parsed subquery plan. Standalone and join execution execute a derived input before attempting
inherited-CTE lookup by name.

## Verification

`src/test/java/jparq/CorrelatedInOrTest.java` checks the exact 94-row result, independent predicate controls,
reversed OR operands, parentheses, AND/OR precedence, negation, NOT IN, and uncorrelated IN. A second inner
table uses `g = (id + 1) % 5`, making the correlated IN result empty while its disjunction still returns the
89 range matches. Coverage includes derived tables, CTEs, UNION ALL multiplicities, JOIN WHERE/ON, HAVING,
SUM/CASE, literal IN lists, and an IN list containing a Boolean expression. The original 100-row
reproducer and row-dependent result controls run under all four predicate/projection pushdown combinations.
Detailed precedence variants run once with default connection settings over a 20-row fixture (ids 0–19,
with the same grouping pattern), whose disjunction returns 14 identifiers: 0–4 and 11–19. Query-scope
checks use that compact fixture under all four pushdown combinations. The 100-row CTE shadowing and
derived-table regressions retain their full pushdown matrix. `SubqueryCorrelationCachingTest` asserts that 1,000 outer rows
require one uncorrelated-IN analysis. `StatementCancellationTest` checks outer result identity and SQLState
57014 when EXISTS and correlated IN/OR are canceled between `next()` calls.
`UnqualifiedSubqueryCorrelationTest` checks the HAVING regression, the formerly unsupported WHERE forms,
inner-column shadowing, derived/CTE inputs, scalar and quantified subqueries under all four pushdown
combinations, plus both information_schema views with colliding outer names, prepared execution, and
qualified/correlated controls. `SubqueryColumnResolutionTest` verifies scan-free virtual binding,
conservative handling and caching of unknown inputs, and outer fallback for known inputs without the name.
The analysis-count test also checks that source columns are resolved once for 1,000 rows.
Nested CTE regressions in `CorrelatedInOrTest` cover the 100-row reproducer with qualified and unqualified
correlations, EXISTS and IN, CTE column aliases, an uncorrelated control, inherited-name shadowing, and
duplicate declarations under all four pushdown combinations. `CorrelatedScopeTest` checks that rewritten,
parenthesized SQL retains its CTE declarations in the parsed plan.
Derived-table regressions check the 100-row count, the scalar wrapper, joins with the derived input on
either side, CTE column aliases and filtering, exact identifiers, and preservation of the five-row outer
CTE under all four pushdown combinations. Parser tests confirm that the derived plan links to the local
CTE definition rather than the outer one.

Additional derived-table controls retain filters over inherited CTEs (count 2 directly and through a join),
computed projections (SUM value 10), and an alias matching an outer CTE name (count 100). The SUM control
checks the value without asserting the currently inconsistent JDBC type.

Separate pre-existing bugs reported on origin/main and reproduced by temporary review probes at `616b58e`
are tracked as [FROM-less recursive CTE anchors (#190)](https://github.com/Alipsa/JParq/issues/190) and
[derived integer-expression SUM type consistency (#191)](https://github.com/Alipsa/JParq/issues/191).
The probes were removed; these follow-ups are not fixes included in PR #189.

Longer-term reuse of prepared execution shapes for correlated subqueries is tracked in
[#192](https://github.com/Alipsa/JParq/issues/192). The test-only reduction in repeated precedence checks
and compact fixtures does not change query execution or add a prepared-statement cache.

A local Java 21 full-suite run measured `CorrelatedInOrTest` at 23.8 seconds after the test-only reduction,
versus 53.3 seconds in the preceding full-suite report. These are observations, not timing assertions.
