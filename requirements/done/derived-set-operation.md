# Derived tables containing set operations

Status: completed by PR #221, with nullable Boolean regression coverage added for issue #230.

## Original requirement

Reported and reconfirmed by review probes against main at `4b7a0e3`; not independently rerun during planning.

Wrapping a set operation in a derived table throws `ClassCastException` because a `SetOperationList` is cast
to `PlainSelect`. For a Parquet table `o` with a nullable numeric column `n`, reproduce with:

```sql
SELECT c FROM (
  SELECT n > 1 AS c FROM o
  UNION ALL
  SELECT n > 3 AS c FROM o
) d
```

Expected behavior: plan and execute the derived set operation, preserving both branches' rows, output types,
and SQL NULL values. Investigate assumptions about `PlainSelect` in derived-table parsing and planning.
Add regression tests in the `jparq` package for derived `UNION` and `UNION ALL`, including outer projections,
filters, ordering, empty inputs, and duplicate handling.

This is a separate pre-existing defect from computed Boolean type preservation in
[v1.4.0-issues.md](v1.4.0-issues.md). Top-level set-operation Boolean typing remains covered by that issue's
[plan](v1.4.0-issues-plan.md); resolving the derived set-operation exception is outside that plan.

## Completion

Implemented by [PR #221](https://github.com/Alipsa/JParq/pull/221). Follow-up coverage for
[issue #230](https://github.com/Alipsa/JParq/issues/230) in
`DerivedSetOperationTest.nullablePhysicalComparisonsRemainBoolean` exercises the exact query above
and its `UNION` variant over a nullable physical numeric column, with predicate and projection pushdown
both enabled and disabled. It verifies JDBC BOOLEAN metadata, Java Boolean values, and NULL and duplicate
counts, including stable metadata after exhaustion. An outer `WHERE c IS NULL` filter retains two NULL
rows for `UNION ALL` and one for `UNION`; these rows also verify that JDBC `getBoolean` returns false
with `wasNull()` set to true. The connection settings are exercised in both modes; these queries do not
assert that a physical Parquet predicate is pushed down.
