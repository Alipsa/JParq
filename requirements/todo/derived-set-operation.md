# Derived tables containing set operations

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
