According to the SQL standard (specifically starting with SQL:1999 and onwards), 
you should be able to ORDER BY any column that is available after the GROUP BY phase, 
regardless of whether it appears in the final output projection (SELECT list).

The codebase supports grouping by fields that are not included in the SELECT list. The engine's design uses the GROUP BY clause to define
how rows are grouped, and the SELECT clause to define the final projection from those groups, which is consistent with standard SQL behavior.

The primary limitation identified is:

* You cannot `ORDER BY` a grouping column if it is not in the `SELECT` list. The method sortAggregatedRows in AggregateFunctions.java
  explicitly throws an IllegalArgumentException if an ORDER BY column is not found in the final projection.

For example:
* SELECT SUM(hp) FROM mtcars GROUP BY cyl; — This is supported.
* SELECT SUM(hp) FROM mtcars GROUP BY cyl ORDER BY cyl; — This is currently NOT supported, as cyl is not in the SELECT list for the ORDER BY clause to
  reference.

The Fix: To support standard SQL, you need to swap the order of operations in your engine's pipeline:
1. Group & Aggregate: Calculate SUM(hp) and keep cyl as the key. 
2. Sort: Sort the results based on cyl. (The column cyl must still exist in memory here). 
3. Project: Finally, look at the SELECT list and discard cyl because the user didn't ask to see it.

The test OrderByTest.testOrderByOnlyInGroupBy() should pass after a successful implementation (it is currently disabled)