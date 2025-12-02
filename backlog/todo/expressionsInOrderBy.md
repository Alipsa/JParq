The codebase currently supports expressions in ORDER BY clauses, with some limitations.

Here's how it works:
You can use an expression in an ORDER BY clause if you give it an alias in the SELECT list and then use that alias in the ORDER BY clause. For
example: SELECT a + b AS c FROM t ORDER BY c.

However, there are two main limitations:
1. Set Operations: For UNION, INTERSECT, etc., the ORDER BY clause only supports column names or positional indices.
2. Aggregation: When using GROUP BY, you can only ORDER BY columns that are present in the SELECT list. For example, SELECT SUM(a) FROM t GROUP
   BY b ORDER BY b is not supported.

## The Aggregation Limitation (GROUP BY)
Current Issue: SELECT SUM(a) FROM t GROUP BY b ORDER BY b fails because b is not in the SELECT list.

Verdict: Critical Fix Needed. This is a clear violation of SQL-92 and SQL:1999 standards.

Why it happens: The engine is likely performing the Projection (discarding unselected columns) before the Sort.

Why it matters: BI tools (like Tableau or PowerBI) often generate queries that sort by the grouping key to ensure the chart renders correctly, even if they don't explicitly display the key label in the data grid. If your driver rejects this, these tools will crash or show errors.

The Logical Fix: Respect the Logical Order of Operations in SQL. Even though SELECT is written first, it is executed almost last.

Correct Execution Pipeline:
1. FROM/WHERE: Filter raw rows. 
2. GROUP BY: Collapse rows into groups. At this stage, the "row" contains the Grouping Keys + Aggregate Results. 
3. HAVING: Filter groups. 
4. ORDER BY: Sort the groups. (Crucial: The Grouping Keys are still available here!)
5. SELECT: Finally, pick which columns to return to the user and discard the rest.
   
## The Set Operations Limitation (UNION, etc.)
Current Issue: ORDER BY only supports column names or indices (e.g., ORDER BY 1), not expressions or aliases, when attached to a UNION.

Verdict: High Priority Fix. Standard SQL allows you to order the results of a Set Operation by:
1. Column names from the first query. 
2. Aliases defined in the first query. 
3. Expressions built from those columns.

Example of Valid Standard SQL:

```SQL
SELECT price AS p FROM old_items
UNION
SELECT cost AS p FROM new_items
ORDER BY p + 10; -- Ordering by an expression on the result
```
If your engine requires ORDER BY 1 (positional) or strictly ORDER BY p (name), 
it forces developers to write brittle SQL. 
Positional ordering (ORDER BY 1) has actually been deprecated in the SQL standard (though widely supported), so relying on it as the only workaround is not ideal.

## The "Expression in ORDER BY" Limitation
Current Issue: You can use an expression in an ORDER BY clause if you give it an alias in the SELECT list... 
Example: `SELECT a + b AS c ... ORDER BY c`.

Verdict: Moderate Fix Needed. Standard SQL allows you to sort by expressions without selecting them.

Valid Standard SQL:

```SQL
SELECT name FROM employees ORDER BY salary + bonus;
```
Here, salary + bonus is calculated purely for the sort. It is never shown to the user.

If we required the user to write SELECT name, salary+bonus as sort_key ... ORDER BY sort_key, 
we would be leaking implementation details into the public API.

## Implementation Strategy
To fix the GROUP BY issue (which is the most critical), we don't need to rewrite the whole engine. 
We just need to change the data flow:
- Current Flow (Broken): Aggregate -> Project (remove unused) -> Sort.
- New Flow (Fixed): Aggregate -> Sort -> Project (remove unused).