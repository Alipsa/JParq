The codebase currently supports expressions in ORDER BY clauses, with some limitations.

However, there are two main limitations:
1. Set operations: still limited to column labels or positions. SqlParser.SetOrder only captures positional indices or column labels, no expression tree, so queries like ... UNION ... ORDER BY p
  + 10 are still unsupported (see src/main/java/se/alipsa/jparq/engine/SqlParser.java around the SetOrder parsing).
2. ORDER BY expressions outside SELECT: still limited. computeOrderKeys uses raw expression text but QueryProcessor can only evaluate ORDER BY entries that map to a schema field or to a
  projection label/physical column via orderByExpressions. An expression that is not in the SELECT list (e.g., SELECT name FROM employees ORDER BY salary + bonus) yields a key that has no
  evaluator mapping, so sorting falls back to null values—meaning this scenario remains unsupported (see src/main/java/se/alipsa/jparq/engine/SqlParser.java and src/main/java/se/alipsa/jparq/
  engine/QueryProcessor.java).
   
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

Please fix these 2 issues to ensure full SQL compliance for ORDER BY clauses.