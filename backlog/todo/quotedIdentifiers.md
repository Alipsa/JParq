Problem:

The core issue is the pervasive use of toLowerCase(Locale.ROOT) on SQL identifiers (CTE names, table names, column names, aliases) across the codebase. This is done to achieve case-insensitivity for unquoted identifiers but has the
unintended side effect of making quoted identifiers case-insensitive as well, which violates the SQL standard.

Key Problem Areas:

* SqlParser.java: The identifierKey method incorrectly converts all identifiers to lowercase.
* JParqPreparedStatement.java: The normalizeCteKey method also converts CTE names to lowercase.
* JParqResultSet.java: Uses toLowerCase when finding and resolving column names.
* JParqDatabaseMetaData.java: Uses toLowerCase for pattern matching in methods like getTables and getColumns

A significant refactoring is required to introduce a robust and consistent identifier handling mechanism. This would involve creating a new Identifier class to store the original identifier and its quoted
status, and then refactoring all the affected files to use this new class.

Fix the inconsistent Post-Parse Handling for quoted identifiers: While the parser preserves case, downstream processing can be inconsistent. For instance, Common Table
Expression (CTE) names are explicitly converted to lowercase, making them case-insensitive regardless of quoting.
1. Investigate the codebase to find shortcomings for quoted identifiers
2. Create a plan for fixing them including what the tests need to cover
3. Execute on the plan