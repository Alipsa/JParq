Currently, the getSearchStringEscape method in JParqDatabaseMetaData returns an empty string (""). This is factually correct for how the metadata pattern searches are implemented right now,
as methods like getTables use JParqUtil.sqlLikeToRegex, which does not process any escape characters. However, this leaves a functionality gap,
as it's impossible to search for tables or columns with literal _ or % characters in their names using the standard metadata methods.

Given that the full LIKE operator in the engine (implemented in StringFunctions.like) does support an escape character, the best path forward
is to align the metadata methods with the engine's capabilities.

Therefore, you should:
1. Return `\` from `getSearchStringEscape()`. This is the most common and standard escape character in SQL.
2. Update the implementation of `getTables`, `getColumns`, and other metadata search methods.

The buildRegex method in JParqDatabaseMetaData currently uses JParqUtil.sqlLikeToRegex. You should modify it to use the logic found in
StringFunctions.toLikeRegex, which correctly handles a provided escape character.