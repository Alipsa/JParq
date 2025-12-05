## SQL Standard Named Parameters

The current **SQL Standard** (e.g., in SQL:2003 and later specifications) generally defines parameter markers in prepared statements as **positional** placeholders, usually represented by a question mark (`?`). The host language (like Java in JDBC) then binds values to these markers based on their **position** (1st `?`, 2nd `?`, etc.).

**Named parameters** are more commonly found in:

* **Vendor-Specific SQL Dialects:** Many database systems (like SQL Server, Oracle, PostgreSQL, etc.) support named parameters, especially when calling **stored procedures** or **user-defined functions**. They often use a specific prefix (e.g., `@name`, `:name`) to denote the parameter. In this implementaion we will use the `:name` syntax.
* **Application-Level Abstractions:** Frameworks and libraries (like Spring's `NamedParameterJdbcTemplate`) implement named parameter support *on top of* standard JDBC. They parse the SQL string to map named placeholders to the positional `?` placeholders required by the underlying JDBC `PreparedStatement`.

### Why Use Named Parameters?

Named parameters offer significant advantages over positional parameters:
* **Readability:** They make the SQL clearer, as the name indicates the parameter's purpose (e.g., `WHERE cust_id = :customerId`).
* **Flexibility:** They allow parameters to be supplied in **any order** when calling a procedure (though this is primarily a feature of the SQL environment, not plain JDBC).
* **Maintainability:** If the query/procedure changes and a parameter is added or reordered, applications using named parameters often don't need to change the binding logic, unlike positional binding where all subsequent indices would need updating.

---

## Implementing Named Parameter Support in the JParq JDBC Driver

Since **plain JDBC** (specifically `PreparedStatement`) is designed around the positional parameter marker (`?`), implementing named parameter support in a JDBC driver usually involves a layer that translates the named format to the positional format.

Here are the important things to consider:

### 1. SQL Parsing and Translation

* **Identifier Recognition:** You need a parser that can correctly identify your custom named parameter syntax (e.g., `:paramName`, `@paramName`) within the SQL string.
* **Mapping:** You must create a mapping (e.g., a `Map<String, Integer>`) from the **parameter name** to its **positional index** (the 1-based index of the equivalent `?` marker).
* **SQL Rewrite:** The original SQL string with named parameters must be rewritten into a standard JDBC-compatible SQL string using only positional `?` markers, which is then passed to `Connection.prepareStatement()`.

### 2. Handling Dialect and Edge Cases

* **Escape/Literal Conflicts:** Your parser must distinguish between a named parameter and a seemingly identical string that is actually part of an SQL literal, a comment, or vendor-specific syntax. For example, a string like `'This is :not a parameter'` should not be parsed as a named parameter.
* **Reused Parameters:** If a query uses the same named parameter multiple times (e.g., `... WHERE a = :val OR b = :val`), your implementation must bind the same value to *each* corresponding positional `?` marker.
  * *Implementation detail:* The single named parameter will map to multiple positional indices in the rewritten SQL.

### 3. User Experience and API

* **Parameter Binding API:** You would need to expose an API (perhaps a custom extension of `PreparedStatement` or a wrapper) that accepts the parameter name for binding, rather than the index.
  * *Example:* `void setString(String paramName, String value)`
* **Metadata:** You should consider how to handle parameter metadata. Standard JDBC parameter metadata (obtained via `PreparedStatement.getParameterMetaData()`) is index-based, but a custom implementation could provide name-to-index translation to offer more helpful metadata to the user.

## TODO 
- Add support for named parameters as described above.
- Update JParqDatabaseMetaData.supportsNamedParameters() to return true
- Add tests to prove that named parameters work as expected in prepared statements.
