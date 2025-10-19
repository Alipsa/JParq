# JParq
JParq is a JDBC driver for parquet files. It allows you to query parquet files using SQL.
It works by regarding a directory as a database and each parquet file in that directory as a table. Each parquet file must have a `.parquet` extension and each such file is referred to using the filename (minus the .parquet extension) as the table.

JParq relies heavily on Apache Arrow and Apache Parquet libraries for reading the parquet files and on jsqlparser to parse the sql into processable blocks.

## SQL Support
The following SQL statements are supported:
- `SELECT` with support for
  - `DISTINCT` support in `SELECT` clause
  - `ALIAS` support for columns 
  - `*` to select all columns
  - Date functions
  - Numeric functions
  - String functions
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators 
  - `AND`, `OR`, `NOT` logical operators in `WHERE` clause
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=` in `WHERE` clause
  - Null checks: `IS NULL`, `IS NOT NULL` in `WHERE` clause
  - `LIMIT` clauses
- `GROUP BY` with simple grouping
  - `COUNT(*)` aggregation
  - `HAVING` clause with simple conditions
  - `SUM`, `AVG`, `MIN`, `MAX` aggregation functions in `SELECT` clause
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options

