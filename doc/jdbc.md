# JDBC support

[Back to the README](../README.md#using-jparq-in-sql-clients)

JParq is read-only and supports only `SELECT` statements.

## Contents

- [Statements and result sets](#statements-and-result-sets)
- [Prepared statements](#prepared-statements)
- [JParqSql helper](#jparqsql-helper)
- [Predicate pushdown and cancellation](#predicate-pushdown-and-cancellation)
- [Transactions and connection state](#transactions-and-connection-state)
- [Metadata](#metadata)
- [Unsupported APIs](#unsupported-apis)

## Statements and result sets

Result sets support `TYPE_FORWARD_ONLY`, `CONCUR_READ_ONLY`, and `CLOSE_CURSORS_AT_COMMIT` holdability.

`executeUpdate`/DML, generated keys, and updatable result sets are not implemented.
`Statement` batching and `addBatch(String)` are unsupported.

## Prepared statements

SQL is planned when the `PreparedStatement` is created. Positional `?` parameters are bound with the standard
`setXxx` methods and rendered as safe SQL literals. Named `:paramName` parameters are also supported as a
non-standard extension.

Parameter metadata reports generic `VARCHAR`/`IN` characteristics because actual typing happens at execution.

Overloads requesting other result set types, generated keys, or key column selection throw
`SQLFeatureNotSupportedException`.

Batching without a SQL argument is supported and reports `SUCCESS_NO_INFO` per entry.

## JParqSql helper

`JParqSql` provides a callback-based query API:

```java
import se.alipsa.jparq.JParqSql;

JParqSql jparqSql = new JParqSql("jdbc:jparq:/home/user/data");
jparqSql.query("SELECT model, cyl, mpg FROM mtcars WHERE model LIKE 'Toyota%'", rs -> {
  try {
    while (rs.next()) {
      System.out.println(rs.getString(1) + ", " + rs.getInt(2) + ", " + rs.getDouble(3));
    }
  } catch (java.sql.SQLException e) {
    throw new IllegalStateException("Query failed", e);
  }
});
```

## Predicate pushdown and cancellation

Supported `WHERE` predicates are attached to Parquet readers, enabling statistics, column index, dictionary,
and bloom filter pruning. Unsupported terms are evaluated in Java.

`JParqPreparedStatement.getPushdownInfo()` reports the attached predicate and the residual filter.
See [predicate pushdown](parquet-and-hugging-face.md#predicate-pushdown) for supported expressions and details.

`Statement.cancel()` is supported, with SQLState `57014`.
See [statement cancellation](sql-reference.md#statement-cancellation) for execution and reuse behavior.

## Transactions and connection state

The connection is always read-only with `TRANSACTION_NONE`.

- `commit`, `rollback`, and savepoints are no-ops.
- `getAutoCommit()` always returns `true`.
- Changing isolation level, holdability, or poolability is not supported.
- `isValid()` returns `true` while the connection is open and `false` after it is closed.

## Metadata

`DatabaseMetaData` exposes tables and columns from the base directory and its subdirectories (schemas).
The catalog is the base directory name.

## Unsupported APIs

- `CallableStatement` and stored procedures
- Custom type mappings
- `createClob`, `createBlob`, and `createArrayOf`
- Network timeout and client info
