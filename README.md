# JParq
JParq is a JDBC driver for parquet files. It allows you to query parquet files using SQL.
It works by regarding a directory as a database and each parquet file in that directory as a table. Each parquet file must have a `.parquet` extension and each such file is referred to using the filename (minus the .parquet extension) as the table.

JParq relies heavily on Apache Arrow and Apache Parquet libraries for reading the parquet files and on jsqlparser to parse the sql into processable blocks.

Note: A large proportion of the code was created in collaboration with ChatGPT 5.

# Usage
```xml
<dependency>
  <groupId>se.alipsa</groupId>
  <artifactId>parquet-jdbc</artifactId>
  <version>0.1.0</version>
</dependency>
```


```java
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import se.alipsa.jparq.JParqSql;

public class JParqExample {

  // Standard jdbc
  void selectMtcarsLimit() throws SQLException {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM mtcars LIMIT 5")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
      }
    }
  }

  // Using the JParqSql helper
  void selectMtcarsToyotas() throws SQLException {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars where model LIKE('Toyota%')", rs -> {
      try {
        while (rs.next()) {
          System.out.println(rs.getString(1) + ", " + rs.getInt(2) + ", " + rs.getDouble(3));
        }
      } catch (SQLException e) {
        System.out.println("Query failed: " + e);
      }
    });
  }
}
```
The driver is automatically registered using the service interface, but if your client needs the driver for some reason,
the Driver class name is `se.alipsa.jparq.JParqDriver`.
e.g:
```groovy
Class.forName("se.alipsa.jparq.JParqDriver")
Connection conn = DriverManager.getConnection(jdbcUrl)
// etc...
```

## SQL Support
The following SQL statements are supported:
- `SELECT` with support for
  - `*` to select all columns
  - alias support for columns and tables
  - Support computed expressions with aliases (e.g. SELECT mpg*2 AS double_mpg)
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators 
  - `AND`, `OR`, `NOT` logical operators 
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=` 
  - Null checks: `IS NULL`, `IS NOT NULL` 
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options
- `DISTINCT` support
- Functions support
  - Date functions

### To be implemented in the near future
- Functions support
  - Numeric functions
  - String functions
- `GROUP BY` with simple grouping
  - `COUNT(*)` aggregation
  - `HAVING` clause with simple conditions
  - `SUM`, `AVG`, `MIN`, `MAX` aggregation functions in `SELECT` clause
- `OFFSET` support
- Subquery support

### Might be implemented in the future
- Join support
- CTE
- Windowing
