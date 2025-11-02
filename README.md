[![Maven Central](https://maven-badges.herokuapp.com/maven-central/se.alipsa/jparq/badge.svg)](https://maven-badges.herokuapp.com/maven-central/se.alipsa/jparq)
[![javadoc](https://javadoc.io/badge2/se.alipsa/jparq/javadoc.svg)](https://javadoc.io/doc/se.alipsa/jparq)
# JParq
JParq is a JDBC driver for parquet files. It allows you to query parquet files using SQL.
It works by regarding a directory as a database and each parquet file in that directory as a table. Each parquet file must have a `.parquet` extension and each such file is referred to using the filename (minus the .parquet extension) as the table.

JParq relies heavily on Apache Arrow and Apache Parquet libraries for reading the parquet files and on jsqlparser to parse the sql into processable blocks.

Note: A large proportion of the code was created in collaboration with ChatGPT 5.

# Usage
```xml
<dependency>
  <groupId>se.alipsa</groupId>
  <artifactId>jparq</artifactId>
  <version>0.5.0</version>
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
  - `CASE` support
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators 
  - `AND`, `OR`, `NOT` logical operators 
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=` 
  - Null checks: `IS NULL`, `IS NOT NULL` 
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options
- `OFFSET` and `LIMIT` support
- `DISTINCT` support
- Functions support
  - Date functions
  - Aggregate functions (count, sum, avg, max, min)
  - CAST support
  - coalesce (The COALESCE() function returns the first non-null value in a list.)
  - String functions (all SQL standard string function supported)
  - Numeric functions (abs, ceil, floor, round, sqrt, truncate, mod, power, exp, log, rand, sign, sin, cos, tan, asin, acos, atan, atan2, degrees, radians)
- comments (line --) and block (/* */)
- Subquery support
  - In the SELECT Clause : Used to return a single value or a set of values. e.g.
    SELECT first_name, (
    SELECT department_name FROM departments WHERE departments.department_id = employees.department_id
    ) AS department_name
    FROM employees;
  - In the FROM Clause : Treated as a derived table or inline view. E.g:
    SELECT *
    FROM (SELECT first_name, salary FROM employees WHERE salary > 5000) AS "high_salaried"
  - In the WHERE Clause : Used to filter the results. e.g
    SELECT first_name
    FROM employees
    WHERE department_id IN (SELECT department_id FROM departments WHERE location_id>1500);
  - In the HAVING Clause : Used to filter groups. E.g:
    SELECT department_id, AVG(salary)
    FROM employees
    GROUP BY department_id
    HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);
- `GROUP BY` support
  - `COUNT(*)` aggregation
  - `HAVING` clause with conditions
  - support aggregation functions and case statements in the `GROUP BY` and `SELECT` clause
- exists support 
- any and all support
- INNER, LEFT, RIGHT, FULL, CROSS, and Self Join support
- union and union all support
- intersect and except support

## Roadmap: Might be implemented in the future
- CTE
- Windowing
  - Ranking functions
    - row_number, rank, dense_rank, PERCENT_RANK, CUME_DIST, ntile,
  - aggregate window functions
    - SUM, AVG, MIN, MAX, COUNT
  - Analytic Value/Navigation Functions
    -  lag, lead, FIRST_VALUE, LAST_VALUE, NTH_VALUE

#### String functions support details
##### Character Length and Position
- CHAR_LENGTH(string) or CHARACTER_LENGTH(string)	Returns number of characters in a string.	CHAR_LENGTH('hello') → 5
- OCTET_LENGTH(string)	Returns number of bytes in the string (depends on encoding).	OCTET_LENGTH('Å') → 2 (in UTF-8)
- POSITION(substring IN string)	Finds the position (1-based) of substring in string.	POSITION('l' IN 'hello') → 3

##### Substrings and Extraction
- SUBSTRING(string FROM start [FOR length])	Extracts substring starting at start, optionally limited by length.	SUBSTRING('abcdef' FROM 2 FOR 3) → 'bcd'
- LEFT(string, count) (optional extension)	Leftmost characters.	LEFT('abcdef', 3) → 'abc'
- RIGHT(string, count) (optional extension)	Rightmost characters.	RIGHT('abcdef', 2) → 'ef'

##### Concatenation
- CONCAT(string1, string2, …)	Concatenates two or more strings (SQL:2016 added variadic support).	CONCAT('a','b','c') → 'abc'

##### Case Conversion
- UPPER(string)	Converts to uppercase.	UPPER('sql') → 'SQL'
- LOWER(string)	Converts to lowercase.	LOWER('SQL') → 'sql'

##### Trimming and Padding
- TRIM([LEADING	TRAILING	BOTH] [characters] FROM string)
- LTRIM(string) (extension)	Trims leading spaces.	LTRIM(' hi') → 'hi'
- RTRIM(string) (extension)	Trims trailing spaces.	RTRIM('hi ') → 'hi'
- LPAD(string, length [, fill]) (SQL:2008 optional)	Pads string on the left.	LPAD('42', 5, '0') → '00042'
- RPAD(string, length [, fill]) (SQL:2008 optional)	Pads string on the right.	RPAD('42', 5, '0') → '42000'

##### Searching and Replacing
- OVERLAY(string PLACING replacement FROM start [FOR length])	Replaces part of string starting at start with replacement.	OVERLAY('abcdef' PLACING 'xyz' FROM 3 FOR 2) → 'abxyze f'
- REPLACE(string, search, replace) (SQL:2008)	Replaces all occurrences of search with replace.	REPLACE('banana', 'na', 'xy') → 'baxyxy'

##### Collation and Comparison
- COLLATE(string, collation_name)	Applies a specific collation to a string.	'abc' COLLATE "sv_SE"
- SIMILAR TO e.g.
  'cat' SIMILAR TO '(cat|dog)'      → TRUE
  'cab' SIMILAR TO 'c(a|o)b'        → TRUE
  'cab' SIMILAR TO 'c(a|e)b'        → FALSE
  'abc' SIMILAR TO 'a%'             → TRUE
- REGEXP_LIKE (pattern matching operators) e.g:
  REGEXP_LIKE('abc123', '^[a-z]+[0-9]+$')   → TRUE
  REGEXP_LIKE('AbC', 'abc', 'i')            → TRUE  -- 'i' = case-insensitive
  REGEXP_LIKE('cat', 'dog|cat')             → TRUE

##### Unicode and Codepoints
- CHAR(code)	Returns the character corresponding to a code point.	CHAR(65) → 'A'
- UNICODE(string)	Returns Unicode code point of first character.	UNICODE('A') → 65

##### SQL:2016–2023 Additions
- NORMALIZE(string [USING form])	Normalizes Unicode text (SQL:2016).	NORMALIZE('é') → 'é'
- STRING_AGG(expression, separator)	Aggregates values into a single string with a separator.	STRING_AGG(name, ', ') → 'Alice, Bob, Carol'
- JSON_VALUE, JSON_QUERY, JSON_OBJECT, JSON_ARRAY	JSON construction/extraction—technically not core string functions but string-returning functions standardized in SQL:2016–2023.

### Standard prompt
Please implement support for the SQL standard for 

Each section (starting with # above) should have its own test class to verify the functionality.
Create test to verify the functionality.
Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
All tests must pass after the implementation using `mvn -Dspotless.check.skip=true test` to ensure that there is no regression.
Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself). 
