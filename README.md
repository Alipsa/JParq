[![Maven Central](https://img.shields.io/maven-central/v/se.alipsa/jparq)](https://mvnrepository.com/artifact/se.alipsa/jparq)
[![javadoc](https://javadoc.io/badge2/se.alipsa/jparq/javadoc.svg)](https://javadoc.io/doc/se.alipsa/jparq)

# JParq

JParq is a JDBC driver for Apache Parquet files. It treats a directory as a database and every `.parquet` file in that
directory as a table. The table name is the filename without the `.parquet` extension. JParq uses Apache Arrow and Apache
Parquet for efficient columnar reads and jsqlparser to parse SQL statements. It aims to be 100% compliant with the
read part of the SQL standard. There are a few common extensions supported as well e.g. LIMIT, ASCII, REPEAT, DIFFERENCE.

Note: When common implementations differ from the SQL standard, we stick to the standard. An example of this is
convert which in many databases is used for data type casting, but in the SQL standard is used for character set
conversion. If you want to convert data types, use CAST instead.

> **Note**
> The majority of the code was created in collaboration with (vibe coded with) ChatGPT Codex with Copilot and 
> myself (Per Nyfelt) as code reviewers.

## Requirements

- Java 21 (higher is not supported due to Hadoop restrictions)
- Parquet files stored in a directory accessible from the JVM running the driver

## Installation

Add the dependency to your build. Replace `x.y.z` with the latest version number from the Maven Central badge above.

```xml
<dependency>
  <groupId>se.alipsa</groupId>
  <artifactId>jparq</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Usage

The driver registers itself when the `se.alipsa.jparq.JParqDriver` class is loaded, so simply placing the JAR on the
classpath is normally enough. If your runtime requires explicit registration you can call
`Class.forName("se.alipsa.jparq.JParqDriver")` before obtaining a connection.

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import se.alipsa.jparq.JParqSql;

public class JParqExample {

  // Standard JDBC
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
  void selectMtcarsToyotas() {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars WHERE model LIKE 'Toyota%'", rs -> {
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

### Connection options

- `caseSensitive` — defaults to `false`. Set to `true` to make table-name resolution case sensitive.
  ```text
  jdbc:jparq:/home/user/data?caseSensitive=true
  ```
- Paths may be specified directly or using the `file://` prefix.

## SQL Support
The following SQL statements are supported:
- `SELECT` with support for
  - `*` to select all columns
  - Qualified wildcard projections (table.*).
  - alias support for columns and tables
  - Support computed expressions with aliases (e.g. SELECT mpg*2 AS double_mpg)
  - Quoted identifiers using "double quotes" as per the SQL standard.
  - `CASE` support
- `SELECT` statements with `WHERE` supporting:
  - `BETWEEN`, `IN`, `LIKE` operators
  - `AND`, `OR`, `NOT` logical operators
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Null checks: `IS NULL`, `IS NOT NULL`
- `ORDER BY` clause with multiple columns and `ASC`/`DESC` options
  - support for optional explicit NULLS FIRST and NULLS LAST
- `OFFSET` and `LIMIT` support
  - Also support the PostgreSQL shorthand `LIMIT n OFFSET m` syntax
- Standard row-limiting syntax (FETCH FIRST / OFFSET … FETCH)
- `DISTINCT` support
- Functions support
  - Date functions
  - Aggregate functions (count, sum, avg, max, min)
  - CAST support
  - CONVERT support (for both character set conversion with USING and data type conversion without USING, as per SQL standard and common extensions)
  - coalesce (The COALESCE() function returns the first non-null value in a list.)
  - String functions (all SQL standard string function supported)
    - also support || operator for string concatenations
  - Numeric functions (abs, ceil/ceiling, floor, round, sqrt, truncate/trunc, mod, power/pow, exp, log, log10,
    rand/random, sign, sin, cos, tan, cot, asin, acos, atan, atan2, degrees, radians, pi)
  - ARRAY constructor function
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
- Join support: INNER, LEFT, RIGHT, FULL, CROSS, and Self Join, join ... using syntax
- union and union all support
- intersect and except support
- Complete set-operation coverage.
  - EXCEPT
  - INTERSECT
  - INTERSECT ALL
  - EXCEPT ALL
  - nesting of set operations
- CTE (Common Table Expressions) support
- Windowing
  - Ranking functions
    - ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE
  -  Aggregate window functions
    - SUM, AVG, MIN, MAX, COUNT
  - Analytic Value/Navigation Functions
    -  LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- Advanced GROUP BY constructs i.e:
  - GROUPING SETS
  - ROLLUP
  - CUBE
- Derived Tables: UNNEST with or without a table wrapper, LATERAL derived tables, VALUES table constructors
- INFORMATION_SCHEMA.COLUMNS and INFORMATION_SCHEMA.TABLES

#### String functions support details
##### Character Length and Position
- CHAR_LENGTH(string) or CHARACTER_LENGTH(string)       Returns number of characters in a string.       CHAR_LENGTH('hello') → 5
- OCTET_LENGTH(string)  Returns number of bytes in the string (depends on encoding).    OCTET_LENGTH('Å') → 2 (in UTF-8)
- POSITION(substring IN string) Finds the position (1-based) of substring in string.    POSITION('l' IN 'hello') → 3

##### Substrings and Extraction
- SUBSTRING(string FROM start [FOR length])     Extracts substring starting at start, optionally limited by length.     SUBSTRING('abcdef' FROM 2 FOR 3) → 'bcd'
- LEFT(string, count) (optional extension)      Leftmost characters.    LEFT('abcdef', 3) → 'abc'
- RIGHT(string, count) (optional extension)     Rightmost characters.   RIGHT('abcdef', 2) → 'ef'

##### Concatenation
- CONCAT(string1, string2, …)   Concatenates two or more strings (SQL:2016 added variadic support).     CONCAT('a','b','c') → 'abc'
- Support for the || operator, covering both binary and text concatenation, as well as null propagation.

##### Case Conversion
- UPPER(string) Converts to uppercase.  UPPER('sql') → 'SQL'
- LOWER(string) Converts to lowercase.  LOWER('SQL') → 'sql'

##### Trimming and Padding
- TRIM([LEADING TRAILING        BOTH] [characters] FROM string)
- LTRIM(string) (extension)     Trims leading spaces.   LTRIM(' hi') → 'hi'
- RTRIM(string) (extension)     Trims trailing spaces.  RTRIM('hi ') → 'hi'
- LPAD(string, length [, fill]) (SQL:2008 optional)     Pads string on the left.        LPAD('42', 5, '0') → '00042'
- RPAD(string, length [, fill]) (SQL:2008 optional)     Pads string on the right.       RPAD('42', 5, '0') → '42000'

##### Searching and Replacing
- OVERLAY(string PLACING replacement FROM start [FOR length])   Replaces part of string starting at start with replacement.    OVERLAY('abcdef' PLACING 'xyz' FROM 3 FOR 2) → 'abxyze f'
- REPLACE(string, search, replace) (SQL:2008)   Replaces all occurrences of search with replace.        REPLACE('banana', 'na', 'xy') → 'baxyxy'

##### Collation and Comparison
- COLLATE(string, collation_name)       Applies a specific collation to a string.       'abc' COLLATE "sv_SE"
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
- CHAR(code)    Returns the character corresponding to a code point.    CHAR(65) → 'A'
- UNICODE(string)       Returns Unicode code point of first character.  UNICODE('A') → 65

##### SQL:2016–2023 Additions
- NORMALIZE(string [USING form])        Normalizes Unicode text (SQL:2016).     NORMALIZE('é') → 'é'
- STRING_AGG(expression, separator)     Aggregates values into a single string with a separator.        STRING_AGG(name, ', ') → 'Alice, Bob, Carol'
- JSON_VALUE, JSON_QUERY, JSON_OBJECT, JSON_ARRAY       JSON construction/extraction—technically not core string functions but string-returning functions standardized in SQL:2016–2023.

##### Non-standard extensions
- ASCII(string)    Returns the ASCII code of the first character.    ASCII('A') → 65
- LOCATE(substring, string[, start])    Locates substring in string with optional start position.    LOCATE('c', 'abcabc', 3) → 3
- REPEAT(string, count)    Repeats the string N times.    REPEAT('a', 4) → 'aaaa'
- SPACE(count)    Creates a string of N space characters.    SPACE(5) → '     '
- INSERT(string, start, length, replacement)    Inserts replacement at position after removing length characters.    INSERT('abcdef', 3, 2, 'XYZ') → 'abXYZef'
- SOUNDEX(string)    Computes the Soundex phonetic code.    SOUNDEX('Robert') → 'R163'
- DIFFERENCE(string1, string2)    Calculates similarity based on Soundex codes (0-4).    DIFFERENCE('Smith', 'Smyth') → 4

## Roadmap: _Might_ be implemented in the future

### Non standard extensions
- Named parameters i.e. :paramName syntax for prepared statements
- Allow omission of from clause in some cases
  - Several popular databases allow you to omit the FROM clause when you are only selecting literal values, performing arithmetic, or evaluating scalar functions (functions that return a single value). In these cases, the query returns a single row e.g: `SELECT CURRENT_DATE as cur_date`.
- Support for variable assignment and use within SQL scripts.
  - @variable_name syntax to define a variable that exists for the duration of the connection
    - Example (direct assignment, connection scope):
      declare @myVar INT = 10;
      SELECT * FROM myTable WHERE myColumn > @myVar;
      SELECT * FROM anotherTable LIMIT @myVar;
- PIVOT and UNPIVOT operators.
- TABLESAMPLE clause for sampling rows from a table.
- Support for modular encryption via a keystore and/or a Key Management Service

## Out of scope (will not be supported, at least not in the foreseeable future)
- Data modification statements (INSERT, UPDATE, DELETE, MERGE)
- Transaction control (COMMIT, ROLLBACK, SAVEPOINT)
- Data definition statements (CREATE, ALTER, DROP, TRUNCATE)
- User management and security (GRANT, REVOKE, CREATE USER, etc.)
- Stored procedures and functions (CREATE PROCEDURE, CREATE FUNCTION)
- Triggers (CREATE TRIGGER, DROP TRIGGER)
- Advanced indexing and optimization hints
- Full-text search capabilities
- TEMPORARY TABLES, you need to use CTE's or value tables instead.

### Build and test

```bash
mvn verify
```
- To run without spotless, add -Dspotless.check.skip=true
- To skip unit tests, add -DskipTests
- To skip integration tests add -DskipITs=true

This project uses Checkstyle, PMD, and Spotless. The checks run automatically as part of the Maven build. Use the
`spotless:apply` goal before committing if you need to fix formatting issues.

### Release notes

See [release.md](release.md) for the full version history and work in progress.
