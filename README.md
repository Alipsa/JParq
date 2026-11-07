[![Maven Central](https://img.shields.io/maven-central/v/se.alipsa/jparq)](https://mvnrepository.com/artifact/se.alipsa/jparq)
[![javadoc](https://javadoc.io/badge2/se.alipsa/jparq/javadoc.svg)](https://javadoc.io/doc/se.alipsa/jparq)

# JParq

JParq is a read-only JDBC driver for [Apache Parquet](https://parquet.apache.org/) files. It lets you query a
directory of Parquet files with standard SQL from any Java program, SQL client, or the bundled command line tool.

- A **directory** is a database.
- Every **`.parquet` file** is a table, named after the file without its extension.
- Every **subdirectory** is a schema.

```
/data
  ├── employees.parquet      -> PUBLIC.employees
  ├── departments.parquet    -> PUBLIC.departments
  └── sales
      ├── orders.parquet     -> sales.orders
      └── customers.parquet  -> sales.customers
```

JParq uses Apache Arrow and Apache Parquet for efficient columnar reads and JSqlParser to parse SQL. It aims to be
100% compliant with the read part of the SQL standard, plus a few common extensions such as `LIMIT`, `ASCII`,
`REPEAT`, and `DIFFERENCE`.

> **Standard first:** when common implementations differ from the SQL standard, JParq follows the standard.
> For example, `CONVERT ... USING` performs character set conversion as the standard defines. Use `CAST` for
> data type conversion.

> **Note:** most of the code was written in collaboration with ChatGPT Codex, with Copilot and me (Per Nyfelt) as
> code reviewers.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Connection options](#connection-options)
- [Command line interface](#command-line-interface)
- [Using JParq in SQL clients](#using-jparq-in-sql-clients)
- [Further documentation](#further-documentation)

## Requirements

- **Java 21.** This matches the client runtime supported by
  [Hadoop 3.5.0](https://hadoop.apache.org/docs/r3.5.0/index.html). Java 25 passes JParq's test suite on macOS
  ARM64 and can be used on a best-effort basis, but Hadoop 3.5.0 does not officially support it.
- Parquet files in a directory that the JVM running the driver can access.

## Installation

Add the dependency to your build. Replace `x.y.z` with the latest version from the Maven Central badge above.

```xml
<dependency>
  <groupId>se.alipsa</groupId>
  <artifactId>jparq</artifactId>
  <version>x.y.z</version>
</dependency>
```

### Fat JAR

A "fat" JAR that includes all dependencies is also published, using the Maven classifier `fat`
(`jparq-x.y.z-fat.jar`). Use it to add the driver to a tool's classpath. It is available from Maven Central and
from the [releases](https://github.com/Alipsa/JParq/releases) page.

## Usage

The driver registers itself when `se.alipsa.jparq.JParqDriver` is loaded, so placing the JAR on the classpath is
normally enough. If your runtime requires explicit registration, call
`Class.forName("se.alipsa.jparq.JParqDriver")` before opening a connection.

```java
import java.sql.DriverManager;
import java.sql.SQLException;

public class JParqExample {
  public static void main(String[] args) throws SQLException {
    String jdbcUrl = "jdbc:jparq:/home/user/data";
    try (var connection = DriverManager.getConnection(jdbcUrl);
         var statement = connection.createStatement();
         var result = statement.executeQuery("SELECT * FROM mtcars LIMIT 5")) {
      while (result.next()) {
        System.out.println(result.getString(1));
      }
    }
  }
}
```

For the callback-based `JParqSql` API and prepared statement details, see [JDBC support](doc/jdbc.md).

## Connection options

The JDBC URL has the form `jdbc:jparq:<directory>`. The directory can be given directly or with a `file://`
prefix. Options are appended as URL query parameters:

```text
jdbc:jparq:/home/user/data?predicatePushdown=false&projectionPushdown=false
```

| Option | Default | Description |
| --- | --- | --- |
| `caseSensitive` | `false` | Set to `true` to make table name resolution case-sensitive. |
| `datasetLayout` | `none` | Use `hf` to combine dataset shards into logical tables. |
| `predicatePushdown` | `true` | Set to `false` to evaluate the entire `WHERE` clause in Java. |
| `projectionPushdown` | `true` | Set to `false` to read every column. Column names are still validated. |

See [dataset layouts](doc/parquet-and-hugging-face.md#hugging-face-dataset-layouts) for naming rules and examples.

## Command line interface

JParq includes an interactive CLI with command history and line editing. Start it with the fat JAR and the
directory containing your Parquet files:

```shell
java -jar path/to/jparq.jar path/to/parquet/dir
```

Download the fat JAR from [releases](https://github.com/Alipsa/JParq/releases).
For a download command, wrapper script, and example session, see the [CLI guide](doc/cli.md).

### Commands

Anything that is not a command below is executed as SQL against the connected directory.

| Command | Description |
| --- | --- |
| `/connect <directory>` | Connect to a directory of Parquet files |
| `/close` | Close the current connection |
| `/list` | List available tables |
| `/describe <table>` | Show the column definitions, row groups, row counts, and page-index availability of a table |
| `/info` | Show information about the current connection, including pushdown settings |
| `/help` | Display help text |
| `/exit` | Exit the CLI |

`/describe` takes literal, unquoted names; see
[naming rules for dotted table names](doc/parquet-and-hugging-face.md#table-names-containing-dots-or-hyphens).

## Using JParq in SQL clients

Register `jparq-x.y.z-fat.jar` as a custom (generic) JDBC driver. The ordinary JAR needs all runtime dependencies
on the classpath, so adding it alone is not enough to connect and run queries.

| Setting | Value |
| --- | --- |
| Driver class | `se.alipsa.jparq.JParqDriver` |
| JDBC URL | `jdbc:jparq:/absolute/path/to/parquet-directory` |
| Windows URL example | `jdbc:jparq:C:/data/parquet` |
| Credentials | Leave username and password empty |
| Client JVM | Java 21 supported; Java 25 best effort; Java 17 cannot load the driver |

Tips:

- Check which Java runtime the client uses, including any bundled JVM. JParq is compiled for Java 21, so a
  client running on Java 17 can reject the driver classes before you can select a driver. Java 25 results do
  not extend Hadoop's official Java support.
- Choose read-only, forward-only result sets when the client offers that option.

### Client setup

These recipes use each client's custom driver facility. The GUI workflows have **not** been verified;
SQL Workbench/J build 133 has a tested headless batch integration (see
[Building and testing](doc/development.md#optional-sql-workbenchj-integration-test)).

- **DBeaver:** in Database → Driver Manager, create a generic driver named JParq. Add the fat JAR under
  Libraries, set the driver class, and use the complete JDBC URL when creating a connection.
  See [DBeaver's driver manager documentation](https://dbeaver.com/docs/dbeaver/Driver-Manager/).
- **DbVisualizer:** create a user driver from the Custom template in Driver Manager, add the fat JAR, set Driver
  Class, and configure a connection with the JDBC URL.
  See [DbVisualizer's installation guide](https://www.dbvis.com/docs/ug/reference-material/installing-a-jdbc-driver/).
- **SQuirreL SQL:** create a driver definition with the fat JAR on its extra classpath and the driver class,
  then create an alias using that driver and the complete JDBC URL.
- **SQL Workbench/J:** add a driver definition with the fat JAR and driver class, then select it in a
  connection profile and enter the complete JDBC URL.

### Checking a client

1. Browse schemas, tables, and columns.
2. Run a query, such as `SELECT * FROM salary LIMIT 5` when the directory contains `salary.parquet`.
3. Close and reconnect.

If discovery or browsing fails, record the client version, the Java version it actually runs on, the JParq
version, and the full exception.

A successful standalone JDBC test does not prove compatibility with the SQL or metadata calls a specific GUI
client generates.

The [compatibility verification record](requirements/done/sql-client-compatibility.md) lists tested runtimes,
platforms, loader scenarios, and remaining limits, including x64 coverage.

See [JDBC support](doc/jdbc.md) for result set modes, prepared statements, transactions, and metadata.

## Further documentation

| Guide | Contents |
| --- | --- |
| [SQL reference](doc/sql-reference.md) | Supported SQL, functions, data types, name resolution, and error codes |
| [JDBC support](doc/jdbc.md) | Prepared statements, result sets, metadata, and connection behavior |
| [Parquet and dataset layouts](doc/parquet-and-hugging-face.md) | File formats, predicate pushdown, and multi-file datasets |
| [CLI guide](doc/cli.md) | Download commands, wrapper scripts, and an example session |
| [Development](doc/development.md) | Building, testing, optional client integration, and roadmap |
| [Release notes](release.md) | Changes by version |
