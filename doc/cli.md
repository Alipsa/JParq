# Command line interface

[Back to the README](../README.md#command-line-interface)

JParq includes an interactive CLI with command history and line editing. Start it with the fat JAR and the
directory containing your Parquet files:

```shell
java -jar path/to/jparq.jar path/to/parquet/dir
```

## Downloading the fat JAR

Download the latest fat JAR from [releases](https://github.com/Alipsa/JParq/releases), or with:

```shell
LATEST_VERSION=$(curl -s https://repo1.maven.org/maven2/se/alipsa/jparq/maven-metadata.xml | sed -nE 's/.*<release>(.*)<\/release>.*/\1/p') && \
curl -o jparq.jar https://repo1.maven.org/maven2/se/alipsa/jparq/$LATEST_VERSION/jparq-$LATEST_VERSION-fat.jar
```

## Wrapper script

For convenience, put a wrapper script (for example `~/bin/jparq`) on your path:

```shell
#!/usr/bin/env bash
java -jar /path/to/jparq.jar "$@"
```

## CLI commands

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
[naming rules for dotted table names](parquet-and-hugging-face.md#table-names-containing-dots-or-hyphens).

## Example session

```
$ java -jar ~/lib/jparq.jar target/test-classes/acme
JParq CLI version 1.4.0
Connected to /home/user1/project/JParq/target/test-classes/acme
jparq(acme)> select * from employees e left join salary s on e.id = s.employee
id | first_name | last_name  | id | employee | salary   | change_date
---+------------+------------+----+----------+----------+------------
1  | Per        | Andersson  | 1  | 1        | 150000.0 | 2020-03-01
1  | Per        | Andersson  | 6  | 1        | 160000.0 | 2021-01-01
1  | Per        | Andersson  | 8  | 1        | 165000.0 | 2021-08-01
2  | Karin      | Pettersson | 2  | 2        | 180000.0 | 2020-03-01
3  | Tage       | Lundström  | 3  | 3        | 130000.0 | 2021-01-01
3  | Tage       | Lundström  | 7  | 3        | 140000.0 | 2021-12-01
4  | Arne       | Larsson    | 4  | 4        | 195000.0 | 2020-10-01
5  | Sixten     | Svensson   | 5  | 5        | 230000.0 | 2020-12-15
jparq(acme)> /exit
$
```
