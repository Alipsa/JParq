`typed.parquet` is written by parquet-avro with nullable Avro `time-millis` (INT)
and `time-micros` (LONG) logical types. Its physical row order is:

| id | t | u |
| --- | --- | --- |
| 2 | 39600000 | 39600000000 |
| 1 | 36000000 | 36000000000 |
| 3 | NULL | NULL |

The non-null values represent 11:00:00 and 10:00:00 respectively. The reversed
order verifies sorting independently of file order. `jparq.ParquetTimeTest`
covers issue #251, including JDBC metadata/getters, casts, predicates, NULLs,
and ordering with each combination of predicate and projection pushdown settings.
TIME literal comparisons currently remain in the residual filter; a planning test
records this behavior and checks that eligible integer conjuncts are still pushed down.
Update that planning test when [#359](https://github.com/Alipsa/JParq/issues/359) implements TIME pushdown.

Regenerate from the repository root using Java 21 and the project's dependencies:

```sh
mvn dependency:build-classpath -Dmdep.outputFile=target/time-fixture-classpath.txt
java -cp "$(cat target/time-fixture-classpath.txt)" tools/CreateTimeTestData.java
```
