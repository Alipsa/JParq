# Correlated execution shapes (#192)

Correlated subqueries bind outer Java values to stable placeholders instead of generating new literal SQL per row.
A result owns its execution-local cache. Simple physical-table subqueries reuse prepared plans; derived tables,
CTEs, joins, and set operations reuse parsed queries and validated schemas while rebuilding current input data.
Per-call results and readers are closed after consumption. Cached statements close with the owning result or join.
Binding frames restore ancestor bindings on success or failure. Neither results nor cancellation state cross executions.

Eligible correlated predicates are translated from the current bindings at execution time, preserving
per-row pushdown without caching an earlier row's filter values. Residual conditions still run in Java.
This does not decorrelate queries or cache results by correlation key: nested inputs are still read on each evaluation.
Typed binding also preserves the outer object's class, including INTEGER as Integer rather than a rendered literal's Long.
LATERAL table suppliers retain their existing literal execution path; row-varying literal SQL is excluded from
this cache so it does not retain a separate parsed query for every distinct outer value.

## Deterministic work counts

`CorrelatedExecutionShapeTest.preparationWorkIsIndependentOfOuterRowCount` counts actual calls to
`SqlParser.parseQuery` and `ParquetTable.validate`, including outer preparation and initial reads.
The same test was run against separately compiled main-branch production classes at `dbfcbda` and the implementation.
Both Statement and PreparedStatement gave these counts:

| Outer rows selected | Before parses | After parses | Before schema validations | After schema validations |
| --- | ---: | ---: | ---: | ---: |
| 10 | 12 | 2 | 12 | 2 |
| 40 | 42 | 2 | 42 | 2 |

The test deliberately fails against the baseline and passes with reuse. Other deterministic tests check constant
parse/validation work for derived inputs, local CTEs, grouped subqueries, joins, and uncorrelated subqueries.
`SubqueryExecutorLifecycleTest` checks that repeated correlated execution prepares an eligible shape once, returns
fresh results, closes every per-call result, and closes cached statements even when iteration or cleanup fails.

## Warm benchmark

Measured locally on macOS ARM64 with Java 21.0.12.1. The fixture contains 500 outer INTEGER ids with `g = id % 5`
and five inner rows with the same group calculation. Each mode runs one unmeasured warm-up and three measured
queries, consuming and checking all 500 identifiers. The timer includes preparation, executeQuery, iteration, and close.
Baseline: `dbfcbda`; after: the #192 implementation. Baseline and final measurements ran without concurrent tests.

| Predicate pushdown | Projection pushdown | Before samples (ms) | After samples (ms) | Before median (ms) | After median (ms) |
| --- | --- | --- | --- | ---: | ---: |
| true | true | 3758, 3778, 3786 | 1484, 1379, 1404 | 3778 | 1404 |
| true | false | 3299, 3208, 3200 | 1336, 1375, 1283 | 3208 | 1336 |
| false | true | 3217, 3462, 3066 | 1261, 1293, 1239 | 3217 | 1261 |
| false | false | 2615, 2703, 2593 | 1240, 1248, 1237 | 2615 | 1240 |

These small local samples show about 2.1–2.7 times faster full iteration for this fixture; they are not timing
assertions or a guarantee for other workloads. The remaining repeated Parquet opens and scans still cost time.

Run after compilation, using the Maven-generated runtime classpath:

```sh
mvn -DskipTests test-compile
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "target/test-classes:target/classes:$(cat target/runtime-classpath.txt)" \
  jparq.performance.CorrelatedSubqueryBenchmark
```

The manual benchmark creates and removes its own temporary fixtures and is not executed by Maven's test suite.
