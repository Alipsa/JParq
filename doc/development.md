# Development

[Back to the README](../README.md)

## Building and testing

```bash
mvn verify
```

| Flag | Effect |
| --- | --- |
| `-Dspotless.check.skip=true` | Skip the Spotless formatting check |
| `-DskipTests` | Skip unit tests |
| `-DskipITs=true` | Skip integration tests |

Checkstyle, PMD, and Spotless run as part of the Maven build. Run `mvn spotless:apply` before committing to fix
formatting.

Surefire and Failsafe load Mockito as a startup Java agent, alongside JaCoCo, so inline mocking does not require
dynamic agent loading. To verify this, run `mvn -DargLine=-XX:-EnableDynamicAgentLoading verify`.

When invoking Surefire or Failsafe goals directly, run `initialize` first to resolve the agent path.
Phase-based commands such as `mvn test-compile surefire:test` already include this step.

Small PyArrow-generated Parquet fixtures are committed, so the build does not need Python. Regeneration
instructions and the exact PyArrow dependency are in [tools/createHfOptimizedData.py](../tools/createHfOptimizedData.py).

### Optional SQL Workbench/J integration test

SQL Workbench/J can run scripts without a GUI. To include its client loader and batch execution in the
packaged-driver tests, install it separately from the [official download](https://sql-workbench.eu/downloads.html),
build JParq, then run:

```sh
mvn initialize -Djacoco.skip=true -Dit.test=JParqPackagedDriverIT \
  -Djparq.it.sqlWorkbenchJar=/path/to/sqlworkbench.jar failsafe:integration-test failsafe:verify
```

The test lists schemas and tables, describes columns, queries all fixture codecs, opens a second batch session,
and checks that a deliberate query failure aborts the script. Without the property, the test is reported as
skipped.

It uses the selected `jparq.it.javaHome` runtime and needs Java 21 or later for build 133.
The client is not bundled, not downloaded during Maven builds, and not added to JParq's application classpath.

## Roadmap

### Might be implemented in the future

- Variables in SQL scripts, using `@variable_name` syntax with connection scope:
  ```sql
  declare @myVar INT = 10;
  SELECT * FROM myTable WHERE myColumn > @myVar;
  SELECT * FROM anotherTable LIMIT @myVar;
  ```
- `PIVOT` and `UNPIVOT` operators
- Parquet modular encryption via a keystore and/or a Key Management Service

### Out of scope

These will not be supported, at least not in the foreseeable future:

- Data modification (`INSERT`, `UPDATE`, `DELETE`, `MERGE`)
- Transaction control (`COMMIT`, `ROLLBACK`, `SAVEPOINT`)
- Data definition (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`)
- User management and security (`GRANT`, `REVOKE`, `CREATE USER`, etc.)
- Stored procedures and functions (`CREATE PROCEDURE`, `CREATE FUNCTION`)
- Triggers (`CREATE TRIGGER`, `DROP TRIGGER`)
- Advanced indexing and optimization hints
- Full-text search
- Temporary tables; use CTEs or `VALUES` tables instead
