# Copilot Agent Onboarding Guide for JParq

## 1. Repository snapshot
- **Purpose:** JDBC driver that lets SQL clients query Parquet files via Apache Arrow/Parquet with SQL parsing handled by JSqlParser. The driver treats a directory as a database and each `.parquet` file as a table.
- **Tech stack:** Java 21 with Maven build. Heavy use of Apache Parquet/Arrow, Hadoop filesystem classes (local-mode), Avro, Jackson, SLF4J. Tests use JUnit 6 (Jupiter 6.0.0).
- **Repo size:** Single-module Maven project (~30 main Java classes, extensive test suite). Expect the first Maven run to download ~200 MB of dependencies and take ~1–2 minutes.

## 2. Project layout & key files
- **Build & metadata:** `pom.xml`, `checkstyle.xml`, `pmd-ruleset.xml`, `spotless-formatting.xml`, `version-plugin-rules.xml`.
- **Source tree:** `src/main/java/se/alipsa/jparq/` contains the JDBC implementation:
  - `JParqDriver`, `JParqConnection`, `JParqStatement`, `JParqPreparedStatement`, `JParqResultSet`, `JParqResultSetMetaData`, `JParqDatabaseMetaData`, plus helpers like `AggregateResultSetMetaData` and `JParqSql`.
  - Driver registration is handled via ServiceLoader descriptor `src/main/resources/META-INF/services/java.sql.Driver`.
- **Tests:** `src/test/java/jparq/` mirrors feature areas (e.g., `WhereTest`, `StringFunctionsTest`, `SubQueryTest`, `DateFunctionsTest`). Additional focused suites live under subpackages such as `jparq.strfunc` and `jparq.engine`.
- **Test data:** `src/test/resources/` holds `.parquet` fixtures (e.g., `mtcars.parquet`, `acme/*.parquet`) and a simplelogger config. Tests rely on these files—avoid renaming without updating paths.
- **Utility scripts:** `count-loc.sh` (line counts), `todo.md` (backlog notes).

## 3. Build, format, and test workflow
Always work with **Java 21** and **Maven ≥ 3.9.9**; the Maven Enforcer plugin blocks other versions.

### Bootstrap
1. Ensure Java 21 is active (`java -version`).
2. Run `mvn -version` once to confirm Maven ≥ 3.9.9 and warm the local repo.

### Fast feedback loop
1. `mvn -Dspotless.check.skip=true test`
   - Runs enforcer (version checks & bytecode ceilings), Checkstyle, resource processing, compilation, Spotless *download* (but skipped by the property), PMD, and the full JUnit suite (112 tests). Expect ~1 min after dependencies are cached.
   - If Spotless problems are reported when the skip flag is absent, fix with `mvn spotless:apply`.
2. If you touch integration-style tests ending with `*FS.java`, execute `mvn -Dspotless.check.skip=true verify` to run the Maven Failsafe phase (PredicatePushdownPerfFS). The `verify` goal tolerates FS failures (`testFailureIgnore=true`) but you should inspect results manually.
3. For release artifacts (jar + sources/javadoc), use `mvn -Dspotless.check.skip=true package`. This also triggers the steps above.

### Linting & static analysis
- **Checkstyle:** auto-runs in `validate`. Rules live in `checkstyle.xml`; they enforce formatting, imports, and Javadoc coverage.
- **PMD:** runs during `validate` with custom rules in `pmd-ruleset.xml`.
- **Spotless:** enforces Eclipse formatter profile (`spotless-formatting.xml`). Apply fixes via `mvn spotless:apply`.
- **Javadoc:** the plugin attaches docs during packaging; missing/incorrect Javadoc causes Checkstyle failures first.

### Common pitfalls
- First Maven invocation downloads many Hadoop/Parquet transitive dependencies—be patient; reruns are fast.
- Skipping Spotless (`-Dspotless.check.skip=true`) still downloads the plugin; that is expected.
- Integration tests use Hadoop’s local filesystem APIs; ensure temp directories are writable if you add new FS-based tests.
- Avoid using Java APIs beyond release 21—enforcer will fail the build.
- SQL parsing relies on **JSqlParser 5.3** (`com.github.jsqlparser`). Earlier versions have incompatible AST structures and method
  signatures; do not downgrade or copy code samples that target pre-5.x releases.

## 4. Coding & testing expectations
- **Tests are mandatory** for every behavioral change. Follow the existing naming convention (`FeatureTest`, `*FunctionsTest`, etc.) and place suites beside related features. Prefer parameterized JUnit 6 tests where practical.
- **Javadoc is required** on all public classes and methods. Include descriptions, `@param`, `@return`, and `@throws` as appropriate. Checkstyle enforces this strictly.
 note that private methods, including private utility methods, do not require Javadoc.
- **Style compliance:** obey Checkstyle, PMD, and Spotless. Use the existing helper methods/classes rather than duplicating logic (e.g., reuse `JParqSql` for query helpers).
- Pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- **Logging:** tests configure SLF4J Simple via `simplelogger.properties`. Use SLF4J APIs; avoid `System.out` in production code.

## 5. Validation pipelines & CI
- There are currently **no GitHub Actions workflows**. Assume maintainers will run the Maven commands above. Keep your local run logs handy to demonstrate passing `mvn -Dspotless.check.skip=true test` (and `verify` when you touch FS tests).

## 6. Navigational quick reference
- Root files: `pom.xml`, `README.md` (high-level usage guide & SQL support list), `LICENSE`, `checkstyle.xml`, `pmd-ruleset.xml`, `spotless-formatting.xml`, `todo.md`, `count-loc.sh`.
- Key packages under `src/main/java/se/alipsa/jparq/`:
  - `JParqDriver` registers the JDBC driver.
  - `JParqConnection` manages schema discovery over Parquet directories.
  - `JParqStatement` / `JParqPreparedStatement` execute parsed SQL via Arrow readers.
  - `JParqResultSet` & metadata classes project Arrow data into JDBC types.
  - `JParqSql` offers a lightweight wrapper for direct query execution in client code.
- Test suites of note:
  - `jparq.WhereTest`, `LikeTest`, `IsNullTest` cover predicate evaluation.
  - `jparq.engine.*` exercises parser and evaluator internals.
  - `PredicatePushdownPerfFS` (Failsafe) verifies filesystem behavior.

## 7. Working efficiently
- Start by reading `README.md` for supported SQL syntax and roadmap.
- Reuse existing helper methods/tests—many corner cases are already modeled (search within the `jparq` test package before writing new logic).
- When adding SQL features, mirror the structure: implement in the relevant engine class and add a dedicated test class (per README guidance).
- Keep parquet fixtures in `src/test/resources` small; large files slow down CI.
- If you modify dependencies, update `version-plugin-rules.xml` when the versions plugin complains.

## 8. Final reminder
Trust the guidance above. Only perform additional repository-wide searches when you suspect these instructions are incomplete or inaccurate.
