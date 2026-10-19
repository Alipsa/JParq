# SQL client compatibility verification (#179)

The implementation fixes two reproduced failures: the assembled JAR lost Hadoop
filesystem providers, and Hadoop could not resolve the local filesystem when a
client supplied a foreign thread context class loader (TCCL). The configuration
factory explicitly supplies the local filesystem and JParq's defining loader.
It normalizes private copies of caller configurations, preserving caller options
without changing caller-owned instances. There is no filesystem preload, TCCL
swap, or new driver initialization step.

Assembly merges service descriptors, excludes dependency module descriptors and
invalid dependency signatures (including `.EC`), removes stale JAR indexes, and
enables multi-release dependency classes. A single file-aggregator preserves all
root `META-INF/LICENSE*` and `META-INF/NOTICE*` dependency text together in
`META-INF/NOTICE`; it matches filenames case-insensitively. The artifact test
compares every source attribution against the aggregate after normalizing line
endings. The current graph contributes 68 license and 64 notice files from 68
runtime dependencies. The ordinary artifact, fat classifier, and executable CLI entry point remain.

The merged dependency text does not map each text to its dependency JAR: its
header lists only entry names, and individual texts have no added separators.
It is a text-preservation measure, not a dependency-to-license inventory. Two
`file-aggregator` handlers were tried during review verification; Assembly 3.8.0
reuses the handler instance by name, so the second configuration displaced the
first and the attribution test detected missing content. The combined
`META-INF/NOTICE` is retained deliberately; there is no `META-INF/LICENSE`.
JParq's own unmodified `LICENSE` is included at the root of both the thin and
fat JARs, outside the dependency aggregator, and checked against the source file.

The aggregator adds a build-time timestamp, and the build does not set
`project.build.outputTimestamp`. Checksums below identify the specific tested
artifacts, not reproducible outputs; a rebuild is expected to differ.

Java 21 remains the compiler release and supported runtime. JParq uses Java 21
APIs, including `List.getFirst()`; lowering the minimum would require a separate
source and dependency audit. Java 25 remains best effort because
[Hadoop 3.5.0](https://hadoop.apache.org/docs/r3.5.0/index.html) declares Java 17/21
client support, not Java 25. The build enforcer remains a build constraint.

## Reproduction

A clean pre-fix package on macOS ARM64 / Java 21.0.12.1 reproduced:

| Artifact | Driver TCCL | Platform TCCL |
| --- | --- | --- |
| Original fat JAR | `No FileSystem for scheme "file"` | same failure |
| Original fat JAR with filesystem service merge only | 4 tables, salary count 8 | same failure |
| Original thin JAR plus resolved runtime dependencies | 4 tables, salary count 8 | same failure |

Each cell ran in a fresh JVM with the driver isolated below the platform loader.
Pre-fix SHA-256 checksums:

- Fat: `ea3960f658d4a14e68be9cdfdf3676a1db8c830778a6a220dc9505bce65219b6`
- Thin: `6b603782b8eb8a7d96765c41a141edb2cb3c6dd69aff8fa3edc533690e5066c3`

## Automated coverage

`JParqPackagedDriverIT` launches a copied JDK-only probe, never the whole test or
production output directory. Automatic DriverManager discovery, explicit load,
isolated driver/foreign TCCLs, foreign-first Hadoop cache initialization,
concurrent first connections, discovery-only scanning, and loader replacement
run in separate subprocesses. Replacement deliberately creates two driver loaders
in one subprocess, closing the first loader and its JDBC resources before the
second reads the files. Thin-plus-runtime-dependency controls are independent of
the assembled artifact. All subprocesses have timeouts and captured diagnostics.

The probe validates connections and schema/table/column metadata, consumes every
row/column, and compares SHA-256 digests with reference reads on the ordinary
Maven classpath. Reference reads use UTC, length-framed cells and explicit NULL
markers; array-valued cells are compared by contents. Known row counts are
checked independently: salary 8, mtcars 32, optimized 16,384, and GZIP 3. GZIP is
generated deterministically by parquet-java during test setup (id 1–3 and text
`gzip-1`–`gzip-3`, asserted independently). Maven requires no Python or fixture
generation tool. ZSTD and Snappy use the committed mtcars/optimized fixtures.

Artifact assertions cover JDBC/Hadoop descriptors, provider class presence,
module descriptor exclusions, complete Hadoop provider merging from every input
runtime JAR, the multi-release flag, and actual versioned resource selection in
the selected subprocess JVM. A manual enabled/disabled-manifest comparison also
produced identical digests for every uncompressed, ZSTD, and Snappy fixture value
on Java 21 and 25; the flag is retained to preserve dependency behavior. Non-SPI JLine resources below nested
`META-INF/services/org/jline/...` paths are not treated as Java service files.
Discovery also finds a second test JDBC provider and records class loading in a
separate file, rejecting Hadoop/Parquet loads before connection. TCCL identity
is checked after successful reads and a deliberately failing query.

Runtime selection is separate from Maven's JDK. Build on Java 21:

```sh
mvn clean verify
mvn -Dit.test=JParqPackagedDriverIT -Djparq.it.javaHome=/path/to/jdk25 failsafe:integration-test failsafe:verify
mvn -Dit.test=JParqPackagedDriverIT -Djparq.it.javaHome=/path/to/jdk17 failsafe:integration-test failsafe:verify
```

Required properties are plugin-level Failsafe settings, validated for null/blank
values. The runtime default is a POM property, overridable with `-D`.
`generate-test-resources` writes the runtime dependency classpath used by thin
controls, so rerunning direct Failsafe goals requires a prior build in that
checkout. A Java 17 runtime runs only the source-mode driver rejection probe;
functional scenarios are skipped via JUnit assumptions. The rejection must be
`UnsupportedClassVersionError` identifying JParqDriver. A missing startup marker,
source compilation failure, or runtime below Java 17 fails the test setup.
Negative configuration probes confirmed blank runtime/artifact properties and a
Java 11 feature version are rejected before launching the driver probe.

## Results and limits

Original-delivery validation date: 2026-10-19. Platform results apply only to
ARM64, not x64. Updated review verification is recorded below.
The Linux container initially had 1 GiB RAM: Checkstyle exhausted its default
heap, and a targeted test-only verification reached the coverage gate with only
15% coverage (expected when selecting three unit tests). A subsequent full run
was interrupted to apply the user-approved increase to 4 GiB while preserving
the existing filesystem. The final run uses all tests and unskipped checks with
`MAVEN_OPTS=-Xmx1g`.

| Environment | Runtime | Result |
| --- | --- | --- |
| macOS 27.0.1 ARM64 | BellSoft 21.0.12.1 | Clean verification passed; final `mvn verify` with Workbench/J passes 1,243 unit tests and 16 IT cases (one skip in each group), coverage and all quality checks |
| macOS 27.0.1 ARM64 | BellSoft 25.0.4.1 | Packaged/client matrix passes (13 cases, rejection skipped); best effort |
| macOS ARM64 | BellSoft 17.0.20.1 | Driver rejection and artifact checks pass; 11 functional/client scenarios skipped |
| Ubuntu container ARM64 (kernel 6.18.35) | BellSoft 21.0.12.1 | `mvn clean verify` with Workbench/J passes: 1,243 unit tests (no skips), 16 IT cases (rejection skipped), coverage and all quality checks |
| Linux ARM64 | Java 25 | Untested: no Java 25 runtime installed |
| Windows 11 ARM64 (10.0.26300.9711) | Microsoft 21.0.12.1 | 3 configuration tests and 13 packaged/client cases pass (rejection skipped); `HADOOP_HOME` unset and no `winutils.exe` |
| Windows ARM64 | Java 25 | Untested: no Java 25 runtime installed |
| macOS/Linux/Windows x64 | Any | Untested: no x64 test environment |

Initial-delivery fat-JAR SHA-256 checksums (before the review follow-up, not a published release):

- macOS and Linux: `0174c6c607fc84c072791957be87cbbfc57b6d29c4ff17a1b45510dfb73e9b22`
- Windows: `c4bbd0e17e310edf6d9da7ee2babb4cd10122473a9bcf4dbf643eb417a00b17f`

Closing an isolated URLClassLoader can cause Hadoop's JVM shutdown hook to log
`NoClassDefFoundError: org/apache/hadoop/util/ShutdownHookManager$2`: it lazily
loads a helper after the loader has closed. The probe reports this separately;
it does not establish safe Hadoop loader unloading. Connections, complete reads,
and the second loader's native codecs succeed. No production workaround for this
separate Hadoop shutdown lifecycle behavior is introduced. An executable fat-JAR
CLI smoke check also listed all four acme tables and described salary's four
columns, one row group, and eight rows, with settings/history confined to a
temporary user home.

| Reported client | Version / bundled JVM | Outcome and disposition |
| --- | --- | --- |
| DbVisualizer | Not available | Untested GUI workflow; custom-driver recipe documented |
| DBeaver | Not available | Untested GUI workflow; generic-driver recipe documented |
| SQuirreL SQL | Not available | Untested GUI workflow; driver/alias recipe documented |
| SQL Workbench/J | Build 133 (2026-05-31), external JDK 21 | Headless batch integration passes on macOS ARM64 (21/25), Linux ARM64 (21), and Windows ARM64 (21); GUI workflow untested |

DbVisualizer, DBeaver, and SQuirreL are not installed in the test environment.
Their scanners, generated SQL, and bundled JVMs remain unverified. Workbench/J's
headless metadata commands and result handling are tested; its GUI scanner and
interactive workflow are still unverified. The demonstrated
packaging/TCCL failures are fixed; this does not prove the issue's original
zero-driver-class symptom is resolved in any particular GUI. Do not advertise
verified client support until the discover/connect/browse/query/close/reconnect
workflow is run with exact versions recorded. Windows ARM64 ran the packaged IT with `HADOOP_HOME` unset and no
`winutils.exe`; all codecs and replacement loaders passed. Windows x64 remains
untested. Windows JDK 21, Maven 3.9.9, and Workbench/J build 133 are permanently
installed as user tools; Java/Maven user environment variables and PATH work in
new SSH sessions.

## Optional SQL Workbench/J system integration

The official batch mode loads the fat JAR through `-driverJar`; the JParq JAR is
not placed on the client's application classpath. The optional
`sqlWorkbenchBatchClient` IT uses `-Djava.awt.headless=true`, isolated settings,
metadata commands (`WbListSchemas`, `WbList`, `DESCRIBE`), and queries over all
four fixture codecs. It opens two successive client processes and requires an
exact success-marker line and expected results. A separate deliberate missing-table
query must return nonzero without printing the success marker, guarding against
false success from the client harness. This verifies batch sessions, not GUI
scanning or closing/reconnecting inside an interactive window.

Install the client separately and opt in with
`-Djparq.it.sqlWorkbenchJar=/path/to/sqlworkbench.jar`; an absent installation is
a JUnit skip, while a specified invalid installation fails. There is no download
or third-party client dependency in the ordinary Maven build. Build 133 was
retrieved from `https://sql-workbench.eu/Workbench-Build133.zip` and checked
against its publisher SHA-1; the downloaded archive's SHA-256 is
`b3dcce4befd011f34f1ef7ab5eb6bb4eec02597fc8cc4220c4f164dc715a6706`.
See the [official batch documentation](https://sql-workbench.eu/manual/using-scripting.html).
DbVisualizer also offers `dbviscmd`, but its [CLI requires Pro](https://www.dbvis.com/docs/ug/command-line-interface/);
no license-dependent test was added.

Windows testing exposed a separate build portability problem: without Git
metadata, Spotless defaulted to native Windows endings and rejected every LF
source file. Java files now explicitly use LF in Spotless and `.gitattributes`;
no mass source reformatting or skipped formatting check is needed.

## Review follow-up

The completed [implementation plan](sql-client-compatibility-plan.md) is now in
this directory. The artifact asserts absence of `META-INF/INDEX.LIST` and all
`.SF`, `.RSA`, `.DSA`, and `.EC` signature files. The foreign-first subprocess now
fails if the cold lookup unexpectedly succeeds, so the test cannot lose its
cache-order precondition silently. The Workbench/J failure timeout waits up to
ten seconds after forced termination before reporting the timeout, matching the
ordinary probe cleanup path.

Review verification on 2026-10-19:

- macOS ARM64 / Java 21: full unskipped `mvn verify` with Workbench/J passed
  1,243 unit tests and 17 integration cases, with one expected skip in each group.
  Coverage, Checkstyle, PMD, Spotless, and Javadoc passed.
- Linux ARM64 / Java 21 and Windows ARM64 / Java 21: rebuilt artifacts and all
  14 packaged/client cases passed (the older-runtime rejection case was skipped).
  Windows retained `HADOOP_HOME` unset and no `winutils.exe`.

- macOS ARM64 / Java 25: all 14 packaged/client cases passed (rejection skipped).
  Java 17: rejection and both artifact checks passed; 11 functional/client cases
  were skipped.

Reviewed fat-JAR SHA-256 checksums (before adding JParq's own root `LICENSE`):

- macOS: `20526aae8cbe719ca50f463ec7de2daba6739dc74c4894823745942167d24cd8`
- Linux: `39c0fb36bcd64ab424a4846281f650a3c782e190897058fa6b2353a823dc589d`
- Windows: `f60dee2aa23d715bb628c954ccaaef0982142e57a6a6cd0b1d4c4507b2ab4195`

The root-license resource verification was followed by a Javadoc edit that
introduced a Spotless violation in commit `fa23d6b`; the reported pass did not
cover that final edit. The follow-up applies formatting and passes the project
license path explicitly through plugin-level Failsafe configuration instead of
deriving it from the fixture directory. After formatting, a fresh full
`mvn verify` with Workbench/J enabled passed on macOS ARM64 / Java 21:
1,243 unit tests and 17 integration cases, with one expected skip in each group.
Coverage, Checkstyle, PMD, Spotless, and Javadoc passed on the final Java sources.
The artifact test checks JParq's complete license
in both JARs and all dependency license/notice text in the combined file.
Linux, Windows, and the alternate runtimes were not rerun for this resource-only
follow-up; their results above apply to the earlier artifacts.
