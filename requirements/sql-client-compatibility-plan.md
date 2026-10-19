# SQL client compatibility: issue #179

Status: packaging and class-loading fixes implemented. See the
[verification record](done/sql-client-compatibility.md) for tested configurations
and outstanding GUI/platform checks.

## Evidence and scope

This section preserves pre-fix inspection and review evidence. See the linked
verification record for implementation results and the retained Java 21 minimum.

[Issue #179](https://github.com/Alipsa/JParq/issues/179) reports failures in SQuirreL,
SQL Workbench/J, and DBeaver without a description or reproduction steps. The two
comments add DbVisualizer: scanning the ordinary JAR finds zero driver classes,
and using the fat JAR reportedly does not help. Client versions, JVM versions,
JParq versions, and underlying class-loading exceptions are not provided.

The current source has a public `se.alipsa.jparq.JParqDriver` implementing
`java.sql.Driver`, a public no-argument constructor, and
`src/main/resources/META-INF/services/java.sql.Driver`. Adding a missing JDBC
service registration is therefore not an evidence-based fix.

Inspection of the existing `target/jparq-1.3.0.jar` and fat JAR found the driver
class and correct JDBC service entry in both. The driver class has bytecode major
version 65 (Java 21). The Maven enforcer range is `[21,23)` and both manifests
specify `Max-Jdk-Version: 21`. The README previously rejected anything above
Java 21 "due to Hadoop restrictions"; that wording was added in November 2025,
when JParq used Hadoop 3.4.2. It now states the policy below. Check the JVM
actually running each client, including bundled runtimes.

Java policy for this issue, based on the
[Hadoop 3.5.0 release overview](https://hadoop.apache.org/docs/r3.5.0/index.html)
("On the client side, Java 17 and Java 21 are supported"):

- **Supported:** Java 21, matching Hadoop's documented client support.
- **Tested, best effort:** Java 25. Hadoop 3.5.0 ships JDK 25 fixes
  (HADOOP-19212 UserGroupInformation `Subject` replacement APIs, HADOOP-19821
  token initialization `NoClassDefFoundError`, HADOOP-19653 ByteBuddy/ASM for
  Java 25 bytecode) but does not declare Java 25 support, so JParq does not
  either.
- **Candidate minimum:** Java 17, which Hadoop supports for clients. Only
  a sampled dependency bytecode check has been performed: no dependency blocker
  was found in that sample. JParq's own code has not been checked for Java 21-only
  features.

A separate defect was reproduced on Java 21 using the existing fat JAR in an
isolated `URLClassLoader` with the platform class loader as parent and the driver
loader as thread context class loader:

1. Explicit driver loading and public construction succeeded.
2. `ServiceLoader` found `se.alipsa.jparq.JParqDriver`.
3. Connecting to the existing `acme` fixture succeeded.
4. `DatabaseMetaData.getTables` failed with `SQLException`, caused by
   `UnsupportedFileSystemException: No FileSystem for scheme "file"`.

The fat JAR contains `org.apache.hadoop.fs.LocalFileSystem.class`, but its
`META-INF/services/org.apache.hadoop.fs.FileSystem` contains only HDFS providers.
The Hadoop Common dependency's descriptor includes `LocalFileSystem` and other
providers. `src/assembly/fat.xml` unpacks dependencies without service-file
merging. This is a demonstrated packaging failure, but does not by itself explain
the earlier zero-driver-class report. These checks used existing build artifacts;
a clean build must confirm the baseline before implementation.

The review probes distinguish a second defect: Hadoop filesystem
provider discovery depends on the thread context class loader (TCCL). With the
thin JAR and resolved dependencies, a driver-loader TCCL produced four tables and
`COUNT(*) = 8`; a platform-loader TCCL failed with the same filesystem exception.
The existing fat JAR failed with either TCCL. Preloading filesystem providers with
the driver-loader TCCL, then switching to the platform loader, succeeded with
the thin JAR and dependencies. Hadoop's static provider cache makes first-load
order significant. The initial analysis's driver-loader TCCL hid this independent
failure. Actual GUI client TCCL behavior remains unverified.

Review probes also verified that Java 17 rejects the current driver with
`UnsupportedClassVersionError`, which fits, but does not establish, the cause of
DbVisualizer's zero-class result. On Java 25.0.4 (macOS ARM64), the full
`mvn verify` suite passed in a clean copy of HEAD with only the enforcer skipped:
1240 unit tests (1 skipped) and 3 integration tests, plus Checkstyle, PMD,
Spotless, and Javadoc. `UserGroupInformation.getCurrentUser()` and `doAs` also
worked on Java 25 with both Hadoop 3.5.0 and 3.4.3. JParq exercises only Hadoop's
local-filesystem read path, so Kerberos, HDFS, and delegation tokens remain
untested; this supports best-effort status, not a support claim. Java 25 also
warns about `sun.misc.Unsafe::invokeCleaner` (parquet-hadoop `CleanUtil`) and
restricted native access (Hadoop `NativeCodeLoader`), which are risks for later
JDKs. Linux and Windows have not been run on Java 25. Review inspection
also found a root `module-info.class` identifying the fat JAR as
`net.sf.jsqlparser@5.4`, and no `Multi-Release: true` manifest attribute despite
versioned dependency classes. These review findings must be reproduced during
implementation.

Further review probes found that explicit `fs.file.impl` configuration together
with `Configuration.setClassLoader(jparqLoader)` resolves `FileSystem.get` under
a foreign TCCL; neither setting alone suffices. In a throwaway copy, review
probes then replaced every `new Configuration(false)` with the factory and
verified `getTables` and every value from `SELECT *` over `acme/salary.parquet`
(uncompressed), `mtcars.parquet` (ZSTD), and `hf/optimized.parquet` (Snappy).
This used the thin JAR plus resolved dependencies, Java 21, macOS ARM64, and a
platform TCCL, with no preload or TCCL swap. This is the planned fix because it
avoids depending on which TCCL first populates Hadoop's static provider cache.

With filesystem preloading, thin JAR plus dependencies, Java 21, and macOS ARM64,
review probes verified every value in the ZSTD `mtcars.parquet` fixture (32 rows,
12 columns) and Snappy `hf/optimized.parquet` fixture (16,384 rows, 13 columns)
under a platform TCCL. The factory probes above also verified these reads without
preloading. Remaining gaps are the fat JAR, GZIP, Linux and Windows, and replacement
driver loaders. The `acme` files are uncompressed and provide no codec coverage;
a GZIP fixture must be added. Repeat these review probes against the implementation.

## 1. Establish reproducible baselines

- Build on Java 21 and record the result of `mvn clean verify`.
- Check platform readiness before scheduling compatibility runs. Review checks
  found no `ubuntu` Linux container and a stopped Windows VM; installed JDKs and
  Maven in Windows are unverified. Check the local environment instructions and
  obtain any required authorization before creating environments or installing
  packages. Do not assume either platform is ready. If unavailable, record the
  platform/runtime/architecture as `untested` with the reason in the compatibility
  matrix; continue available checks without silently blocking completion or
  claiming platform support from another platform's results.
- Repeat the isolated fat-JAR probe against the newly built artifact, preserving
  the full cause chain and service descriptor contents.
- Repeat the thin-plus-dependencies and fat-JAR TCCL matrix in separate fresh
  JVMs, before any Hadoop provider initialization. Include both the driver loader
  and a foreign loader (platform and a host application loader without driver
  dependencies). Preserve the observed first-load order in failure diagnostics.
- Run a quick Java 17 explicit-load probe to confirm the bytecode rejection.
  Check each client's actual runtime before attributing a scanner failure to it.
- Make an explicit minimum-runtime decision: investigate compiling with
  `--release 17`, focusing first on JParq's own language/API use, or retain Java 21
  as the minimum. The review sample of the first 200 non-versioned classes per
  dependency JAR found Hadoop 3.5.0 at class version 61 and Parquet 1.18.1 at 55;
  this is encouraging but not a full dependency audit. Validate the resolved
  runtime graph and functionality on 17 before claiming support.
- Run required compatibility tests on Java 21 and record the actual JVM shipped
  or used by each client. Build on Java 21 and run the packaged-artifact probes
  on Java 25 as the best-effort tier using the separate subprocess runtime in
  section 3, without skipping the Maven enforcer. Run on macOS and on Linux and
  Windows when available under the readiness prerequisite above; unavailable
  combinations are `untested`. Java 25 failures are recorded limitations, not
  release blockers. Prefer running a client on Java 21 when it
  allows a runtime choice. A client bundling Java 25 is a best-effort
  configuration; one bundling another newer JVM is untested and must be recorded
  as such. Test 17 if chosen as the minimum. Separate build JDK constraints from
  client runtime support and record tested versions explicitly.
- Record exact client/JParq/JVM versions and artifact checksums for each GUI
  reproduction. Separate driver discovery, driver initialization, connection,
  metadata browsing, and query execution failures.
- Start with DbVisualizer because its comments give the most concrete symptom;
  also exercise SQuirreL, SQL Workbench/J, and DBeaver using custom/generic JDBC
  configuration, starting with Java 21. Record a client's runtime limitation
  explicitly if it cannot run the supported JParq configuration.
- Obtain original reporter versions and full exceptions when available; this is
  useful corroboration and need not block the independently reproduced fix.

## 2. Correct fat-JAR packaging and driver class loading

- Add the Maven Assembly `metaInf-services` container descriptor handler to
  `src/assembly/fat.xml`. This is supported by the existing Assembly plugin;
  changing packaging plugins or relocating dependencies is not initially needed.
  See the [official Assembly documentation](https://maven.apache.org/plugins/maven-assembly-plugin/examples/single/using-container-descriptor-handlers.html).
- Preserve the `fat` classifier, executable CLI entry point, and ordinary JAR.
- Verify that the assembled JDBC provider survives and that the Hadoop filesystem
  descriptor includes the providers from the input dependencies, especially
  `org.apache.hadoop.fs.LocalFileSystem`.
- Check other merged service descriptors for missing provider classes or service
  loading errors. Set `fs.file.impl` only in the factory; keep the descriptor
  merge and explicit descriptor assertions regardless, since behavioral tests
  using the factory will not detect a broken merge for the `file` scheme.
- Exclude root `module-info.class` and
  `META-INF/versions/*/module-info.class` from unpacked dependencies so the fat
  JAR does not inherit a dependency's module identity. Assert both exclusions.
- Compare builds with and without `Multi-Release: true` in the assembly manifest.
  Prefer preserving dependency multi-release behavior by adding the attribute;
  inspect versioned entries and test supported runtimes for activation and linkage
  conflicts. If it is omitted, document the reason and the tested consequences.
- Centralize Hadoop configuration creation in a documented factory. Set both
  `fs.file.impl` to `org.apache.hadoop.fs.LocalFileSystem` and the configuration's
  class loader to JParq's defining loader. Preserve existing caller options and
  configuration-copy behavior. Route all production creation sites through this
  factory, including metadata, queries, nested reads, and the CLI. Audit all 13
  production construction sites: nine `new Configuration(false)` calls and four
  copies (`JParqPreparedStatement:1416`, `ParquetTable:94/137/179` at review time).
  Audit their callers too: copies preserve their input settings, so caller-supplied
  configurations reaching `ParquetTable` must also receive both factory settings
  before use. Preserve caller options without mutating caller-owned configurations.
  This single factory supplies explicit local filesystem behavior without
  scattered assignments or a provider-preload dependency.
- Do not initialize Hadoop in `JParqDriver`'s static initializer or constructor.
  DriverManager can swallow service-provider initialization failures and stop
  scanning subsequent providers; a failing class initializer also poisons later
  use of that class. Merely discovering the driver must not start Hadoop.
- The factory requires no one-time setup. Only if implementation reveals a
  necessary one-time step, perform it lazily on the first accepted `connect()`,
  once per defining loader, using a holder idiom. Keep fallible work
  out of throwing class initializers: have the holder retain an initialization
  outcome, including an expected failure, and translate it into `SQLException`
  with its original cause at the connection boundary. Do not catch fatal VM
  errors. In that case, cover direct public `JParqConnection` construction through
  the same initialization path. Do not add a holder or setup layer solely for the
  configuration factory. Unknown JDBC URLs must still return `null` without setup.
- Verify no TCCL change is needed for the configuration-based fix. If a demonstrated
  dependency path still requires a temporary change, limit it to that operation
  and restore it in `finally`, on success and failure.
- Treat first-load order and concurrency as safety checks: initialize JParq's own
  Hadoop under a foreign TCCL before the first connection, then verify the
  factory-based read path still works. A separate host Hadoop loaded by another
  loader has a different static cache and is not a valid substitute for this test.
  Also cover foreign-TCCL-first driver use and concurrent initial connections.
- Test Snappy, ZSTD, and GZIP through metadata and complete query reads with this
  factory under a foreign TCCL. Reuse the verified Snappy/ZSTD fixtures and create
  a small deterministic GZIP Parquet fixture with known expected values; a
  committed PyArrow-generated fixture with regeneration instructions is acceptable
  and must not require Python during Maven builds.

## 3. Add tests against the distributed artifact

- Add `src/test/java/jparq/JParqPackagedDriverIT.java`, executed by the existing
  Failsafe `*IT` configuration after the package phase. Pass the artifact path
  through Maven configuration rather than hard-coding the project version.
- Put a JDK-only probe in `src/test/java/jparq/probe/PackagedDriverProbe.java` so
  normal test compilation, Spotless, and Checkstyle cover it. PMD currently does
  not include test sources; this plan does not change that setting. Copy only its
  compiled class and any explicitly required nested probe classes into a dedicated
  probe output directory for subprocess execution. Never put all of
  `target/test-classes`, `target/classes`, or Maven's dependency classpath on the
  fat-JAR probe's classpath. Failsafe supplies a temporary database directory.
  Add `-Djparq.it.javaHome=<jdk>` for selecting only the probe subprocess runtime;
  define `<jparq.it.javaHome>${java.home}</jparq.it.javaHome>` in `pom.xml`'s
  `<properties>` and forward it through Failsafe. A command-line
  `-Djparq.it.javaHome=...` overrides that default. Resolve
  its `bin/java` (`java.exe` on Windows), validate the executable, and record its
  version. Read `JAVA_VERSION` from `<jdk>/release` to detect the feature version
  without another process; fall back to launching the selected Java executable
  if that file is missing. Reject a selected runtime older than Java 17 as an
  invalid `jparq.it.javaHome` setting before launching any probe. Maven, test
  compilation, and the IT orchestrator stay on Java 21.
  Build once with `mvn clean verify`, then rerun the packaged IT using
  `mvn -Dit.test=JParqPackagedDriverIT -Djparq.it.javaHome=<jdk> failsafe:integration-test failsafe:verify`.
  This tests the same artifact on Java 25 without `-Denforcer.skip=true` or an
  enforcer policy change.
  Keep the forwarded runtime home, artifact path, compiler release, and absolute
  probe source path in plugin-level Failsafe `systemPropertyVariables`, not only
  inside an execution, so direct goal invocation receives them; the IT must fail
  clearly if any required property is null or blank rather than silently
  substituting defaults. Undefined Maven expressions can arrive as empty strings;
  validate these values before resolving paths, including the runtime's `bin/java`.
- Select expectations using the probe runtime's detected feature version and
  the driver's compiler release, passed from `${maven.compiler.release}` through
  Failsafe rather than hard-coded. At or above that release (currently 21), run
  the full matrix and require every executed assertion to succeed, including on
  Java 25. Best-effort status affects the release decision, not test assertions:
  record Java 25 failures rather than turning them into passes. For supported
  probe runtimes from Java 17 up to, but below, the compiler release, run only
  explicit driver rejection and assert
  `UnsupportedClassVersionError` identifying `se.alipsa.jparq.JParqDriver`.
  Use JUnit assumptions to report each incompatible functional scenario as
  skipped, not passed. The same rerun command above therefore selects the
  rejection-only behavior for JDK 17 automatically. If the compiler release is
  lowered to 17, JDK 17 automatically enters the full functional matrix.
- For the below-release rejection check, launch the probe in single-file source
  mode: `<jdk>/bin/java <source-path>/PackagedDriverProbe.java <scenario> ...`.
  Pass its absolute source path through Failsafe using `${project.basedir}`;
  do not launch its release-21 `.class` output on Java 17. Keep probe source and
  APIs compatible with Java 17, using only Java 17 JDK APIs (`java.base`, `java.sql`, and
  `java.logging` for the JDBC provider logger). Normal test compilation and formatting still cover the source,
  but release-21 compilation does not establish Java 17 API compatibility; the
  actual Java 17 source-mode run checks that. A probe compilation/startup failure
  must fail the rejection test, not count as the expected driver rejection.
  Have the probe print a fixed started marker as its first action, before driver
  loading; its absence is a probe startup failure, even if captured diagnostics
  contain `UnsupportedClassVersionError`.
- Run each scenario in its own fresh JVM: automatic `DriverManager` discovery
  without explicit loading; explicit driver loading/instantiation; an isolated
  driver loader with driver TCCL; and an isolated driver loader with foreign
  platform or host application TCCL. For isolated scenarios, the fat JAR belongs
  only to the child loader, not the application classpath. Use direct
  `Driver.connect` there to avoid DriverManager caller-loader filtering.
- Run the first-use, cache-order, and concurrency checks required by section 2
  in separate fresh subprocesses. Do not accidentally warm
  provider caches in setup. Ordinary in-process unit tests cannot establish
  first-load behavior of a static cache.
- Check that scanning/instantiating the provider does not initialize Hadoop or
  prevent another test JDBC provider from being discovered. This is regression
  coverage for behavior already confirmed by a review discovery probe. Run the
  subprocess with `-Xlog:class+load:file=<temporary-log>`, discover the driver through `ServiceLoader`
  in an isolated loader, then call `acceptsURL` and `getPropertyInfo` without
  connecting. Keep that log separate from stdout, stderr, and failure diagnostics.
  Parse its class-load records and assert no class name starts with
  `org.apache.hadoop.` or `org.apache.parquet.`; match class names rather than
  timestamps, loader IDs, or exact log formatting. Only if a necessary
  one-time initialization step is introduced, test that its failures surface from
  `connect()` as `SQLException` with a cause without poisoning the driver class.
  Do not add production fault-injection hooks for setup the factory does not need.
- Assert successful connection, `isValid`, schema/table/column discovery, and a
  bounded query over a temporary copy of the existing `acme` fixture. Check exact
  expected metadata and row values, including result exhaustion and closure.
- Assert the packaged JDBC and Hadoop filesystem provider entries. Bound subprocess
  execution time, capture diagnostic output, and restore context class loaders.
- Check that the caller's TCCL is preserved after successful and failing driver
  operations. Exercise compressed fixtures through both metadata and queries
  in fresh foreign-TCCL processes; assert values, not merely successful loading.
- Within a dedicated subprocess, read Snappy and ZSTD through one isolated driver
  loader, close its resources, then create a second fresh driver loader in the
  same JVM and repeat all reads. This models client driver editing/reloading and
  detects native-library ownership failures such as `UnsatisfiedLinkError`.
  Loader replacement intentionally shares a process; other first-use scenarios
  remain separate. Run native codec coverage on available macOS, Linux, and
  Windows environments under the section 1 prerequisite; record unavailable
  combinations as `untested`. Investigate failures before claiming compatibility.
- Demonstrate the failure before fixes, the remaining foreign-TCCL failure after
  service merging alone, and success after the class-loading fix. Keep thin-JAR
  plus resolved-dependencies scenarios as independent controls. Verify both module
  descriptor exclusions and the selected multi-release behavior in the artifact.

## 4. Resolve remaining client-specific failures from traces

- After fixing packaging and class loading, repeat each client's full workflow: discover the driver,
  connect, browse schemas/tables/columns, open table data, run a bounded query,
  close, and reconnect.
- If discovery still fails, capture `UnsupportedClassVersionError`, linkage errors,
  missing dependencies, and scanner exceptions before changing JDBC behavior.
- If browsing or queries fail, reproduce the exact JDBC call sequence in a focused
  test before changing `JParqConnection`, `JParqDatabaseMetaData`, statements, or
  result sets. Existing code already implements many empty metadata result sets;
  a broad metadata rewrite is not justified by the issue evidence.
- Investigate quoted names and metadata capability reporting if generated client
  SQL exposes a mismatch. Test mixed-case and schema-qualified identifiers.
- Preserve read-only, forward-only semantics. Use client settings where supported;
  introduce additional JDBC behavior only when a reproduced requirement warrants
  it, with tests and complete Javadocs. Do not add general SQL dialect extensions
  merely because a client might send validation SQL such as `SELECT 1`.

## 5. Document setup and validate completion

- Add a README SQL-client section with the fat JAR, exact driver class,
  `jdbc:jparq:/absolute/path` URL, the tested minimum and supported runtime range,
  and tested setup steps for all four clients. Explain that the ordinary JAR
  requires its runtime dependencies. Record actual tested versions and limits.
  Consult the official [DBeaver driver manager](https://dbeaver.com/docs/dbeaver/Driver-Manager/)
  and [DbVisualizer driver installation](https://www.dbvis.com/docs/ug/reference-material/installing-a-jdbc-driver/)
  instructions when preparing client-specific steps.
- Keep the README Requirements entry consistent with the Java policy above, and
  update it if the minimum changes or Java 25 testing finds limitations. Keep
  `Max-Jdk-Version` at the supported version (21); best-effort versions do not
  raise it. The Maven enforcer remains a build-only constraint; Java 25 probes
  use `jparq.it.javaHome` while Maven builds on Java 21. Do not expand the enforcer
  range or skip it merely to execute runtime probes. Keep the README's existing
  macOS ARM64 qualification for the full-suite Java 25 result; add other platforms only after runs
  establish those results. Any
  decision to lower the minimum must also update the compiler release, dependency
  bytecode checks, README, and manifests. Moving Java 25 from best effort to
  supported requires Hadoop to declare that support.
- Add a release note covering the demonstrated packaging fix and verified client
  support. Do not claim the original discovery symptom is fixed without evidence.
- Run `mvn clean verify` with all tests and Checkstyle, PMD, Spotless, coverage,
  and Javadoc checks enabled. If verification fails, also run
  `mvn -Dspotless.check.skip=true -Dpmd.skip=true -Dcheckstyle.skip=true verify`
  to distinguish test failures from tool failures, then resolve all violations
  and finish with an unskipped successful build.
- Test Windows and Linux when ready under the section 1 prerequisite;
  include platform path handling and record the runtime, client version, and CPU
  architecture. Explicitly run the Windows packaged IT with `HADOOP_HOME` unset
  and no `winutils.exe` available; GUI users must not need an undocumented Hadoop
  installation. Investigate any failure before claiming this setup works.
  The configured local environments target Windows ARM and Linux ARM64 but are
  not yet ready as recorded in section 1. Label any eventual results as ARM-only.
  They do not establish x64 compatibility, which needs
  separate x64 testing or an explicit untested entry in the matrix.

Completion requires a clean verified build, packaged-artifact regression coverage
with driver and foreign TCCLs in fresh JVMs, verified compressed reads, a consistent
Java compatibility policy backed by runtime tests, safe discovery,
native codec reads through replacement loaders, and a recorded outcome
for every reported client. Supported configurations must
discover/load the driver, connect, browse metadata, and execute a query; any
remaining client/runtime limitation must have a documented cause and disposition.
Unavailable platform environments must be explicitly marked `untested` with
their blockers; those entries do not silently block completion of available
verification and do not count as compatibility evidence.

## Suggested delivery order

1. Reproduce the fresh-artifact and TCCL matrix, add failing packaged-JAR tests,
   and fix service aggregation, module descriptors, multi-release behavior, and
   Hadoop class loading, including compressed reads.
2. Validate supported Java 21 client configurations, run the Java 25 best-effort
   checks, and make the minimum-runtime decision, then reproduce and fix remaining
   GUI client failures with focused tests.
3. Finish the client compatibility matrix, setup documentation, release notes,
   and complete verification.

The evidence above preserves the initial investigation and review findings.
Implementation verification, minimum-runtime decisions, and remaining limitations
are recorded separately in the linked verification record. The initial temporary
probe and throwaway review patches are not the production fix.
