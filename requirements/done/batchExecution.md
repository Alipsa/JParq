add batch execution support for prepared statements while staying within the read-only design and project standards.
□ Inspect current prepared-statement and statement batching hooks (no-op parameter setters, unsupported batch methods) and map required behavioral changes for a read-only driver.
□ Design batch semantics: how parameter sets are captured/applied per execution, how results/update counts are surfaced, and how to handle errors/clearing to match JDBC expectations.
□ Implement batching support in JParqPreparedStatement (and any shared statement helpers) with necessary state, parameter handling, resource management, and javadoc updates.
□ Add/adjust tests in the jparq package to cover addBatch/clearBatch/executeBatch flows, success and failure cases, and update existing expectations.
□ Run formatting/static analysis/tests (mvn verify) and address any checkstyle/PMD/spotless issues.