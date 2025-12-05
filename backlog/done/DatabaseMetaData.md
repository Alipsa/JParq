JParqDatabaseMetaData is currently a mix of verified and compliant behaviors and areas needing improvement to fully align with JDBC specifications. Some methods, while SQL compliant, are not accurately reflecting the actual implementation. Below are the key areas that require attention:

- Catalog JDBC spec expectations: list required return shapes (null vs empty ResultSet, allowed values) for DatabaseMetaData methods we use, focusing on sections covering table/type listings,
  capabilities, and read-only drivers.
- Map JParq capabilities: confirm supported SQL features, identifier handling, case sensitivity, table/schema layout from JParqConnection, JParqStatement, and existing README notes to know what
  the driver actually supports.
- Audit current implementations in src/main/java/se/alipsa/jparq/JParqDatabaseMetaData.java: flag methods returning null or placeholder booleans (e.g., getTableTypes, getTypeInfo, privilege/key/
  index methods, capability flags) where spec requires specific defaults or empty result sets; note any value mismatches (e.g., identifier casing, null sorting, transaction/reporting flags).
- Define required adjustments per method group: decide correct return values/result-set rows aligned with spec and driver limits (read-only, no transactions, no indexes/procs), including any
  metadata constants (product/driver version, keywords, functions) that need updates; outline Javadoc updates to meet documentation requirements.
- Plan implementation approach: update method bodies and Javadocs, prefer shared helpers (JParqUtil.listResultSet) to produce compliant empty/non-empty results; ensure American English wording
  and avoid deprecated APIs.
- Expand tests: add/adjust unit tests under src/test/java/jparq/meta (or similar) to assert revised behaviors for affected methods (table types, type info, capability flags, null/identifier
  handling); cover both case-sensitive and insensitive settings where relevant.
- Verification: run mvn -Dspotless.check.skip=true verify, and if lint blocks appear rerun with PMD/checkstyle skips to isolate issues; fix any style/PMD/Spotless findings to ensure a clean
  build.