Please add support for querying INFORMATION_SCHEMA.TABLES to list available tables and their metadata.
it should contain the following columns:
"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEMA", "TYPE_NAME",
"SELF_REFERENCING_COL_NAME", "REF_GENERATION"

The JParqDatabaseMetaData.getTables already contains all these field (but with slightly different names), so you should use that to produce the information.

# Important!
- Create test to verify the functionality in a test class called jparq.meta.InformationSchemaTest.java in the src/test/jparq/join directory.
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression. 
- No checkstyle, PMD or Spotless violations shall be present after the implementation.