Please add support for querying INFORMATION_SCHEMA.TABLES to list available tables and their metadata.

The SQL Standard (ISO/IEC 9075) explicitly defines the content and structure of the INFORMATION_SCHEMA.TABLES view. This view is a mandatory component of the INFORMATION_SCHEMA, which serves as the standard, vendor-neutral mechanism for accessing the Metadata (or Catalog) of a SQL database.

it should contain the following columns:
- "TABLE_CATALOG": The name of the catalog (database) containing the table.
- "TABLE_SCHEMA": The name of the schema containing the table.
- "TABLE_NAME": The name of the table, view, or temporary table.
- "TABLE_TYPE": A string indicating the kind of object (e.g., 'BASE TABLE', 'VIEW', or sometimes platform-specific types like 'SYSTEM VIEW').
- "REMARKS"
  - User-Defined Comments: This is its most common and intended use. It stores descriptive notes added by the user, developer, or database administrator to explain the purpose, constraints, or business rules associated with the object. For example:

  For a Table: "Stores all customer records for the North American region, last updated 2024-01-15."

  For a Column: "The internal primary key; not exposed to end-users."

The JParqDatabaseMetaData.getTables() already contains all these field (but with slightly different names), so you should use that to produce the information.

Remarks should be populated from the Parquet schema file-level metadata if available.
Parquet files utilize the file footer to store general metadata, which can be leveraged for documentation:

Key-Value Metadata: The standard way to store a file-level description is by writing a custom key-value entry into the Parquet file's metadata map.

Common Convention: Tools typically use a standardized key, such as comment, description, or parquet.schema.comment, to store the descriptive string that would equate to a database table's REMARKS.

Source Schema Inheritance: If the Parquet file is written based on an Avro or Thrift schema, the highest-level description or doc field in the source schema is often automatically transferred to the Parquet file's metadata to serve as the file description.

This mechanism ensures that the description travels with the file, and tools can read it without scanning the actual data. This is analogous to how database drivers populate the REMARKS column for INFORMATION_SCHEMA.TABLES—by looking for a specific metadata property.

# Important!
- Create test to verify the functionality in a test class called jparq.meta.InformationSchemaTablesTest.java
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression. 
- No checkstyle, PMD or Spotless violations shall be present after the implementation.