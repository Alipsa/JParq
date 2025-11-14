Please add support for querying INFORMATION_SCHEMA.TABLES to list available tables and their metadata.
it should contain the following columns:
"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEMA", "TYPE_NAME",
"SELF_REFERENCING_COL_NAME", "REF_GENERATION"

The JParqDatabaseMetaData.getTables already contains all these field (but with slightly different names), so you should use that to produce the information.