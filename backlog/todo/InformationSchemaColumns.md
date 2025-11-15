Please add support for querying INFORMATION_SCHEMA.COLUMNS to list available columns and their metadata.

The SQL Standard (ISO/IEC 9075) explicitly defines the content and structure of the **`INFORMATION_SCHEMA.COLUMNS`** view. This view is mandatory for any SQL implementation that claims standard compliance, as it provides the official, vendor-neutral way to access metadata about the columns of all tables and views accessible to the current user.

---

## 🏛️ Standard Mandates for `INFORMATION_SCHEMA.COLUMNS`

The primary purpose of this view is to allow applications to introspect the database schema (or **Catalog**) to determine the structure of a relation (table or view).

The standard requires the view to contain columns that precisely describe the following aspects of every column:

### 1. Column Identification (Location)

These columns specify exactly which table and schema the column belongs to:

* **`TABLE_CATALOG`**: The name of the catalog (database) that contains the table.
* **`TABLE_SCHEMA`**: The name of the schema that contains the table.
* **`TABLE_NAME`**: The name of the table or view that contains the column.
* **`COLUMN_NAME`**: The name of the column itself.

### 2. Data Type and Characteristics

These columns define the nature and constraints of the data stored in the column:

* **`DATA_TYPE`**: The name of the SQL data type (e.g., `CHARACTER VARYING`, `INTEGER`, `DATE`).
* **`CHARACTER_MAXIMUM_LENGTH`**: The maximum length of the column in characters (for character types).
* **`NUMERIC_PRECISION`**: The precision for numeric data types (number of significant digits).
* **`NUMERIC_SCALE`**: The scale for numeric data types (number of digits to the right of the decimal point).
* **`DATETIME_PRECISION`**: The precision for datetime data types (e.g., number of fractional seconds).

### 3. Constraint and Ordering

These columns specify the nullability, default value, and ordering of the column:

* **`IS_NULLABLE`**: Indicates whether the column permits null values (`YES` or `NO`).
* **`COLUMN_DEFAULT`**: The default value assigned to the column if no value is explicitly supplied on insert.
* **`ORDINAL_POSITION`**: The sequential position of the column within its table, starting from 1.

Note: The JParqDatabaseMetaData.getTables() already contains most of these field (but with slightly different names).

# Important!
- Create test to verify the functionality in a test class called jparq.meta.InformationSchemaColumnsTest.java.
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression. 
- No checkstyle, PMD or Spotless violations shall be present after the implementation.