## Requirements Document: SQL Standard Compliance Test Suite

**Please implement a comprehensive, data-driven test suite to validate SQL Standard compliance, isolating the tests from the unit test phase and running them during the integration phase (Failsafe).**

This requirement is split into two parts: defining the structure of the test data and creating the execution engine (the JUnit test class).

-----

## 1\. Data Structure Requirements (Test Suite)

The test suite must be organized hierarchically to facilitate modular testing and easy navigation.

### 1.1 Directory Structure

Create a dedicated directory for the test suite under the standard resource path:

`src/test/resources/standards/`

Within this root directory, the structure must be:

```
src/test/resources/standards/
├── Feature_Area_1/ (e.g., Pagination)
│   ├── Feature_Name_A/ (e.g., FetchOffset)
│   │   ├── test_case_1.sql
│   │   └── test_case_1.csv
│   └── Feature_Name_B/
│       ├── another_test.sql
│       └── another_test.csv
└── Feature_Area_2/ (e.g., TableValueFunctions)
    └── Feature_Name_C/
        ├── tvf_syntax.sql
        └── tvf_syntax.csv
```

The sql file should contain a header comment describing the test case, a reference to the sql standard that describes the feature, followed by the SQL `SELECT` statement. The corresponding csv file should contain the expected results.

### 1.2 Required Test Coverage Areas

The test suite must cover all relevant **read-only** aspects of the SQL standard to validate compliance effectively, drawing inspiration from industry standard suites:

* **`Pagination`**: Tests for `OFFSET`, `FETCH FIRST/NEXT`, and interaction with `ORDER BY`.
* **`TableValueFunctions`**: Tests for `UNNEST`, `LATERAL` syntax, and the `TABLE(...)` wrapper.
* **`Grouping`**: Tests for `CUBE`, `ROLLUP`, and complex `GROUPING SETS`, including the `GROUPING()` function.
* **`SetOperations`**: Tests for complex nesting, `INTERSECT ALL`, and `EXCEPT ALL`.
* **`Wildcards`**: Tests for qualified (`Alias.*`) and unqualified (`*`) projections.
* **`WindowFunctions`**: Comprehensive tests for analytic functions (e.g., `ROW_NUMBER`, ranking, aggregates) and various window frames.
* **`Joins`**: Complex join types, including non-equi-joins and multi-way joins.
* **`Subqueries`**: Tests for correlated and non-correlated subqueries in `SELECT`, `WHERE`, and `FROM` clauses.

### 1.3 File Specifications

* **SQL Files (`.sql`):** Must contain a single, complete, standard SQL `SELECT` statement to be executed against the test database.
* **CSV Files (`.csv`):** Must contain the exact, expected result set for the corresponding SQL file. The first row must contain the expected column names, and subsequent rows must contain the data. The order of both columns and rows must be strictly enforced.

-----

## 2\. Execution Engine Requirements (JUnit Class)

### 2.1 Test Class Definition

* Create a JUnit 5 test class, e.g., **`SqlStandardComplianceIT`**.
* This class must be placed in a structure that ensures it is executed during the Failsafe integration-test phase (e.g., naming convention of `*IT.java` if using standard Maven defaults).

### 2.2 Execution Logic

The test class must implement a single or looped test method with the following logic:

1.  **Discovery:** Recursively scan the `src/test/resources/standards/` directory to locate all `.sql` files.
2.  **Execution:** For each `.sql` file, execute the query against the test database using the `JParqSql` driver.
3.  **Result Loading:** Load the content of the corresponding `.csv` file into memory (e.g., as a list of strings or records).
4.  **Comparison:** Compare the actual result set (from the SQL execution) against the expected result set (from the CSV file).
  * **Strict Comparison:** The comparison must be **strict**, validating:
    * The number of columns.
    * The names and order of the columns.
    * The number of rows.
    * The content of every cell (including handling `NULL` values and data type conversions).
5.  **Reporting:** The test must fail immediately upon the first discrepancy and provide a clear report detailing:
  * The path to the failing SQL file.
  * The discrepancy (e.g., "Expected 10 rows, got 8," or "Value in column 'X' was 'A', expected 'B'.").

### 2.3 Maven Configuration

* Ensure the test class naming convention is configured to be picked up by the **Failsafe Plugin** during the **`integration-test`** phase and *excluded* from the Surefire plugin's **`test`** phase.