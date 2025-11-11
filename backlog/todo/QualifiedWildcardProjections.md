**Please add support for Qualified Wildcard Projections (e.g., `TableName.*` or `Alias.*`), aligning with the SQL Standard.**

Since the full implementation is too complex a task, some incremental steps has already been taken i.e:
- Task 1 — Add an AST node for qualified wildcard
- Task 2 — Teach the parser to recognize ident.*
You will now complete the implementation.
We will use the qualifiedWildcard branch for this work. Once the full feature is done and tested, it will be merged into main.

Qualified wildcard projection is a fundamental SQL feature that allows a user to select all columns belonging to a specific table, view, or alias without explicitly listing every column name. This is crucial for maintaining clarity, simplifying join queries, and insulating queries from future schema changes in other (unselected) tables.

-----

### Key Characteristics According to the SQL Standard

A qualified wildcard is a shorthand for all non-hidden, user-defined columns belonging to the specified table or alias.

* **Function:** Expands at query parsing/binding time to a complete, ordered list of columns belonging to the designated table or alias.
* **Placement:** Used in the `SELECT` list. It can be combined with other specific column selections (e.g., `SELECT t1.*, t2.specific_column`).
* **Ambiguity Prevention:** The primary benefit is preventing ambiguity in result sets when multiple tables share column names. The qualified form (`alias.column`) makes the source explicit, but the qualified wildcard (`alias.*`) provides a shorthand for all columns from that explicit source.

### Syntax

The standard syntax requires a table or alias identifier followed immediately by a period and the asterisk:

```sql
SELECT 
    t1.*,          -- Select all columns from Table1
    t2.column_C,   -- Select a specific column from Table2
    t3.* -- Select all columns from Table3
FROM 
    Table1 t1
JOIN 
    Table2 t2 ON t1.id = t2.t1_id
JOIN
    Table3 t3 ON t1.id = t3.t1_id;
```

-----

## Example using the `mtcars` Dataset

We will use a qualified wildcard projection to select all columns from the main `mtcars` table (`m`) while specifically selecting the maximum horsepower from the cylinder group totals (`t`).

**Goal:** List all detail columns for each car alongside the total horsepower for its cylinder group, demonstrating a selective projection in the `SELECT` list.

Assume we have a derived table `CylinderTotals` (`t`) that contains two columns: `cyl` and `MaxHP_in_Group`.

```sql
SELECT
    m.*,               -- Select all columns from the mtcars table (m)
    t.MaxHP_in_Group   -- Select only the MaxHP_in_Group from the derived table (t)
FROM
    mtcars m
JOIN 
    CylinderTotals t ON m.cyl = t.cyl;
```

### Expected Output (Conceptual, listing all original `mtcars` columns first, then the calculated value):

| model\_name    | mpg  | cyl | disp  | ... |  wt   | qsec  | **MaxHP\_in\_Group** |
|:---------------|:----:|:---:|:-----:|:---:|:-----:|:-----:|:--------------------:|
| Mazda RX4      | 21.0 |  6  | 160.0 | ... | 2.620 | 16.46 |         175          |
| Datsun 710     | 22.8 |  4  | 108.0 | ... | 2.320 | 18.61 |         109          |
| Ford Pantera L | 15.8 |  8  | 351.0 | ... | 3.170 | 14.50 |         335          |
| ...            | ...  | ... |  ...  | ... |  ...  |  ...  |         ...          |

-----

## Implementation Requirements

### 1\. Syntax Parsing and Binding

* The parser must recognize the **`Alias.*`** or **`TableName.*`** syntax in the `SELECT` list.
* The query binder **must correctly resolve and replace** the qualified wildcard with the complete list of *exposed* columns belonging to the referenced table or alias.
* The implementation must ensure the order of the expanded columns matches the definition order of the columns in the source table.

### 2\. Ambiguity and Errors

* The system must raise an error if the specified alias or table name is **not resolvable** in the current scope (i.e., not defined in the `FROM` clause).
* The output column names must retain the original names from the source table.

### 3\. Testing Requirements

* Create tests to verify the functionality.
* Tests must verify:
  * Simple `Alias.*` projection returns all columns from the aliased table.
  * Projection combines a qualified wildcard (`t1.*`) with specific columns from other tables (`t2.col`).
  * Projection ensures the correct column names and order are maintained after expansion.
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.

This feature is divided into 8 steps/tasks to ensure clarity and incremental progress.
- Task 1 — Add an AST node for qualified wildcard
- Task 2 — Teach the parser to recognize ident.*
- Task 3 — Binder: resolve the qualifier to a FROM source
- Task 4 — Define “exposed columns” and column order
- Task 5 — Keep column names and sources stable
- Task 6 — Error handling & messages
- Task 7 — Integration seam: projection building
- Task 8 — Unit tests: jparq.projection.QualifiedWildcardTest

Task 1 - 2 has already been completed. Please implement Task 3:

# Task 3 — Define “exposed columns” and column order

Goal: Ensure expansion respects visibility and order.

Changes

Identify how a relation advertises its output columns (base table schema, derived table projection list, CTE outputs, etc.).

Add/confirm a contract:

“Exposed columns” exclude hidden/system columns.

Order is the source definition order:

Base table: physical/declared column order in schema.

Derived/CTE/subquery: the order of its select list.

Implement getExposedColumns(Relation r) to return ordered, non-hidden columns only.

Tests

Join with a derived table CylinderTotals that exposes exactly [cyl, MaxHP_in_Group] (order preserved).

Accept if

QualifiedWildcard expansion mirrors the relation’s exposed, ordered columns exactly.

# Task 5 — Keep column names and sources stable

Goal: The projection should preserve the original column names and source qualifiers.

Changes

When materializing bound columns from t.*, set:

outputName = sourceColumn.name (do not auto-qualify in the output name),

Keep qualifier for internal lineage (used to disambiguate at runtime/planner).

Ensure downstream planning/execution doesn’t re-alias these unless user-specified.

Tests

After binding, check the output column names are exactly the base names (mpg, cyl, …), followed by MaxHP_in_Group when mixed with specific columns.

Accept if

Result set column labels match the source names for t.* columns.

# Task 6 — Error handling & messages

Goal: Friendly, precise errors.

Changes

On unknown qualifier: “Table or alias ‘X’ not found in FROM clause.”

On attempts like SELECT t1.*, t1.* (duplicates):

Either allow duplicates (common behavior) or, if your engine forbids exact duplicates, raise a duplication error that mentions the offending columns—match existing policy.

Tests

Unknown alias in SELECT list → binding error with the message above.

Accept if

Errors are deterministic and covered.

# Task 7 — Integration seam: projection building

Goal: Make sure the expansion flows through to the same pipeline as explicit columns.

Changes

Wherever a List<SelectItem> turns into a List<BoundColumn> or equivalent, ensure your “expand select list” step is applied before planning/execution.

If there is already an “unqualified * expander”, plug qualified expansion into the same mechanism (shared utility).

Tests

End-to-end query on a tiny in-memory catalog/table (or existing mtcars fixture) where execution returns rows with expected column count and order.

Accept if

No code paths skip expansion.

# Task 8 — Unit tests: jparq.projection.QualifiedWildcardTest

Goal: Add the required tests in one place, green on mvn -Dspotless.check.skip=true verify.

Where

src/test/java/jparq/projection/QualifiedWildcardTest.java

Tests to include

Simple alias wildcard

SELECT m.* FROM mtcars m

Assert the bound projection equals the full set of mtcars exposed columns, in order.

Join mix

SELECT m.*, t.MaxHP_in_Group FROM mtcars m JOIN CylinderTotals t ON m.cyl = t.cyl

Assert: all mtcars columns first (ordered), then MaxHP_in_Group last.

Order & names

Ensure output labels match original names.

Unknown alias error

SELECT x.* FROM mtcars m → binding error.

Hidden columns excluded (if applicable in your fixtures).

If you don’t have mtcars and CylinderTotals readily available in unit tests, create a minimal in-memory schema in the test setup:

Table mtcars(id, model_name, mpg, cyl, disp, wt, qsec, hp) (trimmed set is fine)

Derived/aux table CylinderTotals(cyl, MaxHP_in_Group) with dummy data or a mocked relation descriptor.

Accept if

All new tests pass locally without touching other tests.

Style tools happy (or at least pass with -Dspotless.check.skip=true).