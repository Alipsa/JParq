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