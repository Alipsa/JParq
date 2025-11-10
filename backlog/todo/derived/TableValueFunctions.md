**Please add support for Table-Valued Functions (TVFs) as used in the `FROM` clause, aligning with the SQL:1999/2003 standard.**

A Table-Valued Function (TVF), often referenced in the standard as a **Table Function**, is a routine that returns a result set (a virtual table) and must be callable within the `FROM` clause. This feature enables complex logic, data generation, or iteration to be integrated directly into a standard SQL query structure.

-----

## Key Characteristics According to the SQL Standard

A TVF allows parameters to be passed to procedural code that generates a dynamic set of rows. It is the logical successor to the non-standard `FROM`-clause subquery for dynamic, parameterized data generation.

* **Placement:** The function call must occur in the `FROM` clause, requiring an alias and column list for its output.
* **Correlated Usage:** The function must support being executed once per row of a preceding table (i.e., acting as a **Lateral Derived Table**), usually achieved via the standard **`LATERAL`** keyword (or an implicit cross join when no correlation exists).

### Syntax (General Standard Form)

```sql
SELECT
    t1.column_a,
    t2.output_column
FROM
    Table1 t1
[JOIN | CROSS JOIN | LEFT JOIN]
    TABLE(MyTableFunction(t1.InputParameter)) AS t2 (output_column)
ON ...;
```

### Mandatory `UNNEST` Support

Since arrays/repeated records are a core part of modern data formats, the **`UNNEST`** function (which is a standard Table Function) must be implemented to handle simple and complex array flattening.

* **UNNEST Syntax:**
  ```sql
  FROM 
      TableName t, 
      UNNEST(t.array_column) AS u (item_column)
  ```
  This is the read-only, standard way to work with repeated data.

-----

## Example using the `mtcars` Dataset: Simulating a TVF with `UNNEST`

Since a read-only JDBC driver can't create procedural functions, the core requirement is to correctly handle the standard syntax of a table function call in the `FROM` clause, using **`UNNEST`** as the primary test case.

**Goal:** Simulate a calculation of gear ranges by flattening an imaginary array of allowed gears per cylinder count.

Assume `mtcars` is conceptually augmented with an array column, `GearRanges`, which is an array of integers representing specific gear configurations (e.g., `[3, 4, 5]`).

```sql
SELECT
    m.cyl,
    m.hp,
    g.Allowed_Gear
FROM
    mtcars m
-- UNNEST (a standard Table Function) is executed for each row of mtcars.
-- This requires implicit or explicit LATERAL functionality.
JOIN 
    UNNEST(m.GearRanges) AS g(Allowed_Gear) ON TRUE
WHERE
    g.Allowed_Gear > 4
ORDER BY
    m.cyl, g.Allowed_Gear;
```

*(Note: The actual implementation must allow any user-defined TVF, but `UNNEST` serves as the crucial test of the structural requirement.)*

### Expected Output (Conceptual, assuming `GearRanges = [3, 4, 5]` for all rows):

| cyl | hp  | Allowed\_Gear |
|:---:|:---:|:-------------:|
|  4  | 93  |       5       |
|  4  | 109 |       5       |
|  6  | 110 |       5       |
| ... | ... |      ...      |
|  8  | 335 |       5       |

-----

## 📝 Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize a function call within the `FROM` clause where a table name or derived table is expected.
* The parser must be able to handle the output aliasing and column assignment (e.g., `AS alias(col1, col2)`).
* The implementation must support the standard **`UNNEST(array_column)`** syntax, correctly flattening the source array data.

### 2\. Correlated Execution

* The driver **must correctly implement the execution logic equivalent to a `LATERAL` join** when a TVF (like `UNNEST`) references a column from a table preceding it in the `FROM` clause. The function must be executed once for every row of the primary table.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.derived.TableValueFunctionTest`**.
* Tests must verify:
  * Correct parsing and execution of the `UNNEST` function on simple array columns (as demonstrated in previous test data).
  * Correct execution when the TVF is correlated to the primary table.
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.