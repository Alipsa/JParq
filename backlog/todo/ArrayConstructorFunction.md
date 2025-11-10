**Please add support for the `ARRAY` constructor function, aligning with the SQL:1999/2003 standard for Collection Value Constructors.**

The `ARRAY` constructor function is a standard SQL feature used to explicitly create a single, ordered collection value (an array) from a list of scalar expressions or from the result of a subquery. Its primary purpose is to **nest** or group multiple values into a single column, enabling the storage and manipulation of complex data types.

-----

### Key Characteristics According to the SQL Standard

The `ARRAY` function returns a **single scalar value** of type `ARRAY`. This value can be used in the `SELECT` list, `WHERE` clause, or any expression where a scalar value is permitted.

* **Function:** Takes multiple individual values (scalar expressions) and bundles them into one column value (the array).
* **Homogeneous Type:** All elements provided to the constructor must be of the same data type or mutually castable to a common type.
* **Placement:** It is typically used in the `SELECT` list or within an `INSERT` statement's `VALUES` clause.

### Syntax

The standard defines two main forms:

1.  **From a List of Expressions (Literal Construction):**

    ```sql
    ARRAY[expression_1, expression_2, ...]
    ```

2.  **From a Subquery Result (Query Construction):**

    ```sql
    ARRAY(SELECT column_name FROM AnotherTable WHERE condition)
    ```

    This form gathers all values from the single column of the subquery result into a single array value.

-----

## Example using the `mtcars` Dataset

We will use the `ARRAY` constructor to group related characteristics (`mpg`, `hp`, and `qsec`) into a single, nested column for each car.

**Goal:** Create a single array containing the performance metrics (MPG, HP, QSEC) for each car, demonstrating the constructor's use in the `SELECT` list.

```sql
SELECT
    cyl,
    -- Construct a new array containing three performance metrics
    ARRAY[CAST(mpg AS DOUBLE), CAST(hp AS DOUBLE), qsec] AS Performance_Metrics
FROM
    mtcars
WHERE
    cyl = 4
ORDER BY
    mpg DESC;
```

*(Note: Casting to `DOUBLE` is often necessary to ensure a homogeneous type for all array elements.)*

### Expected Output (Conceptual, for 4-cylinder cars):

| cyl | Performance\_Metrics (ARRAY[DOUBLE]) |
|:---:|:-------------------------------------|
|  4  | `[33.9, 91.0, 19.5]`                 |
|  4  | `[30.4, 91.0, 16.9]`                 |
|  4  | `[30.4, 113.0, 18.0]`                |
| ... | ...                                  |

-----

## Implementation Requirements

### 1\. Syntax and Type Handling

* The parser must recognize the **`ARRAY[...]`** syntax for constructing an array from a literal list of expressions.
* The function must enforce **type homogeneity**, ensuring all elements within the array resolve to a single compatible data type.
* The implementation should support the **`ARRAY(SELECT ...)`** syntax for constructing arrays from subqueries.

### 2\. Output and Storage

* The function must correctly return a **single column value** (the array object) suitable for storage in a column defined as an array type (e.g., Parquet repeated field).
* The driver must handle the serialization of this constructed array value into the Parquet format during insertion or data processing.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.ArrayConstructorTest`**.
* Tests must verify:
  * Correct construction of simple arrays from literals (`ARRAY[1, 2, 3]`).
  * Correct handling of type promotion (e.g., `INT` and `DOUBLE` resulting in an `ARRAY[DOUBLE]`).
  * Correct construction of arrays from subquery results (`ARRAY(SELECT ...)`).
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.