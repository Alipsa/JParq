**Please add support for the SQL Standard `OFFSET` clause, aligning with the SQL:2003 standard (and subsequent revisions).**

The `OFFSET` clause is a standard feature used to skip a specified number of rows from the beginning of a result set. It is essential for implementing **pagination**—retrieving data in chunks—in a standard, portable manner. Since the proprietary `LIMIT` clause is already supported for row counting, implementing `OFFSET` completes the standard pagination mechanism.

-----

### Key Characteristics According to the SQL Standard

The `OFFSET` clause is optional and is defined as part of the `ORDER BY` clause, preceding the `FETCH` clause (if present).

* **Function:** Skips the first N rows of the result set that would have been returned after the `WHERE` and `ORDER BY` clauses have been applied.
* **Placement:** Must appear after the `ORDER BY` clause and before the `FETCH` clause (or the end of the statement if `FETCH` is absent).
* **Keywords:** The standard requires the use of either `ROW` or `ROWS` after the offset value.

### Syntax

The implementation must support the following structure:

```sql
SELECT 
    column_A, 
    column_B 
FROM 
    TableName
ORDER BY 
    column_A 
OFFSET N {ROW | ROWS};
```

*(Note: Logically, this maps to the `OFFSET` functionality often built into a driver's existing `LIMIT/OFFSET` handling, even if `LIMIT` was the only supported keyword previously.)*

-----

## Example using the `mtcars` Dataset

We will use the `OFFSET` clause to skip the top two most fuel-efficient cars and start the list with the third, demonstrating its use in conjunction with sorting.

**Goal:** Find all cars sorted by MPG, but skip the top 2 results (i.e., start listing from the third most fuel-efficient car).

```sql
SELECT
    model_name,
    mpg,
    cyl
FROM
    mtcars
ORDER BY
    mpg DESC
OFFSET 2 ROWS;
```

### Expected Output (Conceptual)

The output starts with the car that has the third-highest `mpg` value.

| model\_name                    | mpg  | cyl |
|:-------------------------------|:----:|:---:|
| Porsche 914-2                  | 26.0 |  4  |
| Fiat X1-9                      | 27.3 |  4  |
| ... (remainder of the dataset) | ...  | ... |

-----

## Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize the **`OFFSET N {ROW | ROWS}`** syntax.
* The parser must accept **`ROW`** or **`ROWS`** after the numerical offset value.
* The parser must ensure that the `OFFSET` clause appears **after the `ORDER BY` clause** and **before the `FETCH` clause** (if `FETCH` is also implemented).

### 2\. Logical Mapping

* The `OFFSET` clause must be logically mapped to the existing offset functionality used by the proprietary `LIMIT` clause's implementation (if it supports skipping rows).
* The system must correctly extract the offset count (N).

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.OffsetTest`**.
* Tests must verify:
  * **Basic `OFFSET`:** Correctly skips the first $N$ rows (e.g., `OFFSET 5 ROWS`).
  * **Keyword Variants:** Test the different keyword combinations (`OFFSET 1 ROW`, `OFFSET 3 ROWS`).
  * **Integration with `ORDER BY`:** Verify that the offset is applied *after* the sorting is complete.
  * **Integration with `LIMIT/FETCH`:** Test a query that uses both `OFFSET` and `LIMIT` (or `FETCH`) to retrieve a specific page of results (e.g., "skip 10, take 5").
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.