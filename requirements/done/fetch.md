**Please add support for the SQL Standard `FETCH` and `FETCH FIRST` clauses, aligning with the SQL:2003 standard (and subsequent revisions).**

These clauses provide a standard, portable way to limit the number of rows returned by a query. While a proprietary `LIMIT` clause is already supported, implementing `FETCH` ensures full compliance with the SQL standard for row limiting, which is essential for maximizing driver portability.

-----

### Key Characteristics According to the SQL Standard

The `FETCH` clause is part of the `ORDER BY` clause and is defined to follow the optional `OFFSET` clause.

* **Function:** Restricts the size of the final result set, typically after the rows have been sorted by the `ORDER BY` clause.
* **Placement:** Must appear at the very end of the `SELECT` statement, following the optional `OFFSET` clause.
* **Syntax Variants:** The standard defines several variants to express the same functionality:
  * `FETCH FIRST N ROWS ONLY`
  * `FETCH FIRST N ROW ONLY`
  * `FETCH NEXT N ROWS ONLY`
  * `FETCH NEXT N ROW ONLY`
* **Difference from `LIMIT`:** The primary difference is syntactic compliance. Logically, `FETCH FIRST N ROWS ONLY` performs the same row limiting as `LIMIT N`.

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
-- OFFSET is optional and must precede FETCH if present
[OFFSET start_row {ROW | ROWS}]
FETCH {FIRST | NEXT} N {ROW | ROWS} ONLY;
```

*(Note: Since `LIMIT` is supported, the core logic for restricting row count is expected to be reusable.)*

-----

## Example using the `mtcars` Dataset

We will use the `FETCH FIRST` clause to retrieve the two most fuel-efficient cars from the dataset, demonstrating its use after the `ORDER BY` clause.

**Goal:** Find the top 2 cars by MPG using the standard SQL `FETCH FIRST` clause.

```sql
SELECT
    model_name,
    mpg,
    cyl
FROM
    mtcars
ORDER BY
    mpg DESC
FETCH FIRST 2 ROWS ONLY;
```

### Expected Output (Conceptual)

The output must contain exactly two rows corresponding to the two highest `mpg` values.

| model\_name    | mpg  | cyl |
|:---------------|:----:|:---:|
| Toyota Corolla | 33.9 |  4  |
| Lotus Europa   | 30.4 |  4  |

-----

## Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize the **`FETCH FIRST`** and **`FETCH NEXT`** variants.
* The parser must accept **`ROW`** or **`ROWS`** and **`ONLY`** as terminal keywords.
* The parser must ensure that the `FETCH` clause appears only **after the `ORDER BY` and optional `OFFSET`** clauses.

### 2\. Logical Mapping

* The `FETCH` clause must be logically mapped to the existing `LIMIT` implementation within the query execution plan.
* The system must correctly extract the row count (N) from the `FETCH` clause.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.FetchTest`**.
* Tests must verify:
  * **Basic `FETCH`:** Correctly restricts the result set (e.g., `FETCH FIRST 5 ROWS ONLY`).
  * **Keyword Variants:** Test the different keyword combinations (`FETCH FIRST 1 ROW ONLY`, `FETCH NEXT 3 ROWS ONLY`).
  * **Integration with `ORDER BY`:** Verify that `FETCH` only takes the top rows *after* sorting has occurred.
  * **Coexistence:** Ensure that the new `FETCH` implementation does not interfere with the existing `LIMIT` support and reuse implementation constructs where possible.
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.