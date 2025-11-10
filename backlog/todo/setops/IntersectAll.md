## Requirements Document: SQL `INTERSECT ALL` Set Operator

**Please add support for the `INTERSECT ALL` set operator, aligning with the SQL Standard (SQL:1999/2003 and later).**

The `INTERSECT ALL` operator is a standard set operation that returns all matching rows from the two input queries, preserving duplicates. Unlike the basic `INTERSECT` (which is equivalent to `INTERSECT DISTINCT` and removes duplicates), `INTERSECT ALL` is required for advanced analytical and reporting tasks where the frequency of matching rows must be maintained.

-----

### Key Characteristics According to the SQL Standard

`INTERSECT ALL` performs the set intersection while respecting the multiplicity (the count) of matching rows.

* **Function:** Returns all rows that exist in **both** the first query's result set (R_1) and the second query's result set (R_2).
* **Multiplicity Rule:** If a row appears M times in R_1 and N times in R_2, it appears in the final result set **MIN(M, N)** times.
* **Compatibility:** Both result sets must be **union-compatible** (same number of columns, and corresponding columns must have compatible data types).
* **Precedence:** `INTERSECT ALL` has a higher precedence than `UNION ALL` or `EXCEPT ALL`, meaning it is evaluated first unless overridden by parentheses.

### Syntax

```sql
SELECT column_A, column_B FROM Query1
INTERSECT ALL
SELECT column_A, column_B FROM Query2;
```

-----

## Example using the `mtcars` Dataset

We will use `INTERSECT ALL` to find the cars that are common between two different "virtual" lists, preserving the exact number of times a car appears in both lists.

**Goal:** Find the `cyl` and `gear` combinations that appear in both Query 1 (Cars with HP > 150) and Query 2 (Cars with MPG < 16), preserving the lower count of the two.

```sql
-- Query 1: Cars with high HP (> 150)
SELECT cyl, gear FROM mtcars WHERE hp > 150
INTERSECT ALL
-- Query 2: Cars with low MPG (< 16)
SELECT cyl, gear FROM mtcars WHERE mpg < 16;
```

### 📊 Expected Output (Conceptual)

Assuming the following counts for the row combination `(8, 3)`:

* Query 1 (HP \> 150): The row `(8, 3)` appears 12 times.
* Query 2 (MPG \< 16): The row `(8, 3)` appears 8 times.

The result set for that combination must contain MIN(12, 8) = 8 rows of `(8, 3)`.

| cyl | gear | (Number of times this row appears) |
|:---:|:----:|:-----------------------------------|
|  8  |  3   | 8 times (since 8 < 12)             |
|  8  |  5   | 1 time (since 1 < 2)               |
| ... | ...  | ...                                |

-----

## Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize the **`INTERSECT ALL`** keyword sequence.
* The parser must ensure both input queries are **union-compatible**.

### 2\. Multiplicity Logic

* The implementation must correctly calculate and maintain the multiplicity of rows in the final result set.
  * For every distinct row, the implementation must count its occurrences (M) in the first result set and its occurrences (N) in the second result set.
  * The output must include the row MIN(M, N) times.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.set.IntersectAllTest`**.
* Tests must verify:
  * **Multiplicity:** A row that appears M times in Query 1 and N times in Query 2 appears MIN(M, N) times in the result.
  * **No Duplicates Removal:** Test that `INTERSECT ALL` output differs from the already implemented `INTERSECT` output when duplicates are present in the source data.
  * **Compatibility Errors:** Test that the query fails if the two input queries are not union-compatible (e.g., different column counts).
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.