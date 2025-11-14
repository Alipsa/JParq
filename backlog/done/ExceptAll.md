**Please add support for the `EXCEPT ALL` set operator, aligning with the SQL Standard (SQL:1999/2003 and later).**

The `EXCEPT ALL` operator is a standard set operation that returns rows present in the first query's result set (R_1) but not in the second query's result set (R_2), critically **preserving duplicates** based on a subtraction rule. Unlike the basic `EXCEPT` (which is equivalent to `EXCEPT DISTINCT` and removes duplicates), `EXCEPT ALL` is vital when the frequency of rows is meaningful and must be maintained after subtraction.

-----

### Key Characteristics According to the SQL Standard

`EXCEPT ALL` performs the set difference operation while respecting the multiplicity (the count) of rows.

* **Function:** Returns all rows from R_1 that do not have a matching instance in R_2.
* **Multiplicity Rule:** If a distinct row appears M times in R_1 and N times in R_2, it appears in the final result set **MAX(0, M - N)** times.
* **Compatibility:** Both result sets must be **union-compatible** (same number of columns, and corresponding columns must have compatible data types).
* **Precedence:** `EXCEPT ALL` has the same precedence as `UNION ALL` (which is typically lower than `INTERSECT ALL`), meaning operations are evaluated left-to-right unless overridden by parentheses.

### Syntax

```sql
SELECT column_A, column_B FROM Query1
EXCEPT ALL
SELECT column_A, column_B FROM Query2;
```

-----

## Example using the `mtcars` Dataset

We will use `EXCEPT ALL` to find the car models that appear in a primary list, but subtract duplicates that are removed in a secondary list.

**Goal:** Start with a list of all `cyl` and `gear` combinations (Query 1) and subtract all instances of `(4, 4)` and `(6, 3)` found in Query 2. The final result should show the remaining counts for each combination.

```sql
-- Query 1: All combinations of Cylinders and Gears (duplicates preserved)
SELECT cyl, gear FROM mtcars
EXCEPT ALL
-- Query 2: A specific list of rows to subtract
SELECT 4 AS cyl, 4 AS gear FROM (VALUES(1), (2), (3)) AS T(i) -- Creates 3 rows of (4, 4)
UNION ALL
SELECT 6 AS cyl, 3 AS gear FROM (VALUES(1), (2)) AS T(i);     -- Creates 2 rows of (6, 3)
```

### 📊 Expected Output (Conceptual)

| Combination | Query 1 Count (M) | Query 2 Count (N) | Result Count (MAX}(0, M-N)) |
| :---: | :---: | :---: | :---: |
| **(4, 4)** | 8 | 3 | **5** |
| **(6, 3)** | 4 | 2 | **2** |
| **(8, 3)** | 12 | 0 | **12** |

The final result set will include **5 rows of `(4, 4)`**, **2 rows of `(6, 3)`**, and all other combinations with their original counts preserved, as N=0 for them.

-----

## Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize the **`EXCEPT ALL`** keyword sequence.
* The parser must ensure both input queries are **union-compatible**.

### 2\. Multiplicity Logic

* The implementation must correctly calculate and maintain the multiplicity of rows in the final result set.
  * For every distinct row, the implementation must count its occurrences (M) in R_1 and its occurrences (N) in R_2.
  * The output must include the row MAX(0, M - N) times.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.setops.ExceptAllTest`**.
* Tests must verify:
  * **Multiplicity Subtraction:** A row with M instances in R_1 and N instances in R_2 appears MAX(0, M-N) times in the result.
  * **No Negative Counts:** Test cases where N > M result in 0 rows of that combination.
  * **Difference from `EXCEPT`:** Test that the output is distinct from the already implemented `EXCEPT` (DISTINCT) when duplicates are involved.
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.