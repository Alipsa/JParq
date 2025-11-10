## 📝 Requirements Document: SQL `TABLESAMPLE` Clause

**Please add support for the `TABLESAMPLE` clause, aligning with the SQL standard (specifically adopted by many vendors like SQL Server, Oracle, and PostgreSQL, based on early proposals).**

The `TABLESAMPLE` clause is a standard-aligned feature used to efficiently retrieve a **random subset (a sample)** of rows from a table rather than scanning the entire table. This is crucial for initial data exploration, quick analytical checks, and developing models on large datasets where processing the full table is unnecessary or too time-consuming.

-----

## ⚙️ Key Characteristics According to the Standard

`TABLESAMPLE` is used in the `FROM` clause immediately following the table name and must adhere to a specific sampling method.

* **Function:** Returns a random subset of the table's rows, significantly reducing the data volume processed by the query.
* **Placement:** It is applied directly to a table (or view/derived table) in the `FROM` clause.
* **Randomness:** The sampling must provide a degree of randomness, although the exact method is often left to the implementation (e.g., Row-based or Page/Block-based sampling).

### Syntax

The most commonly supported syntax, reflecting the standard proposal, involves specifying a percentage of the rows:

```sql
SELECT
    column_A,
    column_B
FROM
    TableName TABLESAMPLE (N PERCENT)
    [REPEATABLE (seed_value)]; -- Optional clause for deterministic sampling
```

* **`N PERCENT`:** Specifies the approximate percentage of the rows to be included in the sample.
* **`REPEATABLE (seed_value)` (Optional):** Ensures that if the query is run multiple times with the same `seed_value`, the same sample of rows is returned. This is essential for testing and reproducible analysis.

-----

## 🏎️ Example using the `mtcars` Dataset

We will use `TABLESAMPLE` to retrieve a random subset of approximately $50\%$ of the cars, which is useful for quick statistical tests or validation.

**Goal:** Retrieve a sample of approximately $50\%$ of the cars in the `mtcars` table.

```sql
SELECT
    model_name,
    mpg,
    hp
FROM
    mtcars
TABLESAMPLE (50 PERCENT)
REPEATABLE (12345) -- Use a seed to ensure the same 50% subset is returned each time
ORDER BY
    mpg DESC;
```

### Expected Output (Conceptual, $\approx 50\%$ of 32 rows, likely 15-17 rows):

| model\_name                   | mpg  | hp  |
|:------------------------------|:----:|:---:|
| Datsun 710                    | 22.8 | 93  |
| Lotus Europa                  | 30.4 | 113 |
| Ford Pantera L                | 15.8 | 264 |
| Maserati Bora                 | 15.0 | 335 |
| Porsche 914-2                 | 26.0 | 91  |
| ... (approx. 10-12 more rows) | ...  | ... |

-----

## 📝 Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize the **`TABLESAMPLE (N PERCENT)`** clause immediately following a table identifier in the `FROM` clause.
* The parser must recognize and handle the optional **`REPEATABLE (seed_value)`** clause.

### 2\. Sampling Logic

* The implementation must apply a sampling mechanism to the data source (Parquet file) to return approximately $N$ percent of the rows.
* If `REPEATABLE (seed_value)` is present, the sampling mechanism **must use the provided seed** to ensure the result set is deterministic and reproducible across multiple query executions.
* The sampling must be performed efficiently, ideally without requiring a full read and sort of the entire table if the underlying data source allows block-level sampling.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.sampling.TableSampleTest`**.
* Tests must verify:
  * Basic sampling (e.g., `TABLESAMPLE (50 PERCENT)` yields about half the rows).
  * **Determinism:** Two calls with the same `REPEATABLE (seed)` return the exact same rows.
  * **Non-Determinism:** Calls without the `REPEATABLE` clause return different, random sets of rows (with high probability).
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.