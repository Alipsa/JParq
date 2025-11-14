**Please add specific support for the `UNNEST` Table Function, aligning with the SQL:1999/2003 standard for Collection Derived Tables.**

`UNNEST` is a standard Table-Valued Function (TVF) used specifically to transform an array or collection data type into a set of rows (a table). Its primary purpose is to "flatten" complex or repeated data structures, making the individual array elements accessible for querying, joining, and filtering.

-----

## Key Characteristics According to the SQL Standard

`UNNEST` is the canonical read-only method for working with repeated data and is required for any driver supporting array or collection types.

* **Function:** `UNNEST` takes an array expression and returns a table where each element of the array becomes a row in the result set.
* **Placement:** It must be called in the `FROM` clause, requiring an alias for the resulting table and optional aliases for its columns.
* **Correlation:** It is inherently a **correlated function** when used on a column of a preceding table, necessitating the implicit or explicit functionality of a **Lateral Join**.

### Syntax

```sql
SELECT 
    p.column_A, 
    u.item 
FROM 
    ParentTable p, 
    UNNEST(p.array_column) AS u (item);
```

### Mandatory `WITH ORDINALITY` Support

The standard mandates support for tracking the original position of the elements during the flattening process:

* **Syntax:**
  ```sql
  FROM 
      UNNEST(p.array_column) WITH ORDINALITY AS u (item, position);
  ```
* **Function:** The `WITH ORDINALITY` clause adds a second column (`position` in the example) to the output table, containing the 1-based index of the element in the original array.

-----

## Example using the `mtcars` Dataset

We will use `UNNEST` and `WITH ORDINALITY` on a hypothetical `Tags` array column to demonstrate full functionality.

**Goal:** List the name of each car and all of its tags, including the order in which the tag appeared in the original list.

Assume `mtcars` is conceptually augmented with a column `Tags` (e.g., `['Sedan', 'Sport']`).

```sql
SELECT
    m.model_name,
    t.tag_value,
    t.tag_position
FROM
    mtcars m
JOIN 
    UNNEST(m.Tags) WITH ORDINALITY AS t(tag_value, tag_position) ON TRUE
WHERE
    t.tag_position = 1 -- Find the primary tag for each car
ORDER BY
    m.model_name;
```

### Expected Output (Conceptual, assuming `Tags` array):

| model\_name | tag\_value | tag\_position |
| :--- | :--- | :---: |
| Mazda RX4 | Sport | 1 |
| Datsun 710 | Sedan | 1 |
| Valiant | Family | 1 |
| ... | ... | ... |

-----

## Implementation Requirements

### 1\. Syntax and Array Handling

* The parser must recognize the **`UNNEST(array_column)`** function call in the `FROM` clause.
* The parser must correctly handle the optional **`WITH ORDINALITY`** clause, adding an integer column for the array index.
* The implementation must correctly flatten the underlying array data structure (Parquet repeated field) into multiple logical rows.

### 2\. Correlated Execution

* The driver **must correctly implement the execution logic equivalent to a `LATERAL` join** when `UNNEST` references a column from a preceding table. The `UNNEST` operation must execute once for every row of the primary table.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.derived.UnnestTest`**.
* Tests must verify:
  * Correct flattening of simple array columns (`UNNEST(array)`).
  * Correct functionality of `WITH ORDINALITY` returning accurate 1-based indexes.
  * Correct flattening of nested array structures (arrays of records/structs).
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.