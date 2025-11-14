**Please add support for the nesting of SQL set operations (`UNION`, `INTERSECT`, `EXCEPT`), utilizing parentheses `()` to enforce order of evaluation, aligning with the SQL Standard (ISO/IEC 9075).**

Nesting set operations allows for the construction of complex queries that involve combining and comparing multiple intermediate result sets. This capability is fundamental to solving sophisticated business logic problems that require more than one simple set transformation.

-----

### Key Characteristics According to the SQL Standard

Nesting is primarily governed by the use of parentheses and the defined operator precedence.

* **Function:** Parentheses explicitly dictate the order in which set operations must be performed. The result of an inner (parenthesized) operation becomes the input for the outer operation.
* **Precedence:** While parentheses override all default precedence, the standard defines a hierarchy for unparenthesized operations: **`INTERSECT`** usually has higher precedence than **`UNION`** and **`EXCEPT`** (which are typically equal and evaluated left-to-right). The implementation must respect this standard precedence rule when parentheses are absent.
* **Compatibility:** Every subquery involved in a set operation must be **union-compatible** with the subquery it is combined with (same number of columns, and corresponding columns must have compatible data types).

### Syntax

The implementation must support arbitrary nesting levels, constrained only by resource limits.

```sql
SELECT column_A FROM Query1
UNION ALL
( 
    SELECT column_A FROM Query2
    INTERSECT 
    (
        SELECT column_A FROM Query3
        EXCEPT ALL
        SELECT column_A FROM Query4
    )
);
```

-----

## Example using the `mtcars` Dataset

We will use nested set operations to find cars that satisfy two distinct conditions: those in the (4-cylinder AND 4-gear) group, OR those that are in the (6-cylinder group) but NOT in the (3-gear group).

  **Goal:** Calculate [Set 1] U ([Set 2] \ [Set 3])

```sql
-- Query: Find cars (cyl, gear) that are either (4-cyl AND 4-gear)
-- OR (6-cyl AND NOT 3-gear).

(   -- Set 1: Find 4-cyl, 4-gear cars
    SELECT cyl, gear FROM mtcars WHERE cyl = 4 AND gear = 4
)
UNION
(   -- Inner Set Operation: (Set 2 EXCEPT Set 3)
    (
        -- Set 2: All 6-cylinder cars
        SELECT cyl, gear FROM mtcars WHERE cyl = 6
    )
    EXCEPT
    (
        -- Set 3: All 3-gear cars (to subtract from Set 2)
        SELECT cyl, gear FROM mtcars WHERE gear = 3
    )
);
```

### Expected Output (Conceptual)

The final result should exclude the 6-cylinder, 3-gear cars, but include all 6-cylinder, 4-gear, and 5-gear cars, plus all 4-cylinder, 4-gear cars.

| cyl | gear |
| :---: | :---: |
| 4 | 4 |
| 6 | 4 |
| 6 | 5 |

-----

## 📝 Implementation Requirements

### 1\. Parsing and Evaluation

* The parser must correctly recognize and prioritize the evaluation of expressions enclosed in **parentheses `()`**.
* The execution engine must strictly evaluate the innermost parenthesized set operations first, proceeding outward.
* The system must correctly apply the standard precedence rules (`INTERSECT` before `UNION`/`EXCEPT`) when parentheses are absent.

### 2\. Compatibility Checks

* Compatibility checks (same column count and compatible types) must be performed for **every pair of set operations**, ensuring that the result of an inner operation is compatible with the outer operation it feeds into.

### 3\. Testing Requirements

* Create tests to verify the functionality in a test class called **`jparq.setops.NestedSetOperationTest`**.
* Tests must verify:
  * **Parentheses Precedence:** Test a case where parentheses override the default precedence (e.g., forcing a `UNION` before an `INTERSECT`).
  * **Multi-Level Nesting:** Test a query involving three or more nested operations.
  * **Combination of `ALL` and `DISTINCT`:** Test nesting operations that use mixed modifiers (e.g., `UNION ALL` with `EXCEPT DISTINCT`).
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.