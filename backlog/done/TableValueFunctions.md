## Requirements Document: SQL Table-Valued Function (TVF) Syntax Compliance

**Goal:** Ensure the parser supports the full standard syntax for calling Table-Valued Functions (TVFs) in the `FROM` clause, leveraging the existing and proven `LATERAL` execution logic.

-----

### Key Remaining Syntactic Requirements

Given that `LATERAL` is already implemented and functional, the focus is on compliance with the two standard function call formats:

1.  **Standard `TABLE(...)` Wrapper:** A routine (TVF) that returns a table should be formally wrapped in the `TABLE()` keyword, as defined in the SQL standard for general TVF calls.
2.  **Explicit Function Call:** Allowing any user-defined function (or built-in like `UNNEST`) to be called directly in the `FROM` clause.

### Syntax Compliance to be Verified

The implementation must correctly parse and execute the following standard constructs:

```sql
-- 1. Standard TVF Wrapper (Must be supported for general compliance)
FROM
    Table1 t1
JOIN LATERAL
    TABLE(MyCustomFunction(t1.param)) AS t2 (output_col)
ON TRUE;

-- 2. UNNEST with TABLE() wrapper (Verification of general TVF structure)
FROM
    mtcars m
JOIN LATERAL
    TABLE(UNNEST(m.GearRanges)) AS g(Allowed_Gear)
ON TRUE;
```

-----

## Example using the `mtcars` Dataset: Final Structural Test

This test verifies that the parser correctly handles the `TABLE()` wrapper, feeding its result to the existing `LATERAL` logic.

**Goal:** Verify correct parsing and execution using the mandatory standard `TABLE()` syntax wrapper with the already implemented `UNNEST` function.

```sql
SELECT
    m.cyl,
    m.hp,
    g.Allowed_Gear
FROM
    mtcars m
-- The driver must parse and execute this structure correctly.
JOIN LATERAL 
    TABLE(UNNEST(m.GearRanges)) AS g(Allowed_Gear) 
ON TRUE
WHERE
    g.Allowed_Gear > 4
ORDER BY
    m.cyl, g.Allowed_Gear;
```

-----

## Implementation Requirements

### 1\. Syntax Parsing

* The parser must recognize and correctly handle the optional **`TABLE(...)` wrapper** when a function call is present in the `FROM` clause.
* The parser must successfully integrate this `TABLE()` construct into the existing `LATERAL` execution pipeline.

### 2\. Testing Requirements

* Create a new test within the existing **`jparq.derived.TableValueFunctionTest`** class (or create a new, similar test class) specifically to verify the **`TABLE(UNNEST(...))`** syntax.
* Tests must verify:
  * **`TABLE()` Functionality:** Correct execution of a correlated query using the `TABLE(UNNEST(...))` syntax.
* Adhere to all coding standards (Javadoc, Checkstyle, PMD, Spotless).
* All existing tests must pass after implementation using `mvn -Dspotless.check.skip=true verify`.