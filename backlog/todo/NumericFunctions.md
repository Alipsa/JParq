The `DatabaseMetaData.getNumericFunctions()` method should return a comma-separated string containing the canonical numeric (scalar) functions that the JDBC driver and the underlying database support.

Like the string and date/time functions, these are defined in the **JDBC Escape Syntax** (`{fn ...}`) to ensure database portability for common mathematical operations.

-----

## 🔢 Standard JDBC Numeric Functions

A fully compliant JDBC driver is expected to report the following list of canonical numeric functions:

| Canonical Function                    | Purpose                                                                                               | Example Usage in JDBC                  |
|:--------------------------------------|:------------------------------------------------------------------------------------------------------|:---------------------------------------|
| **`ABS(numeric_exp)`**                | Returns the **absolute value** of the numeric expression.                                             | `{fn ABS(-10.5)}` returns 10.5         |
| **`ACOS(float_exp)`**                 | Returns the **arccosine** of the angle (in radians).                                                  | `{fn ACOS(0.5)}`                       |
| **`ASIN(float_exp)`**                 | Returns the **arcsine** of the angle (in radians).                                                    | `{fn ASIN(0.5)}`                       |
| **`ATAN(float_exp)`**                 | Returns the **arctangent** of the angle (in radians).                                                 | `{fn ATAN(1)}`                         |
| **`ATAN2(float_exp1, float_exp2)`**   | Returns the **arctangent** of the x and y coordinates (x is `float_exp2`, y is `float_exp1`).         | `{fn ATAN2(1, 1)}`                     |
| **`CEILING(numeric_exp)`**            | Returns the **smallest integer** greater than or equal to the numeric expression. (Rounds up).        | `{fn CEILING(4.2)}` returns 5          |
| **`COS(float_exp)`**                  | Returns the **cosine** of the angle (in radians).                                                     | `{fn COS(0)}` returns 1.0              |
| **`COT(float_exp)`**                  | Returns the **cotangent** of the angle (in radians).                                                  | `{fn COT(1)}`                          |
| **`DEGREES(numeric_exp)`**            | Converts a numeric expression from **radians to degrees**.                                            | `{fn DEGREES(3.14159)}` returns 180    |
| **`EXP(float_exp)`**                  | Returns the **exponential** value ($e$ raised to the power of `float_exp`).                           | `{fn EXP(1)}` returns $e$              |
| **`FLOOR(numeric_exp)`**              | Returns the **largest integer** less than or equal to the numeric expression. (Rounds down).          | `{fn FLOOR(4.8)}` returns 4            |
| **`LOG(float_exp)`**                  | Returns the **natural logarithm** of the numeric expression.                                          | `{fn LOG(e)}` returns 1.0              |
| **`LOG10(float_exp)`**                | Returns the **base-10 logarithm** of the numeric expression.                                          | `{fn LOG10(100)}` returns 2.0          |
| **`MOD(integer_exp1, integer_exp2)`** | Returns the **remainder** (modulo) of `integer_exp1` divided by `integer_exp2`.                       | `{fn MOD(10, 3)}` returns 1            |
| **`PI()`**                            | Returns the constant value of **Pi** ($3.14159...$).                                                  | `{fn PI()}`                            |
| **`POWER(numeric_exp, power)`**       | Returns the value of `numeric_exp` **raised to the power** of `power`.                                | `{fn POWER(2, 3)}` returns 8.0         |
| **`RADIANS(numeric_exp)`**            | Converts a numeric expression from **degrees to radians**.                                            | `{fn RADIANS(180)}` returns $\pi$      |
| **`RAND([integer_exp])`**             | Returns a random floating-point value between 0 and 1. An optional seed can be provided.              | `{fn RAND(10)}`                        |
| **`ROUND(numeric_exp, count)`**       | Returns the numeric expression **rounded** to `count` decimal places.                                 | `{fn ROUND(4.567, 2)}` returns 4.57    |
| **`SIGN(numeric_exp)`**               | Returns an indicator of the sign of the numeric expression: -1 (negative), 0 (zero), or 1 (positive). | `{fn SIGN(-5)}` returns -1             |
| **`SIN(float_exp)`**                  | Returns the **sine** of the angle (in radians).                                                       | `{fn SIN(1)}`                          |
| **`SQRT(float_exp)`**                 | Returns the **square root** of the numeric expression.                                                | `{fn SQRT(9)}` returns 3.0             |
| **`TAN(float_exp)`**                  | Returns the **tangent** of the angle (in radians).                                                    | `{fn TAN(1)}`                          |
| **`TRUNCATE(numeric_exp, count)`**    | Returns the numeric expression **truncated** to `count` decimal places.                               | `{fn TRUNCATE(4.567, 2)}` returns 4.56 |

### Example Return String

A typical fully compliant return string would look like:

```
{fn ABS}, {fn ACOS}, {fn ASIN}, {fn ATAN}, {fn ATAN2}, {fn CEILING}, {fn COS}, {fn COT}, {fn DEGREES}, {fn EXP}, {fn FLOOR}, {fn LOG}, {fn LOG10}, {fn MOD}, {fn PI}, {fn POWER}, {fn RADIANS}, {fn RAND}, {fn ROUND}, {fn SIGN}, {fn SIN}, {fn SQRT}, {fn TAN}, {fn TRUNCATE}
```