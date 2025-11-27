The `DatabaseMetaData.getStringFunctions()` method returns a comma-separated list of the canonical string (scalar) functions that the JDBC driver and the underlying database support.

These functions are defined in the **JDBC Escape Syntax** (using the `{fn ...}` format) to ensure application portability across different SQL databases.

-----

## 💻 Standard JDBC String Functions

A fully compliant JDBC implementation is expected to support and report the following list of canonical string functions, as defined in the JDBC specification (often based on the ODBC standard):

| Canonical Function                                    | Purpose                                                                                                                                                                           | Example Usage in JDBC                                  |
|:------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------|
| **`ASCII(string_exp)`**                               | Returns the **ASCII code** value of the leftmost character of a string expression.                                                                                                | `{fn ASCII('A')}` returns 65                           |
| **`CHAR(code)`**                                      | Returns the **character** corresponding to the given ASCII code value.                                                                                                            | `{fn CHAR(65)}` returns 'A'                            |
| **`CONCAT(string_exp1, string_exp2)`**                | Returns the string resulting from **concatenating** the two string expressions.                                                                                                   | `{fn CONCAT('Hello', 'World')}` returns 'HelloWorld'   |
| **`DIFFERENCE(string_exp1, string_exp2)`**            | Returns an integer indicating the **difference** between the `SOUNDEX` values of the two strings (a measure of similarity).                                                       | `{fn DIFFERENCE('Smith', 'Smyth')}`                    |
| **`INSERT(string_exp1, start, length, string_exp2)`** | Returns a string where `length` characters have been deleted from `string_exp1` starting at `start`, and where `string_exp2` has been **inserted** into `string_exp1` at `start`. | `{fn INSERT('ABCDEF', 3, 2, 'xyz')}` returns 'ABxyzEF' |
| **`LCASE(string_exp)`**                               | Converts uppercase characters in the string expression to **lowercase**.                                                                                                          | `{fn LCASE('Hello')}` returns 'hello'                  |
| **`LEFT(string_exp, count)`**                         | Returns the **leftmost** `count` characters of the string expression.                                                                                                             | `{fn LEFT('ABCDEF', 3)}` returns 'ABC'                 |
| **`LENGTH(string_exp)`**                              | Returns the **length** of the string expression (in characters or bytes, depending on the implementation).                                                                        | `{fn LENGTH('Hello')}` returns 5                       |
| **`LOCATE(string_exp1, string_exp2[, start])`**       | Returns the **starting position** of the first occurrence of `string_exp1` within `string_exp2`. An optional `start` position can be specified.                                   | `{fn LOCATE('C', 'ABCDEF')}` returns 3                 |
| **`LTRIM(string_exp)`**                               | Returns the string expression with **leading spaces removed**.                                                                                                                    | `{fn LTRIM('  Text')}` returns 'Text'                  |
| **`REPEAT(string_exp, count)`**                       | Returns a string composed of `string_exp` **repeated** `count` times.                                                                                                             | `{fn REPEAT('a', 3)}` returns 'aaa'                    |
| **`REPLACE(string_exp1, string_exp2, string_exp3)`**  | **Replaces** all occurrences of `string_exp2` in `string_exp1` with `string_exp3`.                                                                                                | `{fn REPLACE('abab', 'a', 'x')}` returns 'xbxb'        |
| **`RIGHT(string_exp, count)`**                        | Returns the **rightmost** `count` characters of the string expression.                                                                                                            | `{fn RIGHT('ABCDEF', 3)}` returns 'DEF'                |
| **`RTRIM(string_exp)`**                               | Returns the string expression with **trailing spaces removed**.                                                                                                                   | `{fn RTRIM('Text  ')}` returns 'Text'                  |
| **`SOUNDEX(string_exp)`**                             | Returns a four-character code (the **Soundex code**) to evaluate the phonetic similarity of words.                                                                                | `{fn SOUNDEX('Robert')}` returns 'R163'                |
| **`SPACE(count)`**                                    | Returns a string consisting of `count` **spaces**.                                                                                                                                | `{fn SPACE(5)}` returns '     '                        |
| **`SUBSTRING(string_exp, start, length)`**            | Returns a **substring** derived from `string_exp` starting at `start` position for `length` characters.                                                                           | `{fn SUBSTRING('ABCDEF', 2, 3)}` returns 'BCD'         |
| **`UCASE(string_exp)`**                               | Converts lowercase characters in the string expression to **uppercase**.                                                                                                          | `{fn UCASE('hello')}` returns 'HELLO'                  |

The actual value returned by `getStringFunctions()` would be a single string listing the functions supported by the specific driver and database, separated by commas.

### Example Return String

A typical fully compliant return string would look like:

```
{fn ASCII}, {fn CHAR}, {fn CONCAT}, {fn DIFFERENCE}, {fn INSERT}, {fn LCASE}, {fn LEFT}, {fn LENGTH}, {fn LOCATE}, {fn LTRIM}, {fn REPEAT}, {fn REPLACE}, {fn RIGHT}, {fn RTRIM}, {fn SOUNDEX}, {fn SPACE}, {fn SUBSTRING}, {fn UCASE}
```