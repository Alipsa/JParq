A fully compliant implementation of `DatabaseMetaData.getSQLKeywords()` should return a $\text{comma-separated list of all $\mathbf{SQL}$ keywords}$ supported by the database that are **not already part of the $\text{SQL92}$, $\text{SQL99}$ or
$\text{SQL2003}$standards**.

---

## Key Requirements for the Return Value

The $\text{JDBC}$ specification defines exactly what this method is meant to report:

1.  **Scope:** The method must only return **non-standard** keywords. This is crucial for applications that generate $\text{SQL}$, as it allows them to identify and avoid using vendor-specific terms as table or column names, thereby preventing syntax errors.
2.  **Exclusions:** It **must not** include keywords defined in the $\text{SQL92}$ or $\text{SQL99}$ standards. These standard keywords are handled by other `DatabaseMetaData` methods, specifically:
  * `getSQLStateType()`
  * `getIdentifierQuoteString()`
  * `getStringFunctions()`, `getNumericFunctions()`, `getSystemFunctions()`, `getTimeDateFunctions()`
3.  **Format:** The list of keywords must be returned as a single **`String`** with the keywords separated by a **comma and a space** (`, `) or just a **comma** (`,`).

## Typical Examples of Keywords

The keywords returned by `getSQLKeywords()` are typically those added by the database vendor to support unique features.

| Vendor                   | Example Keywords Returned                |
|:-------------------------|:-----------------------------------------|
| **PostgreSQL**           | `LIMIT`, `ILIKE`, `RETURNING`, `LATERAL` |
| **MySQL**                | `LIMIT`, `REPLACE`, `ENGINE`, `TINYINT`  |
| **Microsoft SQL Server** | `TOP`, `CLUSTERED`, `GO`, `NOLOCK`       |
| **Oracle**               | `CONNECT`, `PRIOR`, `START`              |

By returning only these proprietary keywords, the $\text{JDBC}$ driver provides the application with the minimum necessary information to correctly escape identifiers or modify $\text{SQL}$ syntax when dealing with that specific database.

Examine the JParq code to create a list of keywords that are not part of the SQL standards and add them to the SUPPORTED_KEYWORDS static variable in JParqDatabaseMetaData.