The SQL standard mandates a few key functions generally categorized as **"Built-in Scalar Functions"** that provide information about the current user, session, and time. These are the functions intended to be returned by $\text{getSystemFunctions()}$ as they relate to the database environment.

Unlike the vendor-specific lists of hundreds of system functions (e.g., in SQL Server's T-SQL), the core SQL standard focuses on a small, portable set of functions:

---

## 🔑 Core SQL Standard System Functions

The following functions are part of the core SQL standard and are used to retrieve information about the current session and time.

### 1. User & Authorization Functions

These retrieve identifying information about the person or process interacting with the database.

* **CURRENT_USER** or **USER**: Returns the **name of the user** currently authenticated and active in the database session. This is the authorization identifier that owns the session.
* **SESSION_USER**: Returns the **session authorization identifier**. In most systems, this is the same as CURRENT\_USER, but it allows for potential differences if the current user has temporarily switched roles.
* **SYSTEM\_USER**: Returns the name of the user connected to the database from the **host operating system**. This function is sometimes treated as standard but is less consistently supported than CURRENT_USER.

The `DatabaseMetaData.getSystemFunctions()` method, according to the JDBC specification, should return a comma-separated list of the **system (or environment) functions** supported by the underlying database and accessible via the JDBC escape syntax.

These functions typically provide information about the current user, database session, or server environment.

-----

## 💻 Standard JDBC System Functions

Unlike the numeric and string functions, the set of canonical system functions is much smaller and generally focuses on user and database information. The list should include the following:

| Canonical Function       | Purpose                                                                                                             | Example Usage in JDBC             |
|:-------------------------|:--------------------------------------------------------------------------------------------------------------------|:----------------------------------|
| **`DATABASE()`**         | Returns the **name of the database** currently in use.                                                              | `{fn DATABASE()}`                 |
| **`IFNULL(exp1, exp2)`** | Returns `exp1` if it is not NULL, otherwise returns `exp2`. This is typically used for **NULL value substitution**. | `{fn IFNULL(column_name, 'N/A')}` |
| **`USER()`**             | Returns the **username** of the current database session user.                                                      | `{fn USER()}`                     |

- Since coalesce is already fully implemented, {fn IFNULL(exp1, exp2)} can just be translated to COALESCE(exp1, exp2)
- {fn DATABASE()} can be implemented as the base directory name from the url (for the url `jdbc:jparq:/home/per/project/JParq/src/test/resources/acme` the databasename would be acme), the JParqDatabaseMetaData.getDatabaseName() can be used for this).
- `{fn USER()}` can be implemented as System.getProperty("user.name"); but the JParqDatabaseMetaData.getUser() whould preferrably be used for this to keep it DRY.

### Example Return String

A fully compliant JDBC driver should return a string containing these three functions:

```
{fn DATABASE}, {fn IFNULL}, {fn USER}
```

- Change the SUPPORTED_SYSTEM_FUNCTIONS in JParqDatabaseMetaData into an enum in a similar manner as the JdbcDateTimeFunction enum.
- Ensure that the JParqDatabaseMetaData.getSystemFunctions() returns a list of the supported numeric functions in the correct (escaped) format.
- Before the SQL is sent to the Sql parser, we should resolve the jdbc function names and translate them to the corresponding SQL function name 
- If the user runs the query SELECT {fn DATABASE} we should send SELECT DATABASE() to the parser (the result after execution should be identical to JParqDatabaseMetaData.getDatabaseName()). 
- The result of {fn IFNULL(exp1, exp2)} should be identical to COALESCE(exp1, exp2).
- The result of {fn USER()} should be identical to System.getProperty("user.name") i.e. the SystemFunctions already has the correct implementation for this.

