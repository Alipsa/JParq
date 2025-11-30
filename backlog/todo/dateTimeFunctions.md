The SQL standard defines a set of **date and time functions** that must be supported by compliant database systems. These functions allow users to retrieve current date/time values, extract specific parts from a date/time value, and perform interval arithmetic.

Of these, most of the standard is already implemented. However, one issue remains:

## 1. Current Date and Time Functions

These functions return the date and/or time based on the database server's clock and are guaranteed to return the same value within a single transaction.

| Function                | Data Type          | Purpose                                                      | Example Output                      |
|:------------------------|:-------------------|:-------------------------------------------------------------|:------------------------------------|
| **`CURRENT_DATE`**      | $\text{DATE}$      | Returns the current date (no time component).                | $\text{2025-11-25}$                 |
| **`CURRENT_TIME`**      | $\text{TIME}$      | Returns the current time (no date component).                | $\text{10:30:00+01:00}$             |
| **`CURRENT_TIMESTAMP`** | $\text{TIMESTAMP}$ | Returns the current date and time (with fractional seconds). | $\text{2025-11-25 22:43:38.123456}$ |
| **`LOCALTIME`**         | $\text{TIME}$      | Returns the local time (time without time zone).             | $\text{22:43:38}$                   |
| **`LOCALTIMESTAMP`**    | $\text{TIMESTAMP}$ | Returns the local timestamp (timestamp without time zone).   | $\text{2025-11-25 22:43:38.123456}$ |

Of these CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP are already implemented but CURRENT_TIME, and CURRENT_TIMESTAMP are implemented incorrectly. LOCALTIME and LOCALTIMESTAMP 
are not implemented (but the syntax is supported) and return null.

Currently, the query:
```sql
SELECT
CURRENT_DATE as cur_date,
CURRENT_TIME AS cur_time,
CURRENT_TIMESTAMP as cur_timestamp,
LOCALTIME as local_time,
LOCALTIMESTAMP as local_timestamp
FROM employees LIMIT 1
```

returns the following

| cur_date   | cur_time  | cur_timestamp	                 | LOCALTIME	 | LOCALTIMESTAMP |
|------------|-----------|--------------------------------|------------|----------------|
| 2025-11-27 | 19:19:26	 | 2025-11-27 19:19:26.434561846	 | null	      | null           |

Of this, only current_date is correct, change the other ones as follows:
- CURRENT_TIME should contain timezone information e.g. 19:19:26+01:00
- LOCALTIME should not contain timezone info i.e. it should behave like the current CURRENT_TIME implementation.
- CURRENT_TIMESTAMP should return a TIMESTAMP WITH TIME ZONE 
- LOCALTIMESTAMP should return a TIMESTAMP WITHOUT TIME ZONE.


A fully compliant JDBC implementation for the `DatabaseMetaData.getSupportedDateTimeFunctions()` method should return a comma-separated string containing the **time and date functions defined in the ODBC/JDBC escape syntax** that are supported by the underlying database.

This list is not based on the raw SQL standard function names (like `CURRENT_DATE`), but on the **canonical functions** that JDBC drivers map to the database's native functions.

-----

## Canonical Date/Time Functions

For full compliance, the returned string should include the following standard canonical date and time functions:

| Canonical Function | JDBC Description                                                                  | SQL Standard Equivalent (General)        |
|:-------------------|:----------------------------------------------------------------------------------|:-----------------------------------------|
| `CURDATE`          | Returns the current date as a DATE value.                                         | `CURRENT_DATE`                           |
| `CURTIME`          | Returns the current time as a TIME value (without time zone).                     | `LOCALTIME` (or similar)                 |
| `NOW`              | Returns the current date and time as a TIMESTAMP value (often without time zone). | `LOCALTIMESTAMP` (or similar)            |
| `DAYOFWEEK`        | Returns the day of the week (1=Sunday, 7=Saturday).                               | -                                        |
| `DAYOFMONTH`       | Returns the day of the month (1-31).                                              | `EXTRACT(DAY FROM date)`                 |
| `DAYOFYEAR`        | Returns the day of the year (1-366).                                              | `EXTRACT(DOY FROM date)`                 |
| `HOUR`             | Returns the hour (0-23).                                                          | `EXTRACT(HOUR FROM time)`                |
| `MINUTE`           | Returns the minute (0-59).                                                        | `EXTRACT(MINUTE FROM time)`              |
| `MONTH`            | Returns the month (1-12).                                                         | `EXTRACT(MONTH FROM date)`               |
| `QUARTER`          | Returns the quarter of the year (1-4).                                            | -                                        |
| `SECOND`           | Returns the second (0-59).                                                        | `EXTRACT(SECOND FROM time)`              |
| `WEEK`             | Returns the week of the year.                                                     | -                                        |
| `YEAR`             | Returns the year.                                                                 | `EXTRACT(YEAR FROM date)`                |
| `TIMESTAMPADD`     | Adds an interval to a timestamp or date value.                                    | Date/Time Arithmetic (`+` or `INTERVAL`) |
| `TIMESTAMPDIFF`    | Returns the difference between two timestamp or date values.                      | Date/Time Subtraction (`-`)              |

### Example Return Value

A fully compliant driver for a database that supports all these functions would return a string like:

```
{fn CURDATE}, {fn CURTIME}, {fn NOW}, {fn DAYOFWEEK}, {fn DAYOFMONTH}, {fn DAYOFYEAR}, {fn HOUR}, {fn MINUTE}, {fn MONTH}, {fn QUARTER}, {fn SECOND}, {fn WEEK}, {fn YEAR}, {fn TIMESTAMPADD}, {fn TIMESTAMPDIFF}
```

## Why the Canonical Names? 💡

The reason JDBC uses these **canonical names** (which are part of the JDBC escape syntax, usually enclosed in `{fn ...}`) rather than the raw SQL standard keywords is to ensure **vendor independence**:

1.  **Normalization:** JDBC provides a standardized way to call common functions, even if the underlying database uses a different name (`NOW()` in MySQL, `GETDATE()` in SQL Server, `SYSDATE` in Oracle, etc.).
2.  **Driver Mapping:** The JDBC driver intercepts the escape syntax, `{fn CURDATE}`, and translates it into the database's native, proprietary SQL syntax before sending the command to the server.

For instance, a Java application can execute:
`SELECT {fn CURDATE()}`
The **JParq** driver should translate this to:
`SELECT CURRENT_DATE`


