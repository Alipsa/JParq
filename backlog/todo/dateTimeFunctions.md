The SQL standard defines a set of **date and time functions** (often grouped with **system functions**) that must be supported by compliant database systems. These functions allow users to retrieve current date/time values, extract specific parts from a date/time value, and perform interval arithmetic.

The most widely supported and truly **standard** date and time functions fall into three main categories:

---

## 1. Current Date and Time Functions

These functions return the date and/or time based on the database server's clock and are guaranteed to return the same value within a single transaction.

| Function | Data Type | Purpose | Example Output |
| :--- | :--- | :--- | :--- |
| **`CURRENT_DATE`** | $\text{DATE}$ | Returns the current date (no time component). | $\text{2025-11-25}$ |
| **`CURRENT_TIME`** | $\text{TIME}$ | Returns the current time (no date component). | $\text{22:43:38}$ |
| **`CURRENT_TIMESTAMP`** | $\text{TIMESTAMP}$ | Returns the current date and time (with fractional seconds). | $\text{2025-11-25 22:43:38.123456}$ |
| **`LOCALTIME`** | $\text{TIME}$ | Returns the local time (time without time zone). | $\text{22:43:38}$ |
| **`LOCALTIMESTAMP`** | $\text{TIMESTAMP}$ | Returns the local timestamp (timestamp without time zone). | $\text{2025-11-25 22:43:38.123456}$ |

***

## 2. Date/Time Component Extraction

The standard provides one primary, powerful function for extracting specific parts (like the year, month, or hour) from a date/time or interval value.

### **`EXTRACT`**

This function takes a date/time field identifier and a date/time or interval value, returning the numeric value of the specified field.

$$\text{EXTRACT} ( \text{field} \ \text{FROM} \ \text{source} )$$

* **$\text{Field}$**: Can be $\text{YEAR}$, $\text{MONTH}$, $\text{DAY}$, $\text{HOUR}$, $\text{MINUTE}$, $\text{SECOND}$, $\text{TIMEZONE\_HOUR}$, $\text{TIMEZONE\_MINUTE}$, etc.
* **$\text{Source}$**: A $\text{DATE}$, $\text{TIME}$, $\text{TIMESTAMP}$, or $\text{INTERVAL}$ value.

| Example | Output |
| :--- | :--- |
| `EXTRACT(YEAR FROM DATE '2025-11-25')` | $\text{2025}$ |
| `EXTRACT(HOUR FROM TIMESTAMP '2025-11-25 22:43:38')` | $\text{22}$ |
| `EXTRACT(MINUTE FROM INTERVAL '10' DAY)` | $\text{0}$ |

***

## 3. Date/Time Arithmetic (Intervals)

The SQL standard handles date/time arithmetic primarily through **intervals** and using standard arithmetic operators (`+`, `-`) with date/time and interval data types.

The use of specific functions like $\text{DATEADD}$ or $\text{DATEDIFF}$ is generally **vendor-specific** (e.g., used in SQL Server and MySQL) and is not part of the standard Core SQL. Instead, the standard relies on direct operator syntax:

| Operation                   | Standard SQL Expression                                             | Purpose                                                               |
|:----------------------------|:--------------------------------------------------------------------|:----------------------------------------------------------------------|
| **Adding an Interval**      | `DATE '2025-11-25' + INTERVAL '1' YEAR`                             | Adds one year to the date.                                            |
| **Subtracting an Interval** | `TIME '10:00:00' - INTERVAL '30' MINUTE`                            | Subtracts 30 minutes from the time.                                   |
| **Finding the Difference**  | `TIMESTAMP '2025-11-25 10:00:00' - TIMESTAMP '2025-11-24 08:00:00'` | Returns an **$\text{INTERVAL}$** value (e.g., $\text{'1 02:00:00'}$). |

To get a specific unit (like the total number of days) from the resulting interval, you would use the $\text{EXTRACT}$ function on the interval result.