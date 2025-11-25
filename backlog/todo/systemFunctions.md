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
