### The Architecture: The "Smart" JDBC Driver

Here is the workflow of a `SELECT` query in this encrypted environment:

#### 1. Configuration (The "Keyring")
Before the JDBC driver can even connect, it needs access to the keys. You wouldn't typically hardcode keys in the connection string (security risk). 
Instead, the JDBC connection properties would point to a Key Management Service (KMS) or a local keystore.

* **Connection String Example:**
  `jdbc:parquet:file://var/data?kmsType=AWS_KMS&kmsEndpoint=...&keyAccessAttributes=...`

#### 2. Metadata Discovery (The "Peek")
When a user (or BI tool like Tableau/DBeaver) connects, the driver needs to show a list of tables and columns.
* **Scenario A: Footer is Unencrypted.** The driver reads the Parquet footer. It sees all column names (e.g., `ssn`, `name`). It exposes these as SQL columns.
* **Scenario B: Footer is Encrypted.** The driver attempts to decrypt the footer using the `footer_key`.
    * *If the driver has the key:* It decrypts the metadata and shows the table schema.
    * *If the driver lacks the key:* It throws an error (e.g., "Table definition inaccessible") or hides the table entirely.

#### 3. Query Execution & Column Pruning
The user runs: `SELECT name, ssn FROM employees`

The JDBC driver parses this SQL. It knows that:
* `name` is unencrypted (Plaintext).
* `ssn` is encrypted (AES-GCM).

#### 4. The Decryption Process (On-the-Fly)
This is the critical "magic" step. The driver performs the following:

1.  **Request Keys:** The driver identifies that `ssn` requires `Key_ID_123`. It requests this key from the configured KMS (or retrieves it from its local cache).
2.  **Verify Permissions:** The KMS checks: *Does the user identity (passed via JDBC credentials) have permission to use `Key_ID_123`?*
    * **Denied:** The KMS refuses. The JDBC driver halts execution and returns a `SQL Access Denied` error.
    * **Approved:** The KMS returns the raw encryption key to the driver's memory.
3.  **Read & Decrypt:** The driver reads the Parquet file's row groups.
    * It reads the `name` column normally.
    * It reads the encrypted bytes of the `ssn` column and uses the key to decrypt them in memory.
4.  **Result Set:** The driver stitches the plaintext `name` and the now-decrypted `ssn` into a standard JDBC `ResultSet` and hands it back to the client.

### Key Challenges in this Setup

If you are implementing or evaluating such a driver, these are the edge cases to watch for:

**1. "Blind" SQL Queries**
If a user runs `SELECT *`, but they only have keys for 3 out of 10 encrypted columns, what happens?
* *Strict Mode:* The query fails completely.
* *Permissive Mode:* The driver returns `NULL` or `****` for the columns where keys are missing, and real data for the others. (This requires custom logic in the driver).

**2. Predicate Pushdown (Filtering)**
SQL relies heavily on filtering (e.g., `WHERE ssn = '123-45'`).
* **The Problem:** The driver usually pushes filters down to the file reader to skip irrelevant data. But if the data is encrypted, the file reader can't check if `ssn` equals `'123-45'` because the file only contains scrambled bytes.
* **The Solution:** The driver must pull *all* the encrypted data into memory, decrypt it, and *then* apply the filter. This kills performance.
    * *Advanced Note:* Parquet Modular Encryption supports **Plaintext Footers with Column Index**, which *might* allow some filtering stats to remain visible, but usually, encryption sacrifices predicate pushdown performance.

**3. Key Caching**
Calling a remote KMS for every single SQL query is slow. The JDBC driver must implement secure memory caching for keys. If the driver lives on a generic application server, this cache becomes a high-value target for hackers.

### Summary of JDBC Driver Role

In this scenario, the JDBC driver acts as the **Policy Enforcement Point**.

| Action             | Standard JDBC               | Encrypted Parquet JDBC                                         |
|:-------------------|:----------------------------|:---------------------------------------------------------------|
| **Schema Read**    | Reads file footer           | Reads footer + Decrypts footer (if needed)                     |
| **Data Access**    | Reads bytes -> Deserializes | Reads bytes -> **Fetches Key** -> **Decrypts** -> Deserializes |
| **Access Control** | None (Relies on DB engine)  | **Enforces Key Access Policies** (via KMS integration)         |

