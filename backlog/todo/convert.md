Core Standard: In the SQL standard, CONVERT is generally categorized as a scalar function used for data type casting or character set manipulation.

Character Set Conversion: In the original SQL standards, the primary use of CONVERT was for converting character data from one character set to another:
CONVERT(string_expression USING conversion_name)

Because Parquet and Avro standardize on UTF-8 for their string types:
- When the client calls CONVERT(expression USING UTF8), the driver should treat this as a no-op (a direct pass-through), as the data is already in UTF-8.
- The primary task of your implementation is handling conversion from the source UTF-8 to the specified target character set (e.g., LATIN1, ASCII, etc.).

### Steps for CONVERT(Expression USING Target_Charset)
1. Read Data from Parquet: The Parquet reader (potentially via Avro's GenericRecord or SpecificRecord) reads the BYTE_ARRAY into a Java String. This process automatically uses the expected UTF-8 encoding, or the encoding specified by the schema/metadata.
2. Determine Target Encoding: The parser extracts the Target_Charset name from the SQL CONVERT clause (e.g., 'LATIN1').
3. Perform Java Conversion: You use Java's String.getBytes() method with the target encoding to get the converted byte array, and then optionally create a new Java String from those bytes for the final result.

You must maintain a mapping between the SQL standard/common character set names (e.g., LATIN1, ISO-8859-1, ASCII) and the corresponding valid Java character set names (e.g., ISO-8859-1, US-ASCII).

The mapping between character set names used in the SQL standard/common database practice and the corresponding names required by the Java runtime (which are used in your JDBC driver's implementation of `CONVERT`) is essential for proper data handling.

In Java, character set names are defined by the `java.nio.charset.Charset` class and generally adhere to the IANA standards.

## Common SQL Character Sets and Java Equivalents

The table below lists the most common character sets and their standard, canonical Java names, which should be used when implementing in-memory conversion (e.g., using `String.getBytes(String charsetName)`).

| SQL/Database Common Name | Purpose                                            | Canonical Java Encoding Name | Alias/Common Java Name |
|:-------------------------|:---------------------------------------------------|:-----------------------------|:-----------------------|
| **UTF8** (or `utf8mb4`)  | Universal character set; most modern standard.     | **UTF-8**                    | `UTF8`                 |
| **UCS2**                 | Fixed-width Unicode (used internally by some DBs). | **UTF-16BE**                 | `UnicodeBigUnmarked`   |
| **LATIN1**               | Western European (ISO 8859-1). Single-byte.        | **ISO-8859-1**               | `ISO8859_1`            |
| **ASCII**                | Basic 7-bit English alphabet.                      | **US-ASCII**                 | `ASCII`                |
| **LATIN2**               | Central/Eastern European (ISO 8859-2).             | **ISO-8859-2**               | `ISO8859_2`            |
| **EUC-JP** (or `ujis`)   | Extended Unix Code for Japanese.                   | **EUC-JP**                   | `EUC_JP`               |
| **SJIS** (or `cp932`)    | Shift JIS for Japanese.                            | **Shift_JIS**                | `SJIS`                 |
| **GBK** (or `gb2312`)    | Simplified Chinese standards.                      | **GBK**                      | `GB2312`               |
| **KOI8-R**               | Russian Cyrillic.                                  | **KOI8-R**                   | `KOI8_R`               |
| **Windows-1252**         | Windows Latin 1 (Superset of Latin1).              | **Windows-1252**             | `Cp1252`               |

---

## Key Mapping Principles for JDBC Drivers

1.  ### Unicode (UTF-8) is Critical
  * **SQL Standard/Modern DBs:** Modern applications and systems (including Parquet/Avro) rely on **UTF-8** (`utf8mb4` in MySQL, `AL32UTF8` in Oracle).
  * **Java's Internal String:** Remember that a Java `String` object internally uses **UTF-16** encoding (specifically the big-endian `UTF-16BE` or `UTF-16`). The JDBC driver's job is always to convert between the **database encoding** (e.g., UTF-8) and the **Java internal `String` encoding** (UTF-16).

2.  ### Use Canonical Java Names
  * When calling Java's character set conversion functions (`Charset.forName()` or `String.getBytes()`), always use the **canonical names** (e.g., `UTF-8`, `ISO-8859-1`) to ensure compatibility across different Java Virtual Machines (JVMs).

3.  ### Handling Aliases
  * Databases often use proprietary names (e.g., MySQL uses `utf8mb4` to represent 4-byte UTF-8, while the standard Java name is simply `UTF-8`). Your driver needs to manage these **aliases** by mapping the incoming SQL name to the correct Java canonical name.

4.  ### Case Insensitivity
  * SQL character set names are typically **case-insensitive**, while Java's `Charset.forName()` method accepts both canonical names and case-insensitive aliases. It's best practice to normalize the SQL input (e.g., convert to uppercase) before looking up the Java name.