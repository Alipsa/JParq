# Please add support for using || for string concatenations

The double pipe || is defined in the SQL-92 standard.

Syntax: string_a || string_b

Philosophy: It acts like a mathematical operator.

1. The "Null Propagation" Rule
This is the most common area where implementations deviate from the standard.
Requirement: If any operand in the concatenation chain is NULL, the result must be NULL.
Compliant Behavior: 'Hello' || NULL -> NULL
Non-Compliant Behavior: 'Hello' || NULL -> 'Hello' (Treating NULL as an empty string).
Why: The SQL standard treats NULL as "unknown." 
If you add an unknown value to a known string, the resulting string is theoretically unknown.

2. Supported Data Types
The operator must support concatenation for two specific categories of data types:

Character Strings: CHAR, VARCHAR, CLOB (Character Large Object).

Binary Strings: BINARY, VARBINARY, BLOB (Binary Large Object).

Requirement: The implementation must allow String || String resulting in a String, and Binary || Binary resulting in Binary. 
It is desirable to support String || Number implicitly (e.g., 'Age: ' || 25).

3. Result Length and Type Derivation
The implementation must calculate the resulting data type based on the input operands:

Length: The length of the result must be the sum of the lengths of the operands.

If you join VARCHAR(10) and VARCHAR(20), the result type should conceptually be VARCHAR(30).

Fixed vs. Variable:

CHAR(A) || CHAR(B) usually results in a VARCHAR(A+B) or CHAR(A+B) depending on specific version nuances, but it strictly must not trim trailing whitespace from CHAR types during the operation.

If ColumnA is CHAR(5) containing 'Hi ' (3 spaces), and ColumnB is 'There', the result must preserve those spaces: 'Hi There'.

4. Collation Handling
If the operands have different collations (e.g., one is Latin1_General_CI_AS and the other is UTF8_General_CI), the standard dictates specific "Coercibility Rules":

Explicit Collation: If one operand has an explicit COLLATE clause, that collation wins.

No Explicit Collation: If they conflict and neither is explicit, the operation should behave as similarly to String.valueOf(columnA) + String.valueOf(columnB).

5. Operator Syntax and Precedence
Symbol: The operator must be recognized as the double pipe ||.

Infix Notation: It must be placed between operands (A || B).

Precedence: It generally shares precedence with additive operators, but implementations must ensure it parses correctly within complex expressions involving math or logical operators.