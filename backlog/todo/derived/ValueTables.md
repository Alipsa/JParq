**Please add support for value tables as described in the SQL standard.**
- Support value tables construction relevant to read-only operations. Examples of value table constructors include:
  - VALUES (1, 'A'), (2, 'B'), (3, 'C')
  - SELECT * FROM (VALUES (1, 'A'), (2, 'B')) AS t(id, name)
  - These constructs allow you to create inline tables of literal values without needing to reference existing database tables.
  - Value table constructors are useful for testing, temporary data sets, or when you need a quick set of values for joins or comparisons within a query.
  - The SQL standard defines the syntax and semantics for value table constructors, making them a recognized feature for read-only queries.
- Standard Substitute for Temp Tables: It acts as the SQL standard replacement for temporary tables in contexts where creating and populating physical temp tables (which are write operations) would be forbidden. This allows users to test data, create lookup lists, and structure complex query logic without violating the read-only constraint.
- Facilitates Complex Joins: It enables the client application to cleanly pass a small, dynamic list of values to the database to be joined or filtered against large, existing tables.

# Example:
SELECT t.customer_name, lookup.category_name
FROM Customers t
JOIN (
    VALUES (1, 'Premium'), (2, 'Standard')
) AS lookup (customer_type_id, category_name)  -- VALUES creates the lookup data
  ON t.customer_type_id = lookup.customer_type_id;

# Important!
- Create test to verify the functionality in a test class called jparq.derived.ValueTablesTest
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true test` to ensure that there is no regression.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
