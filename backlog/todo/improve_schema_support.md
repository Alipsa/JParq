Please improve SCHEMA handling as follows:

# Introduce a default schema
According to the SQL standard, schema can never be null. A table must belong to a schema. Hence we need to introduce 
the concept of a default schema.
Parquet files (tables) residing in the base directory should get the schema "PUBLIC" (today the schema is null)

# User defined schemas
Immediate subdirectories under the base dir should denote the schema name
Example:
I have the following files
/project/acme/employees.parquet
/project/acme/salary.parquet
/project/acme/cars/mtcars.parquet
/project/acme/cars/cars.parquet

The following connection jdbc:jparq:/project/acme

Would give me the following INFORMATION_SCHEMA.TABLES

| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | TABLE_TYPE | REMARKS |
|---------------|--------------|------------|------------|---------|
| acme          | PUBLIC       | salary     | BASE TABLE | null    |
| acme          | PUBLIC       | employees  | BASE TABLE | null    |
| acme          | CARS         | mtcars     | BASE TABLE | null    |
| acme          | CARS         | cars       | BASE TABLE | null    |

Directories nested at a 3:rd level e.g. /project/acme/cars/foo/diamonds.parquet
should be ignored.

This should also be reflected in INFORMATION_SCHEMA.COLUMNS, DataBaseMetaData, and ResultSetMetaData

The PUBLIC schema should be default i.e. if no schema is specified in a query then PUBLIC is assumed.

## Case Sensitivity:

If we have a folder named cars (lowercase), but our SQL is SELECT * FROM CARS.mtcars, 
ensure the schema resolution is case-insensitive (standard for SQL) unless identifiers are quoted.

In INFORMATION_SCHEMA, it is conventional to return TABLE_SCHEMA as uppercase (CARS, PUBLIC) unless 
strictly case-sensitive identifiers are set with caseSensitive=true e.g: jdbc:jparq:/home/user/data?caseSensitive=true

## The "PUBLIC" Folder Collision:

What happens if the user creates a physical folder named /project/acme/public/?

Recommendation: Merge them. The logical default PUBLIC schema and the physical public folder should effectively map to the same namespace.

## Default Schema Logic:

When a query SELECT * FROM employees comes in, the query parser must implicitly resolve this to PUBLIC.employees. 
if there are multiple tables in different schemas with the same name then using no schema denominator should 
still refer to PUBLIC. Example:
- Scenario: You have sales.employees and PUBLIC.employees.
- Query: SELECT * FROM employees
- Result: It returns data from PUBLIC.employees.
- To get the other one: The user must explicitly type SELECT * FROM sales.employees.