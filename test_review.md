# Test Suite Review

## 1. Overview

This document provides a review of the test suite for the JParq project. JParq is a SQL engine for Parquet files, implemented in Java, that provides a JDBC driver to query Parquet files using SQL.

The test suite is located in `src/test/java` and is structured by SQL feature. The tests are generally well-organized and cover a wide range of SQL features.

## 2. Parser Tests

There are several tests that focus specifically on the SQL parser, such as:

*   `src/test/java/jparq/alias/SqlParserProjectionAliasTest.java`
*   `src/test/java/jparq/cte/SqlParserCteTest.java`

These tests are not redundant with the feature-level tests (e.g., `AliasTest.java`, `CTETest.java`). Instead, they are a good example of testing at different levels:

*   **Unit Tests**: The parser tests are unit tests that test the `SqlParser` class directly. They ensure that the parser can correctly parse different SQL constructs and build the correct abstract syntax tree (AST).

*   **Integration Tests**: The feature tests are integration tests that test the entire query execution pipeline, from parsing to result set generation. They ensure that the SQL features work as expected from the user's perspective.

This multi-layered testing approach is a good practice and should be continued.

## 3. Untested Areas

The following areas have been identified as having gaps in test coverage:

*   **Combinations of SQL Features**: 
While individual SQL features are well-tested, there is a lack of tests for complex queries that combine multiple SQL features (e.g., a query with a CTE, a join, and a window function). 
No tests were found that combine `WITH`, `JOIN`, and `OVER` clauses in a single query.

*   **Helper Classes**: 
The following helper classes in `src/main/java/se/alipsa/jparq/helper` do not have any dedicated unit tests:
    *   `DateTimeExpressions`
    *   `JdbcTypeMapper`
    *   `JParqUtil`
    *   `JsonExpressions`
    *   `LiteralConverter`
    *   `StringExpressions`

*   **Error Handling**: Many tests use `try-catch` blocks with `fail(e)` in the `catch` block. 
This is not ideal as it does not test for specific exceptions. While some tests use `assertThrows`, there is room for improvement.

## 4. Recommendations

Based on this review, the following recommendations are made to improve the test suite:

*   **Add Tests for Combined Features**: Add new tests for complex queries that combine multiple SQL features.

*   **Add Unit Tests for Helper Classes**: Add unit tests for the helper classes to ensure that they are working correctly.

*   **Improve Error Handling Tests**: Replace `try-catch` blocks with `assertThrows` to test for specific exceptions. This will make the tests more robust and easier to read.

*   **Develop a Performance Testing Strategy**: There is a single performance test, `src/test/java/jparq/performance/PredicatePushdownPerfFS.java`, but there is no broader performance testing strategy. Consider adding more performance tests for different types of queries and data sizes.
