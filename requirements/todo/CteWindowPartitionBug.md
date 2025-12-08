The jparq.usage.AcmeTest.testLatestPerGroupUsingWindow() fails. The issue looks like to occur when using a combination of a CTE, a window function with a PARTITION BY clause, and a JOIN on the CTE. 
The parser seems to be unable to handle this specific combination of SQL features.
It is possible that this is due to a bug in the jsqlparser library, which maybe fails to parse a query that uses a Common Table Expression (CTE) with a PARTITION BY clause in a JOIN

1. Validate that the test case is correct i.e. that the sql in the test case is valid against the schema for the employees and salary table in the acme dataset.
2. If the test is valid then investigate if the suspicion about a bug is true by creating a simpler test that shows the issue.

If it is a jsqlparser library bug then
- implement a workaround within the JParq codebase. by rewriting the SQL query to use a derived table (a subquery in the FROM clause) instead of a CTE. This will produce the same result but avoid the parsing bug in JSqlParser.
- create a Bug Report: In parallel with implementing a workaround, create a bug report in the form of a markdown file that we can submit to the JSqlParser project. The minimal, reproducible test case created will be invaluable in helping them to identify and fix the issue in their library.

If it is not a jsqlparser issue then investigate the cause and create a plan for how to fix the issue.