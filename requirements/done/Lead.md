**Please add support for the LEAD Window Navigation Function as described in the SQL:2003 standard.**
The LEAD() function is a Window Navigation Function defined in the SQL standard (SQL:2003 and later). It is the counterpart to LAG(), providing access to a value from a row that follows the current row by a specified physical offset within the same partition, based on the ordering of the rows.

LEAD() is invaluable for forecasting, predictive comparisons, and calculating future deltas in sequential data analysis.

This function is essential for looking ahead in a sequence—for example, comparing a current car's price to the price of the next model in a sequential list.

# Key Characteristics According to the SQL Standard:
- Navigation Function: It fetches data from a future row without summarizing data.

# Syntax: LEAD() takes up to three arguments:
LEAD(expression [, offset [, default_value]]) OVER ( [PARTITION BY column(s)] ORDER BY column(s) [ASC|DESC] )

- expression (Mandatory): The column or expression whose value you want to retrieve from the following row.
- offset (Optional): A non-negative integer specifying how many rows forward (down) from the current row to look. If omitted, the default is 1 (the immediately following row).
- default_value (Optional): The value to return if the offset goes beyond the boundary of the partition (i.e., if there is no following row to look ahead to). If omitted, the default is NULL.
- ORDER BY Clause (Mandatory): Required to define a consistent sequence, determining which row is "next."
- PARTITION BY Clause (Optional): If specified, the leading only occurs within that group. The result for the last row of a partition is the default_value.

# Example using the mtcars Dataset: Comparing Horsepower
We'll use LEAD() to compare each car's horsepower (hp) with the hp of the immediately heavier car, based on car weight (wt).

Goal: Calculate the difference in hp between the current car and the car that immediately follows it in the sequence ordered by weight ascending.

```sql
SELECT
    wt,
    hp,
    -- Get the hp value from the row 1 position after the current row
    LEAD(hp, 1, 0) OVER (ORDER BY wt ASC) AS Next_Car_HP,
    -- Calculate the difference (Next Car HP - Current HP)
    LEAD(hp, 1, hp) OVER (ORDER BY wt ASC) - hp AS HP_Difference_To_Next
FROM
    mtcars
ORDER BY
    wt ASC;
```
Example Output (Partial)
```
wt,hp,Next_Car_HP,HP_Difference_To_Next
1.513,91,113,22
1.615,113,66,-47
1.835,66,97,31
1.935,97,109,12
...,...,...,...
4.070,175,264,89
5.424,180,0,-180
```

Explanation of the ResultRow 
- Row 1. (wt 1.513):
   - Next_Car_HP fetches the hp of the next row: 113.
   - HP_Difference_To_Next is $113 - 91 = 22. The next heavier car has 22 more horsepower.
- Row 2 (wt 1.615):
   - Next_Car_HP fetches the hp of the next row: 66.
   - HP_Difference_To_Next is $66 - 113 = -47. The next heavier car has 47 less horsepower.
- Last Row (wt 5.424):
   - Since this is the last row in the set, there is no following row. LEAD(hp, 1, 0) returns the default value of 0.
   - HP_Difference_To_Next is $0 - 180 = -180.

# Important!
- Create test to verify the functionality in a test class called jparq.LateralTest
- Remember to also update javadocs (all classes and methods must have a description, all params must be listed and return and throws specified when appropriate) where needed.
- Adhere to the coding standard defined in checkstyle.xml, pmd-ruleset.xml and spotless-formatting.xml and also pay attention to the design principles of low coupling, high cohesion, clarity and DRY (don't repeat yourself).
- All tests must pass after the implementation using `mvn -Dspotless.check.skip=true verify` to ensure that there is no regression.
- No checkstyle, PMD or Spotless violations shall be present after the implementation.

