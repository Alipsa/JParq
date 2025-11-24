WITH LatestSalary AS (
    SELECT
        employee,
        salary,
        change_date,
        -- Window Function #1: Partition by employee, order by date desc to find the newest entry
        ROW_NUMBER() OVER (PARTITION BY employee ORDER BY change_date DESC) as rn
    FROM salary
),
     EmployeeData AS (
         SELECT
             e.first_name,
             e.last_name,
             d.department,
             ls.salary,
             ls.change_date
         FROM LatestSalary ls
                  -- JOINS: Linking Salary -> Employee -> Link Table -> Department
                  JOIN employees e ON ls.employee = e.id
                  JOIN employee_department ed ON e.id = ed.employee
                  JOIN departments d ON ed.department = d.id
         WHERE ls.rn = 1 -- Filter to keep only the current salary
     )
SELECT
    department,
    first_name || ' ' || last_name AS full_name,
    salary,

    -- Window Function #2: Calculate Average Salary for the specific Department
    CAST(AVG(salary) OVER (PARTITION BY department) AS INT) as avg_dept_salary,

    -- Calculation: How much more/less than average does this person make?
    salary - CAST(AVG(salary) OVER (PARTITION BY department) AS INT) as diff_from_avg,

    -- Window Function #3: Rank employees within their specific department
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank_in_dept

FROM EmployeeData
ORDER BY department, salary DESC;