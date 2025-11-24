WITH SalaryHistory AS (
    SELECT
        employee,
        salary as current_salary,
        change_date,

        -- Window Function #1: LAG
        -- Look at the row immediately before this one (partitioned by employee, ordered by date)
        LAG(salary) OVER (PARTITION BY employee ORDER BY change_date) as previous_salary,

        -- Window Function #2: ROW_NUMBER
        -- Standard technique to filter for the 'latest' record later
        ROW_NUMBER() OVER (PARTITION BY employee ORDER BY change_date DESC) as rn
    FROM salary
)
SELECT
    d.department,
    concat(e.first_name, ' ', e.last_name) as employee_name,
    sh.current_salary,
    sh.previous_salary,

    -- Calculate Percentage Change: ((New - Old) / Old) * 100
    ROUND(
        (sh.current_salary - sh.previous_salary) * 100.0 / sh.previous_salary,
        2
    ) as pct_increase

FROM SalaryHistory sh
-- JOINS for context
    JOIN employees e ON sh.employee = e.id
    JOIN employee_department ed ON e.id = ed.employee
    JOIN departments d ON ed.department = d.id

WHERE sh.rn = 1 -- Only look at the most recent status
ORDER BY pct_increase DESC NULLS LAST;