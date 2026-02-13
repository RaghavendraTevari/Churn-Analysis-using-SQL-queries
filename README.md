# Churn-Analysis-using-SQL-queries
## 1. Identifying User Activity Status
First, we use the LAG() function to see when each user was last active relative to their current record.
```sql
WITH user_monthly_status AS (
  SELECT 
    user_id,
    active_month,
    -- Get the previous month this specific user was active
    LAG(active_month) OVER (PARTITION BY user_id ORDER BY active_month) AS prev_active_month
  FROM (
    -- Ensure we have unique user-month entries
    SELECT DISTINCT user_id, active_month FROM user_activity
  )
)
SELECT * FROM user_monthly_status;
```
----
## 2. Calculating Monthly Retention Rate
 A user is "Retained" if the difference between their active_month and prev_active_month is exactly 1 month.
```sql
WITH activity_lag AS (
  SELECT 
    user_id,
    active_month,
    LAG(active_month) OVER (PARTITION BY user_id ORDER BY active_month) AS prev_active_month
  FROM (SELECT DISTINCT user_id, active_month FROM user_activity)
),
retention_logic AS (
  SELECT 
    active_month,
    user_id,
    CASE 
      WHEN DATE_DIFF(active_month, prev_active_month, MONTH) = 1 THEN 1 
      ELSE 0 
    END AS is_retained
  FROM activity_lag
)
SELECT 
  active_month,
  SUM(is_retained) AS retained_users,
  COUNT(DISTIONAL user_id) AS total_active_users,
  ROUND(SUM(is_retained) / COUNT(DISTINCT user_id), 2) AS retention_rate
FROM retention_logic
GROUP BY active_month
ORDER BY active_month;
```

## 3. Calculating Monthly Churn Rate
Churn is trickier with window functions because "Churn" is the absence of a record in the current month. We usually calculate this by identifying the "Last Month Active" for a user and marking them as churned in the following month.
```sql
WITH user_activity_window AS (
  SELECT 
    user_id,
    active_month,
    -- Find the next time the user appears
    LEAD(active_month) OVER (PARTITION BY user_id ORDER BY active_month) AS next_active_month
  FROM (SELECT DISTINCT user_id, active_month FROM user_activity)
),
churn_logic AS (
  SELECT 
    -- If there is no "next month", they churned AFTER this month
    DATE_ADD(active_month, INTERVAL 1 MONTH) AS churn_month,
    user_id
  FROM user_activity_window
  WHERE DATE_DIFF(next_active_month, active_month, MONTH) > 1 
     OR next_active_month IS NULL
)
SELECT 
  churn_month,
  COUNT(user_id) AS churned_user_count
FROM churn_logic
GROUP BY churn_month
ORDER BY churn_month;
```
## 1. Stored Procedure for Monthly Retention 
 This procedure calculates how many users from a starting month returned in a subsequent target month. Encapsulating this logic as a stored procedure allows for reuse across different reports. 
```sql
CREATE PROCEDURE GetMonthlyRetention (
    @StartMonth DATE, 
    @TargetMonth DATE
)
AS
BEGIN
    SET NOCOUNT ON; -- Improves performance by skipping row count messages
    
    WITH StartUsers AS (
        SELECT DISTINCT user_id 
        FROM user_activity 
        WHERE active_month = @StartMonth
    ),
    TargetUsers AS (
        SELECT DISTINCT user_id 
        FROM user_activity 
        WHERE active_month = @TargetMonth
    )
    SELECT 
        COUNT(s.user_id) AS initial_users,
        COUNT(t.user_id) AS retained_users,
        ROUND(CAST(COUNT(t.user_id) AS FLOAT) / NULLIF(COUNT(s.user_id), 0), 4) * 100 AS retention_rate
    FROM StartUsers s
    LEFT JOIN TargetUsers t ON s.user_id = t.user_id;
END;
```
Execution: EXEC GetMonthlyRetention '2023-01-01', '2023-02-01'; 
## 2. Stored Procedure for Monthly Churn
 This procedure identifies users who were present in the previous month but did not show up in the current month. 
```sql
CREATE PROCEDURE GetMonthlyChurn (
    @ReferenceMonth DATE
)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @PreviousMonth DATE = DATEADD(MONTH, -1, @ReferenceMonth);

    WITH PrevMonthUsers AS (
        SELECT DISTINCT user_id FROM user_activity WHERE active_month = @PreviousMonth
    ),
    CurrMonthUsers AS (
        SELECT DISTINCT user_id FROM user_activity WHERE active_month = @ReferenceMonth
    )
    SELECT 
        COUNT(p.user_id) AS starting_users,
        COUNT(p.user_id) - COUNT(c.user_id) AS churned_users,
        ROUND(CAST(COUNT(p.user_id) - COUNT(c.user_id) AS FLOAT) / NULLIF(COUNT(p.user_id), 0), 4) * 100 AS churn_rate
    FROM PrevMonthUsers p
    LEFT JOIN CurrMonthUsers c ON p.user_id = c.user_id
    WHERE c.user_id IS NULL; -- Only count users who didn't return
END;
```
```sql
WITH user_history AS (
    SELECT 
        user_id,
        active_month,
        -- Get the month of the user's first ever activity
        MIN(active_month) OVER (PARTITION BY user_id) as first_active_month,
        -- Get the date of their immediate previous activity
        LAG(active_month) OVER (PARTITION BY user_id ORDER BY active_month) as prev_active_month
    FROM (SELECT DISTINCT user_id, active_month FROM user_data_large)
),
status_logic AS (
    SELECT 
        user_id,
        active_month,
        CASE 
            WHEN active_month = first_active_month THEN 'New'
            WHEN DATEDIFF(month, prev_active_month, active_month) = 1 THEN 'Retained'
            WHEN DATEDIFF(month, prev_active_month, active_month) > 1 THEN 'Resurrected'
            ELSE 'Churned/Unknown' 
        END AS user_status
    FROM user_history
)
SELECT 
    active_month,
    user_status,
    COUNT(user_id) as user_count
FROM status_logic
GROUP BY active_month, user_status
ORDER BY active_month, user_status;
```
