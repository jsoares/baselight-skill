# DuckDB SQL Patterns for Baselight

Reference for common query patterns. All examples use the `"@user.dataset.table"` format.
Adapt column names based on what `get_table_metadata` returns.

## Table of Contents

1. Basic Queries
2. Aggregation
3. Filtering and Conditions
4. Joins
5. Time Series
6. Date Functions
7. Conditional Logic
8. Window Functions
9. String and Type Operations
10. Subqueries and CTEs
11. Real-World Examples

---

## 1. Basic Queries

### Preview rows
```sql
SELECT * FROM "@user.dataset.table" LIMIT 10
```

### Select specific columns
```sql
SELECT column_a, column_b, column_c
FROM "@user.dataset.table"
LIMIT 100
```

### Distinct values
```sql
SELECT DISTINCT category
FROM "@user.dataset.table"
ORDER BY category
```

### Count rows
```sql
SELECT COUNT(*) AS total_rows
FROM "@user.dataset.table"
```

---

## 2. Aggregation

### Group by with aggregates
```sql
SELECT
  category,
  COUNT(*) AS count,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  SUM(amount) AS total_amount
FROM "@user.dataset.table"
GROUP BY category
ORDER BY count DESC
```

### HAVING for filtered groups
```sql
SELECT
  category,
  COUNT(*) AS count
FROM "@user.dataset.table"
GROUP BY category
HAVING COUNT(*) > 10
ORDER BY count DESC
```

### Percentiles
```sql
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) AS p95_price
FROM "@user.dataset.table"
```

---

## 3. Filtering and Conditions

### WHERE clauses
```sql
SELECT *
FROM "@user.dataset.table"
WHERE status = 'active'
  AND created_at >= '2024-01-01'
  AND amount BETWEEN 100 AND 1000
LIMIT 50
```

### IN lists
```sql
SELECT *
FROM "@user.dataset.table"
WHERE category IN ('crypto', 'defi', 'nft')
LIMIT 50
```

### Pattern matching
```sql
SELECT *
FROM "@user.dataset.table"
WHERE name ILIKE '%bitcoin%'
LIMIT 50
```

### NULL handling
```sql
SELECT *
FROM "@user.dataset.table"
WHERE email IS NOT NULL
  AND last_login IS NULL
LIMIT 50
```

---

## 4. Joins

### Join two tables in the same dataset
```sql
SELECT
  a.id,
  a.name,
  b.transaction_date,
  b.amount
FROM "@user.dataset.table_a" a
JOIN "@user.dataset.table_b" b
  ON a.id = b.user_id
LIMIT 100
```

### Join across datasets
```sql
SELECT
  t1.symbol,
  t1.price,
  t2.market_cap
FROM "@alice.crypto_prices.daily" t1
JOIN "@bob.market_data.caps" t2
  ON t1.symbol = t2.symbol
  AND t1.date = t2.date
LIMIT 100
```

### Left join (preserve all rows from left table)
```sql
SELECT
  a.*,
  b.score
FROM "@user.dataset.users" a
LEFT JOIN "@user.dataset.scores" b
  ON a.id = b.user_id
LIMIT 100
```

---

## 5. Time Series

### Daily aggregation
```sql
SELECT
  DATE_TRUNC('day', timestamp_col) AS day,
  COUNT(*) AS events,
  SUM(value) AS total_value
FROM "@user.dataset.table"
WHERE timestamp_col >= '2024-01-01'
GROUP BY 1
ORDER BY 1
```

### Monthly aggregation
```sql
SELECT
  DATE_TRUNC('month', timestamp_col) AS month,
  COUNT(*) AS events
FROM "@user.dataset.table"
GROUP BY 1
ORDER BY 1
```

### Date range with interval
```sql
SELECT *
FROM "@user.dataset.table"
WHERE timestamp_col >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY timestamp_col DESC
LIMIT 100
```

### Day-over-day change
```sql
SELECT
  day,
  value,
  value - LAG(value) OVER (ORDER BY day) AS daily_change,
  ROUND(100.0 * (value - LAG(value) OVER (ORDER BY day)) / LAG(value) OVER (ORDER BY day), 2) AS pct_change
FROM (
  SELECT
    DATE_TRUNC('day', timestamp_col) AS day,
    SUM(value) AS value
  FROM "@user.dataset.table"
  GROUP BY 1
)
ORDER BY day
```

---

## 6. Date Functions

### EXTRACT (pull date parts)
```sql
SELECT
  EXTRACT(year FROM timestamp_col) AS year,
  EXTRACT(month FROM timestamp_col) AS month,
  EXTRACT(day FROM timestamp_col) AS day
FROM "@user.dataset.table"
ORDER BY year, month, day
LIMIT 100
```

### CAST to DATE (strip time from timestamp)
```sql
SELECT
  CAST(timestamp_col AS DATE) AS date_only
FROM "@user.dataset.table"
LIMIT 100
```

---

## 7. Conditional Logic

### CASE WHEN (categorize data)
```sql
SELECT
  country,
  population,
  CASE
    WHEN population >= 100000000 THEN 'Large'
    WHEN population >= 10000000 THEN 'Medium'
    ELSE 'Small'
  END AS country_size
FROM "@user.dataset.table"
WHERE year = 2023
```

### CASE WHEN in aggregation
```sql
SELECT
  COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_count,
  COUNT(CASE WHEN status = 'inactive' THEN 1 END) AS inactive_count,
  COUNT(*) AS total
FROM "@user.dataset.table"
```

---

## 8. Window Functions

### Ranking
```sql
SELECT
  name,
  score,
  RANK() OVER (ORDER BY score DESC) AS rank,
  ROW_NUMBER() OVER (ORDER BY score DESC) AS row_num
FROM "@user.dataset.table"
LIMIT 20
```

### Running totals
```sql
SELECT
  date,
  amount,
  SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total
FROM "@user.dataset.table"
ORDER BY date
LIMIT 100
```

### Partition-level ranking (e.g., top N per group)
```sql
SELECT * FROM (
  SELECT
    category,
    name,
    score,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS rn
  FROM "@user.dataset.table"
)
WHERE rn <= 5
```

---

## 9. String and Type Operations

### Cast types
```sql
SELECT
  CAST(price_str AS DOUBLE) AS price,
  CAST(date_str AS DATE) AS date_val
FROM "@user.dataset.table"
LIMIT 10
```

### String functions
```sql
SELECT
  LOWER(name) AS name_lower,
  LENGTH(description) AS desc_length,
  SUBSTR(code, 1, 3) AS code_prefix
FROM "@user.dataset.table"
LIMIT 10
```

### Coalesce (default for NULLs)
```sql
SELECT
  name,
  COALESCE(email, 'no-email') AS email
FROM "@user.dataset.table"
LIMIT 10
```

---

## 10. Subqueries and CTEs

### Common Table Expression (CTE)
```sql
WITH daily_totals AS (
  SELECT
    DATE_TRUNC('day', created_at) AS day,
    SUM(amount) AS total
  FROM "@user.dataset.transactions"
  GROUP BY day
)
SELECT
  day,
  total,
  AVG(total) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg
FROM daily_totals
ORDER BY day
```

### Subquery in WHERE
```sql
SELECT *
FROM "@user.dataset.table"
WHERE category IN (
  SELECT category
  FROM "@user.dataset.table"
  GROUP BY category
  HAVING COUNT(*) > 100
)
LIMIT 50
```

---

## 11. Real-World Examples

These use actual public datasets available on Baselight. Always verify column names
with `get_table_metadata` before running — schemas may change over time.

### Cross-dataset join: happiness and country details
```sql
SELECT
  a.country,
  a.population,
  b.gdp
FROM "@owid.happiness.owid_happiness_2" a
INNER JOIN "@kaggle.adityakishor1_all_countries_details.all_countries" b
  ON a.country = b.country
WHERE a.year = 2023
ORDER BY a.population DESC
LIMIT 20
```

### DeFi: daily swap volume with CTE
```sql
WITH daily_volume AS (
  SELECT
    CAST(time AS DATE) AS date,
    SUM(inputamount + outputamount) AS total_volume
  FROM "@portals.transactions.swaps"
  WHERE inputtoken LIKE 'ethereum:%'
  GROUP BY date
)
SELECT *
FROM daily_volume
ORDER BY date DESC
LIMIT 30
```

### Happiness rankings by year using window functions
```sql
SELECT
  country,
  year,
  ROW_NUMBER() OVER (
    PARTITION BY year ORDER BY cantril_ladder_score DESC
  ) AS happiness_rank
FROM "@owid.happiness.owid_happiness_2"
ORDER BY country, year
LIMIT 100
```

---

## Reminders

- Always double-quote table identifiers: `"@user.dataset.table"`
- No semicolons at the end
- Use LIMIT, not TOP
- SELECT only — no DDL or DML
- Check column names via `get_table_metadata` before writing queries
