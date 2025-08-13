-- otter_churn_duckdb.sql
SET VARIABLE v_churn = 'data_raw/oa_churn_requests_sample.csv';
SET VARIABLE v_accts = 'data_raw/oa_account_dimensions_sample.csv';
SET VARIABLE v_act   = 'data_raw/oa_product_activity_sample.csv';

CREATE OR REPLACE VIEW churn AS SELECT * FROM read_csv_auto(getvariable('v_churn'));
CREATE OR REPLACE VIEW accounts AS SELECT * FROM read_csv_auto(getvariable('v_accts'));
CREATE OR REPLACE VIEW activity AS SELECT * FROM read_csv_auto(getvariable('v_act'));

CREATE OR REPLACE TABLE dim_accounts AS
SELECT
  account_id::VARCHAR            AS account_id,   -- ✅ explicit alias
  facility_id::VARCHAR           AS facility_id,
  org_id::VARCHAR                AS org_id,
  parent_account_id::VARCHAR     AS parent_account_id,
  TRY_CAST(created_date AS DATE) AS created_date,
  -- map your real columns to the generic names used later:
  customer_segment               AS segment,
  channel_category               AS market,
  csm_status                     AS onboarding_type
FROM accounts;

CREATE OR REPLACE TABLE churn_events AS
WITH cte AS (
  SELECT
    account_id::VARCHAR                          AS account_id,
    TRY_CAST(churn_date AS DATE)                 AS req_dt,    -- from churn_date column
    COALESCE(churn_primary_reason, churn_secondary_reason) AS reason,  -- map your real cols
    churn_notes                                  AS notes
  FROM churn
)
SELECT
  account_id,
  req_dt                                         AS request_date,
  DATE_TRUNC('month', req_dt)                    AS request_month,
  reason,
  notes
FROM cte;

CREATE OR REPLACE TABLE fct_activity AS
WITH a AS (
  SELECT
      account_id::VARCHAR                       AS account_id,
      TRY_CAST(date AS DATE)                    AS dt,

      -- Normalize all *_usage columns to integers (0/1 or counts)
      COALESCE(CAST(print_usage           AS INTEGER), 0) AS print_usage,
      COALESCE(CAST(pos_usage             AS INTEGER), 0) AS pos_usage,
      COALESCE(CAST(boost_usage           AS INTEGER), 0) AS boost_usage,
      COALESCE(CAST(d2c_usage             AS INTEGER), 0) AS d2c_usage,
      COALESCE(CAST(mercury_usage         AS INTEGER), 0) AS mercury_usage,
      COALESCE(CAST(threepl_usage         AS INTEGER), 0) AS threepl_usage,
      COALESCE(CAST(orderagg_usage        AS INTEGER), 0) AS orderagg_usage,
      COALESCE(CAST(om_usage              AS INTEGER), 0) AS om_usage,
      COALESCE(CAST(basic_insights_usage  AS INTEGER), 0) AS basic_insights_usage,
      COALESCE(CAST(adv_insights_usage    AS INTEGER), 0) AS adv_insights_usage,
      COALESCE(CAST(menu_mgmt_usage       AS INTEGER), 0) AS menu_mgmt_usage
  FROM activity
)
SELECT
    account_id,
    dt,

    -- Total events across all tracked product areas
    (print_usage + pos_usage + boost_usage + d2c_usage + mercury_usage
     + threepl_usage + orderagg_usage + om_usage
     + basic_insights_usage + adv_insights_usage + menu_mgmt_usage) AS events_all,

    -- Breadth: how many distinct product areas had any usage (>0) that day
    ( (print_usage           > 0)::INT
    + (pos_usage             > 0)::INT
    + (boost_usage           > 0)::INT
    + (d2c_usage             > 0)::INT
    + (mercury_usage         > 0)::INT
    + (threepl_usage         > 0)::INT
    + (orderagg_usage        > 0)::INT
    + (om_usage              > 0)::INT
    + (basic_insights_usage  > 0)::INT
    + (adv_insights_usage    > 0)::INT
    + (menu_mgmt_usage       > 0)::INT ) AS categories_used
FROM a
WHERE dt IS NOT NULL;

CREATE OR REPLACE TABLE fct_prechurn AS
SELECT
  c.account_id,
  c.request_date AS churn_dt,

  -- last active day before churn
  MAX(f.dt) FILTER (WHERE f.dt < c.request_date) AS last_active_dt,

  -- activity windows before churn
  SUM(f.events_all) FILTER (WHERE f.dt BETWEEN c.request_date - INTERVAL 7  DAY AND c.request_date - INTERVAL 1 DAY)  AS events_7d,
  SUM(f.events_all) FILTER (WHERE f.dt BETWEEN c.request_date - INTERVAL 30 DAY AND c.request_date - INTERVAL 1 DAY) AS events_30d,

  COUNT(DISTINCT f.dt) FILTER (WHERE f.dt BETWEEN c.request_date - INTERVAL 30 DAY AND c.request_date - INTERVAL 1 DAY) AS active_days_30d,
  MAX(f.categories_used) FILTER (WHERE f.dt BETWEEN c.request_date - INTERVAL 30 DAY AND c.request_date - INTERVAL 1 DAY) AS max_categories_30d
FROM churn_events c
LEFT JOIN fct_activity f
  ON f.account_id = c.account_id
GROUP BY 1,2;

CREATE OR REPLACE TABLE churn_enriched AS
SELECT
  c.account_id                                     AS account_id,   -- qualify
  c.request_date                                   AS churn_dt,     -- keep downstream name
  c.reason,
  d.segment, d.market, d.onboarding_type, d.created_date,
  p.last_active_dt, p.events_7d, p.events_30d, p.active_days_30d, p.max_categories_30d,
 DATEDIFF('day', p.last_active_dt, c.request_date) AS days_since_last_use,
DATEDIFF('day', d.created_date,   c.request_date) AS tenure_days
FROM fct_prechurn   AS p
JOIN churn_events   AS c
  ON p.account_id = c.account_id
 AND p.churn_dt   = c.request_date
LEFT JOIN dim_accounts AS d
  ON d.account_id = c.account_id;  

CREATE OR REPLACE VIEW v_top_reasons AS
SELECT reason, COUNT(*) AS n,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM churn_enriched
GROUP BY 1 ORDER BY n DESC;

CREATE OR REPLACE VIEW v_monthly_churn AS
SELECT DATE_TRUNC('month', churn_dt) AS month, COUNT(*) AS churned
FROM churn_enriched
GROUP BY 1 ORDER BY 1;

CREATE OR REPLACE VIEW v_usage_by_segment AS
SELECT segment,
       ROUND(AVG(events_30d),2) AS avg_events_30d,
       ROUND(AVG(active_days_30d),2) AS avg_active_days_30d,
       ROUND(AVG(days_since_last_use),2) AS avg_days_since_last_use
FROM churn_enriched
GROUP BY 1 ORDER BY 1;

COPY (SELECT * FROM v_monthly_churn) TO 'data_work/kpi_churn_trend.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM v_top_reasons) TO 'data_work/top_reasons.csv' (HEADER, DELIMITER ',');
COPY (
  SELECT segment, reason, COUNT(*) AS n FROM churn_enriched GROUP BY 1,2
) TO 'data_work/reasons_by_segment.csv' (HEADER, DELIMITER ',');

COPY (
  SELECT account_id, segment, market, onboarding_type,
         events_30d, active_days_30d, max_categories_30d,
         days_since_last_use, tenure_days
  FROM churn_enriched
) TO 'data_work/risk_indicators.csv' (HEADER, DELIMITER ',');

SELECT 'Done. CSVs written to data_work/' AS msg;
