-- Cleaned Campaign Dictionary
CREATE OR REPLACE TABLE SANDBOX.CAMPAIGNS_DICTIONARY_CLEANED AS
WITH cleaned_campaigns AS (
    SELECT
        REPLACE(LOWER(TRIM(campaign_name)), ' ', '') AS campaign_name,
        REPLACE(LOWER(TRIM(page_url)), ' ', '') AS page_url
    FROM SANDBOX.CAMPAIGNS_DICTIONARY
)
SELECT * FROM cleaned_campaigns;

-- Cleaned Page Visits
CREATE OR REPLACE TABLE SANDBOX.PAGE_VISITS_CLEANED AS
WITH cleaned_page_visits AS (
    SELECT
        REPLACE(LOWER(TRIM(page_url)), ' ', '') AS page_url,
        TO_DATE(SUBSTR(date, 1, 4) || '-' || SUBSTR(date, 5, 2) || '-' || SUBSTR(date, 7, 2), 'YYYY-MM-DD') AS date,
        REPLACE(REPLACE(LOWER(TRIM(page_views)), ' ', ''), ',', '')::INT AS page_views,
        page_avg_time,
        REPLACE(TRIM(page_bounce_rate), '%', '')::FLOAT / 100 AS page_bounce_rate
    FROM SANDBOX.PAGE_VISITS
)
SELECT * FROM cleaned_page_visits;

-- Cleaned Leads
CREATE OR REPLACE TABLE SANDBOX.LEADS_CLEANED AS
WITH cleaned_leads AS (
    SELECT
        REPLACE(LOWER(TRIM(LEAD_HASHED_ID)), ' ', '') AS lead_hashed_id,
        campaign_joined_date,
        -- Clean Job Titles
        CASE
            WHEN LOWER(TRIM(LEAD_JOB_TITLE)) IN ('', 'none', 'n/a', 'a', 'test', 'unknown') 
                OR LENGTH(TRIM(LEAD_JOB_TITLE)) = 1
                OR REGEXP_LIKE(LEAD_JOB_TITLE, '^[\W\d]+$') THEN 'unknown'
            ELSE LOWER(TRIM(LEAD_JOB_TITLE))
        END AS job_title_cleaned,
        LOWER(TRIM(LEAD_INDUSTRY)) AS lead_industry,
        TRIM(LEAD_COUNTRY) AS country,
        -- Region Mapping
        CASE 
            WHEN country IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN country IN ('BR', 'AR', 'CL') THEN 'South America'
            WHEN country IN ('DE', 'FR', 'IT') THEN 'Europe'
            WHEN country IS NULL THEN 'Unknown'
            ELSE 'Other'
        END AS region,
        -- Campaign Name
        REPLACE(LOWER(TRIM(campaign_name)), ' ', '') AS campaign_name,
        LOWER(TRIM(LEAD_SOURCE)) AS source,
        -- Source Category
        CASE
            WHEN LOWER(LEAD_SOURCE) LIKE '%paid%' THEN 'paid'
            WHEN LOWER(LEAD_SOURCE) LIKE '%organic%' THEN 'organic'
            WHEN LEAD_SOURCE IS NULL THEN 'unknown'
            ELSE LOWER(LEAD_SOURCE)
        END AS source_category,
        REPLACE(LOWER(TRIM(LEAD_STATUS)), ' ', '') AS lead_status
    FROM SANDBOX.LEADS
)
SELECT * FROM cleaned_leads;
