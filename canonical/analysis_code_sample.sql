WITH page_visits AS (
    SELECT
        page_url,
        SUM(page_views) AS total_page_views,
        AVG(
            EXTRACT(HOUR FROM page_avg_time) * 3600 +
            EXTRACT(MINUTE FROM page_avg_time) * 60 +
            EXTRACT(SECOND FROM page_avg_time)
        ) AS avg_time_in_seconds,
        AVG(page_bounce_rate) AS avg_bounce_rate
    FROM SANDBOX.PAGE_VISITS_CLEANED
    WHERE page_url IS NOT NULL -- Exclude summary rows
    GROUP BY ALL
),

leads AS (
    SELECT
        campaign_name,
        COUNT(DISTINCT lead_hashed_id) AS total_leads,
        COUNT(DISTINCT CASE WHEN lead_status = 'mql' THEN lead_hashed_id END) AS total_mqls,
        COUNT(DISTINCT CASE WHEN lead_status = 'sql' THEN lead_hashed_id END) AS total_sqls,
        COUNT(DISTINCT CASE WHEN lead_status = 'opportunity' THEN lead_hashed_id END) AS total_opps
    FROM SANDBOX.LEADS_CLEANED
    GROUP BY ALL
)

SELECT
    pv.page_url,
    dc.campaign_name,
    pv.total_page_views,
    pv.avg_time_in_seconds,
    pv.avg_bounce_rate,
    l.total_leads,
    l.total_mqls,
    l.total_sqls,
    l.total_opps
FROM page_visits AS pv
LEFT JOIN SANDBOX.CAMPAIGNS_DICTIONARY_CLEANED AS dc
    ON pv.page_url = dc.page_url
LEFT JOIN leads AS l
    ON dc.campaign_name = l.campaign_name
ORDER BY pv.page_url, dc.campaign_name;
