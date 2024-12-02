create or replace table SANDBOX.CAMPAIGNS_DICTIONARY (
campaign_name string,
page_url string
);

create or replace table SANDBOX.PAGE_VISITS (
page_url string,
date string,
page_views string,
page_avg_time time,
page_bounce_rate string
);

create or replace table SANDBOX.LEADS (
lead_hashed_id string,
campaign_joined_date date,
lead_job_title string,
lead_industry string,
lead_country string,
campaign_name string,
lead_source string,
lead_status string															
);
