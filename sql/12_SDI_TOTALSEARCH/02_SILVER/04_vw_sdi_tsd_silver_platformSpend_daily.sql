/* =================================================================================================
FILE: 04_vw_sdi_tsd_silver_platformSpend_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_platformSpend_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_platformSpend_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily

PURPOSE:
  Canonical Silver platform spend daily source mart for the Total Search Dashboard.
  This view maps raw spend channels into the conformed dashboard channel set
  and aggregates spend at the reporting grain:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

OUTPUT METRICS:
  - platform_spend

KEY MODELING NOTES:
  - Bronze preserves source truth in channel_raw
  - NATURAL SEARCH is standardized to ORGANIC SEARCH
  - Paid search child channels are rolled up into PAID SEARCH
  - Remaining unexpected channels are collapsed into OTHER CAMPAIGNS

CONFORMED CHANNEL OUTPUT SET:
  AFFILIATE
  DIRECT
  DIRECT MAIL
  DIRECT TV
  DISPLAY
  EMAIL - CAMPAIGN
  EMAIL - ORGANIC
  ORGANIC SEARCH
  ON DEVICE
  ONLINE VIDEO
  OTHER CAMPAIGNS
  OUT OF HOME
  PAID SEARCH
  REFERRING DOMAINS
  RETAIL STORE
  SESSION REFRESH
  SMS
  SOCIAL NETWORK - CAMPAIGN
  SOCIAL NETWORK - NATURAL

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily`
AS

WITH mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel_raw)) AS channel_raw,

        CASE
            /* -------------------------------------------------------------------------------------------------
               1) DIRECT 1:1 MAPPINGS TO FINAL CHANNELS
               ------------------------------------------------------------------------------------------------- */
            WHEN UPPER(TRIM(channel_raw)) = 'AFFILIATE' THEN 'AFFILIATE'
            WHEN UPPER(TRIM(channel_raw)) = 'DIRECT' THEN 'DIRECT'
            WHEN UPPER(TRIM(channel_raw)) = 'DIRECT MAIL' THEN 'DIRECT MAIL'
            WHEN UPPER(TRIM(channel_raw)) = 'DIRECT TV' THEN 'DIRECT TV'
            WHEN UPPER(TRIM(channel_raw)) = 'DISPLAY' THEN 'DISPLAY'
            WHEN UPPER(TRIM(channel_raw)) = 'EMAIL - CAMPAIGN' THEN 'EMAIL - CAMPAIGN'
            WHEN UPPER(TRIM(channel_raw)) = 'EMAIL - ORGANIC' THEN 'EMAIL - ORGANIC'
            WHEN UPPER(TRIM(channel_raw)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            WHEN UPPER(TRIM(channel_raw)) = 'ON DEVICE' THEN 'ON DEVICE'
            WHEN UPPER(TRIM(channel_raw)) = 'ONLINE VIDEO' THEN 'ONLINE VIDEO'
            WHEN UPPER(TRIM(channel_raw)) = 'OTHER CAMPAIGNS' THEN 'OTHER CAMPAIGNS'
            WHEN UPPER(TRIM(channel_raw)) = 'OUT OF HOME' THEN 'OUT OF HOME'
            WHEN UPPER(TRIM(channel_raw)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel_raw)) = 'REFERRING DOMAINS' THEN 'REFERRING DOMAINS'
            WHEN UPPER(TRIM(channel_raw)) = 'RETAIL STORE' THEN 'RETAIL STORE'
            WHEN UPPER(TRIM(channel_raw)) = 'SESSION REFRESH' THEN 'SESSION REFRESH'
            WHEN UPPER(TRIM(channel_raw)) = 'SMS' THEN 'SMS'
            WHEN UPPER(TRIM(channel_raw)) = 'SOCIAL NETWORK - CAMPAIGN' THEN 'SOCIAL NETWORK - CAMPAIGN'
            WHEN UPPER(TRIM(channel_raw)) = 'SOCIAL NETWORK - NATURAL' THEN 'SOCIAL NETWORK - NATURAL'

            /* -------------------------------------------------------------------------------------------------
               2) COLLAPSE SPEND-ONLY CHANNELS INTO EXISTING FINAL CHANNELS
               ------------------------------------------------------------------------------------------------- */
            WHEN UPPER(TRIM(channel_raw)) = 'PROGRAMMATIC DISPLAY' THEN 'DISPLAY'
            WHEN UPPER(TRIM(channel_raw)) = 'CONTENT SYNDICATION' THEN 'DISPLAY'

            WHEN UPPER(TRIM(channel_raw)) = 'OVER THE TOP' THEN 'ONLINE VIDEO'

            WHEN UPPER(TRIM(channel_raw)) IN (
                'BROADCAST TV',
                'CABLE TV',
                'LIVE SPORTS TV',
                'LOCAL TV',
                'SL TV'
            ) THEN 'DIRECT TV'

            WHEN UPPER(TRIM(channel_raw)) IN (
                'STREAMING RADIO',
                'PODCAST',
                'SUPER BOWL',
                'TUESDAYS',
                'OFFLINE',
                '? TFB _EFL_ _SLN_ ?'
            ) THEN 'OTHER CAMPAIGNS'

            /* -------------------------------------------------------------------------------------------------
               3) ANY REMAINING / UNKNOWN CHANNELS
               ------------------------------------------------------------------------------------------------- */
            ELSE 'OTHER CAMPAIGNS'
        END AS channel,

        COALESCE(spend, 0) AS spend
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_platformSpend_daily`
)

SELECT
    event_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,
    SUM(COALESCE(spend, 0)) AS platform_spend
FROM mapped
GROUP BY
    event_date,
    lob,
    channel;