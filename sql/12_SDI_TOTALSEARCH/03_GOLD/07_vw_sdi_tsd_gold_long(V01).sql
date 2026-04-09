/* =================================================================================================
FILE: 06_vw_sdi_tsd_gold_long.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_long

PURPOSE:
  Long-format Gold reporting table for the Total Search Dashboard.

OUTPUT COLUMNS:
  - data_source
  - time_granularity
  - time_granularity_type
  - date
  - lob
  - channel
  - metric_name
  - metric_value

KEY MODELING NOTES:
  - Uses NULL-preserving upstream gold views
  - Weekly date is standardized to WEEK ENDING SATURDAY
  - Monthly date is standardized to MONTH END
  - PROFOUND and GOFISH are emitted separately

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_long`
AS

WITH
daily_base AS (
    SELECT
        event_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(adobe_entries AS FLOAT64) AS adobe_entries,
        CAST(adobe_pspv_actuals AS FLOAT64) AS adobe_pspv_actuals,
        CAST(adobe_cart_starts AS FLOAT64) AS adobe_cart_starts,
        CAST(adobe_cart_start_plus AS FLOAT64) AS adobe_cart_start_plus,
        CAST(adobe_cart_checkout_visits AS FLOAT64) AS adobe_cart_checkout_visits,
        CAST(adobe_checkout_review_visits AS FLOAT64) AS adobe_checkout_review_visits,
        CAST(adobe_postpaid_orders_tsr AS FLOAT64) AS adobe_postpaid_orders_tsr,
        CAST(adobe_orders_web_unassisted AS FLOAT64) AS adobe_orders_web_unassisted,
        CAST(adobe_orders_web_assisted AS FLOAT64) AS adobe_orders_web_assisted,
        CAST(adobe_orders_app_unassisted AS FLOAT64) AS adobe_orders_app_unassisted,
        CAST(adobe_orders_app_assisted AS FLOAT64) AS adobe_orders_app_assisted,
        CAST(adobe_orders_web_all AS FLOAT64) AS adobe_orders_web_all,
        CAST(adobe_orders_app_all AS FLOAT64) AS adobe_orders_app_all,
        CAST(adobe_orders_fully_unassisted AS FLOAT64) AS adobe_orders_fully_unassisted,
        CAST(adobe_orders_fully_assisted AS FLOAT64) AS adobe_orders_fully_assisted,
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all,
        CAST(adobe_storelocator_visits AS FLOAT64) AS adobe_storelocator_visits,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all,

        CAST(platform_spend AS FLOAT64) AS platform_spend,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

weekly_sunsat_base AS (
    SELECT
        weekSunToSat AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(adobe_entries AS FLOAT64) AS adobe_entries,
        CAST(adobe_pspv_actuals AS FLOAT64) AS adobe_pspv_actuals,
        CAST(adobe_cart_starts AS FLOAT64) AS adobe_cart_starts,
        CAST(adobe_cart_start_plus AS FLOAT64) AS adobe_cart_start_plus,
        CAST(adobe_cart_checkout_visits AS FLOAT64) AS adobe_cart_checkout_visits,
        CAST(adobe_checkout_review_visits AS FLOAT64) AS adobe_checkout_review_visits,
        CAST(adobe_postpaid_orders_tsr AS FLOAT64) AS adobe_postpaid_orders_tsr,
        CAST(adobe_orders_web_unassisted AS FLOAT64) AS adobe_orders_web_unassisted,
        CAST(adobe_orders_web_assisted AS FLOAT64) AS adobe_orders_web_assisted,
        CAST(adobe_orders_app_unassisted AS FLOAT64) AS adobe_orders_app_unassisted,
        CAST(adobe_orders_app_assisted AS FLOAT64) AS adobe_orders_app_assisted,
        CAST(adobe_orders_web_all AS FLOAT64) AS adobe_orders_web_all,
        CAST(adobe_orders_app_all AS FLOAT64) AS adobe_orders_app_all,
        CAST(adobe_orders_fully_unassisted AS FLOAT64) AS adobe_orders_fully_unassisted,
        CAST(adobe_orders_fully_assisted AS FLOAT64) AS adobe_orders_fully_assisted,
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all,
        CAST(adobe_storelocator_visits AS FLOAT64) AS adobe_storelocator_visits,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all,

        CAST(platform_spend AS FLOAT64) AS platform_spend,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

monthly_base AS (
    SELECT
        DATE_SUB(DATE_ADD(DATE_TRUNC(monthStart, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(adobe_entries AS FLOAT64) AS adobe_entries,
        CAST(adobe_pspv_actuals AS FLOAT64) AS adobe_pspv_actuals,
        CAST(adobe_cart_starts AS FLOAT64) AS adobe_cart_starts,
        CAST(adobe_cart_start_plus AS FLOAT64) AS adobe_cart_start_plus,
        CAST(adobe_cart_checkout_visits AS FLOAT64) AS adobe_cart_checkout_visits,
        CAST(adobe_checkout_review_visits AS FLOAT64) AS adobe_checkout_review_visits,
        CAST(adobe_postpaid_orders_tsr AS FLOAT64) AS adobe_postpaid_orders_tsr,
        CAST(adobe_orders_web_unassisted AS FLOAT64) AS adobe_orders_web_unassisted,
        CAST(adobe_orders_web_assisted AS FLOAT64) AS adobe_orders_web_assisted,
        CAST(adobe_orders_app_unassisted AS FLOAT64) AS adobe_orders_app_unassisted,
        CAST(adobe_orders_app_assisted AS FLOAT64) AS adobe_orders_app_assisted,
        CAST(adobe_orders_web_all AS FLOAT64) AS adobe_orders_web_all,
        CAST(adobe_orders_app_all AS FLOAT64) AS adobe_orders_app_all,
        CAST(adobe_orders_fully_unassisted AS FLOAT64) AS adobe_orders_fully_unassisted,
        CAST(adobe_orders_fully_assisted AS FLOAT64) AS adobe_orders_fully_assisted,
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all,
        CAST(adobe_storelocator_visits AS FLOAT64) AS adobe_storelocator_visits,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all,

        CAST(platform_spend AS FLOAT64) AS platform_spend,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

profound_weekly_base AS (
    SELECT
        period_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(profound_tmo_citation_share AS FLOAT64) AS profound_tmo_citation_share,
        CAST(profound_tmo_visibility_score AS FLOAT64) AS profound_tmo_visibility_score,
        CAST(profound_att_citation_share AS FLOAT64) AS profound_att_citation_share,
        CAST(profound_att_visibility_score AS FLOAT64) AS profound_att_visibility_score,
        CAST(profound_verizon_citation_share AS FLOAT64) AS profound_verizon_citation_share,
        CAST(profound_verizon_visibility_score AS FLOAT64) AS profound_verizon_visibility_score,

        CAST(gofish_tmo_citation_share AS FLOAT64) AS gofish_tmo_citation_share,
        CAST(gofish_tmo_visibility_score AS FLOAT64) AS gofish_tmo_visibility_score,
        CAST(gofish_att_citation_share AS FLOAT64) AS gofish_att_citation_share,
        CAST(gofish_att_visibility_score AS FLOAT64) AS gofish_att_visibility_score,
        CAST(gofish_verizon_citation_share AS FLOAT64) AS gofish_verizon_citation_share,
        CAST(gofish_verizon_visibility_score AS FLOAT64) AS gofish_verizon_visibility_score
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_weekly`
),

profound_monthly_base AS (
    SELECT
        period_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(profound_tmo_citation_share AS FLOAT64) AS profound_tmo_citation_share,
        CAST(profound_tmo_visibility_score AS FLOAT64) AS profound_tmo_visibility_score,
        CAST(profound_att_citation_share AS FLOAT64) AS profound_att_citation_share,
        CAST(profound_att_visibility_score AS FLOAT64) AS profound_att_visibility_score,
        CAST(profound_verizon_citation_share AS FLOAT64) AS profound_verizon_citation_share,
        CAST(profound_verizon_visibility_score AS FLOAT64) AS profound_verizon_visibility_score,

        CAST(gofish_tmo_citation_share AS FLOAT64) AS gofish_tmo_citation_share,
        CAST(gofish_tmo_visibility_score AS FLOAT64) AS gofish_tmo_visibility_score,
        CAST(gofish_att_citation_share AS FLOAT64) AS gofish_att_citation_share,
        CAST(gofish_att_visibility_score AS FLOAT64) AS gofish_att_visibility_score,
        CAST(gofish_verizon_citation_share AS FLOAT64) AS gofish_verizon_citation_share,
        CAST(gofish_verizon_visibility_score AS FLOAT64) AS gofish_verizon_visibility_score
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly`
),

daily_adobe_long AS (
    SELECT 'ADOBE' AS data_source, 'DAILY' AS time_granularity, 'CALENDAR_DAY' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM daily_base
    UNPIVOT (metric_value FOR metric_name IN (
        adobe_entries, adobe_pspv_actuals, adobe_cart_starts, adobe_cart_start_plus,
        adobe_cart_checkout_visits, adobe_checkout_review_visits, adobe_postpaid_orders_tsr,
        adobe_orders_web_unassisted, adobe_orders_web_assisted, adobe_orders_app_unassisted,
        adobe_orders_app_assisted, adobe_orders_web_all, adobe_orders_app_all,
        adobe_orders_fully_unassisted, adobe_orders_fully_assisted, adobe_orders_all,
        adobe_storelocator_visits
    ))
),

daily_sa360_long AS (
    SELECT 'SA360' AS data_source, 'DAILY' AS time_granularity, 'CALENDAR_DAY' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM daily_base
    UNPIVOT (metric_value FOR metric_name IN (
        sa360_clicks_brand, sa360_clicks_nonbrand, sa360_clicks_all,
        sa360_cart_start_plus_brand, sa360_cart_start_plus_nonbrand, sa360_cart_start_plus_all
    ))
),

daily_gsc_long AS (
    SELECT 'GSC' AS data_source, 'DAILY' AS time_granularity, 'CALENDAR_DAY' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM daily_base
    UNPIVOT (metric_value FOR metric_name IN (
        gsc_clicks_brand, gsc_clicks_nonbrand, gsc_clicks_all,
        gsc_impressions_brand, gsc_impressions_nonbrand, gsc_impressions_all
    ))
),

daily_spend_long AS (
    SELECT 'PLATFORM_SPEND' AS data_source, 'DAILY' AS time_granularity, 'CALENDAR_DAY' AS time_granularity_type, date, lob, channel, 'platform_spend' AS metric_name, platform_spend AS metric_value
    FROM daily_base
),

daily_gmb_long AS (
    SELECT 'GMB' AS data_source, 'DAILY' AS time_granularity, 'CALENDAR_DAY' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM daily_base
    UNPIVOT (metric_value FOR metric_name IN (
        gmb_search_impressions_all, gmb_maps_impressions_all, gmb_impressions_all,
        gmb_call_clicks, gmb_website_clicks, gmb_directions_clicks
    ))
),

weekly_sunsat_adobe_long AS (
    SELECT 'ADOBE' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM weekly_sunsat_base
    UNPIVOT (metric_value FOR metric_name IN (
        adobe_entries, adobe_pspv_actuals, adobe_cart_starts, adobe_cart_start_plus,
        adobe_cart_checkout_visits, adobe_checkout_review_visits, adobe_postpaid_orders_tsr,
        adobe_orders_web_unassisted, adobe_orders_web_assisted, adobe_orders_app_unassisted,
        adobe_orders_app_assisted, adobe_orders_web_all, adobe_orders_app_all,
        adobe_orders_fully_unassisted, adobe_orders_fully_assisted, adobe_orders_all,
        adobe_storelocator_visits
    ))
),

weekly_sunsat_sa360_long AS (
    SELECT 'SA360' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM weekly_sunsat_base
    UNPIVOT (metric_value FOR metric_name IN (
        sa360_clicks_brand, sa360_clicks_nonbrand, sa360_clicks_all,
        sa360_cart_start_plus_brand, sa360_cart_start_plus_nonbrand, sa360_cart_start_plus_all
    ))
),

weekly_sunsat_gsc_long AS (
    SELECT 'GSC' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM weekly_sunsat_base
    UNPIVOT (metric_value FOR metric_name IN (
        gsc_clicks_brand, gsc_clicks_nonbrand, gsc_clicks_all,
        gsc_impressions_brand, gsc_impressions_nonbrand, gsc_impressions_all
    ))
),

weekly_sunsat_spend_long AS (
    SELECT 'PLATFORM_SPEND' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, 'platform_spend' AS metric_name, platform_spend AS metric_value
    FROM weekly_sunsat_base
),

weekly_sunsat_gmb_long AS (
    SELECT 'GMB' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM weekly_sunsat_base
    UNPIVOT (metric_value FOR metric_name IN (
        gmb_search_impressions_all, gmb_maps_impressions_all, gmb_impressions_all,
        gmb_call_clicks, gmb_website_clicks, gmb_directions_clicks
    ))
),

monthly_adobe_long AS (
    SELECT 'ADOBE' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        adobe_entries, adobe_pspv_actuals, adobe_cart_starts, adobe_cart_start_plus,
        adobe_cart_checkout_visits, adobe_checkout_review_visits, adobe_postpaid_orders_tsr,
        adobe_orders_web_unassisted, adobe_orders_web_assisted, adobe_orders_app_unassisted,
        adobe_orders_app_assisted, adobe_orders_web_all, adobe_orders_app_all,
        adobe_orders_fully_unassisted, adobe_orders_fully_assisted, adobe_orders_all,
        adobe_storelocator_visits
    ))
),

monthly_sa360_long AS (
    SELECT 'SA360' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        sa360_clicks_brand, sa360_clicks_nonbrand, sa360_clicks_all,
        sa360_cart_start_plus_brand, sa360_cart_start_plus_nonbrand, sa360_cart_start_plus_all
    ))
),

monthly_gsc_long AS (
    SELECT 'GSC' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        gsc_clicks_brand, gsc_clicks_nonbrand, gsc_clicks_all,
        gsc_impressions_brand, gsc_impressions_nonbrand, gsc_impressions_all
    ))
),

monthly_spend_long AS (
    SELECT 'PLATFORM_SPEND' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, 'platform_spend' AS metric_name, platform_spend AS metric_value
    FROM monthly_base
),

monthly_gmb_long AS (
    SELECT 'GMB' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        gmb_search_impressions_all, gmb_maps_impressions_all, gmb_impressions_all,
        gmb_call_clicks, gmb_website_clicks, gmb_directions_clicks
    ))
),

profound_weekly_long AS (
    SELECT 'PROFOUND' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM profound_weekly_base
    UNPIVOT (metric_value FOR metric_name IN (
        profound_tmo_citation_share, profound_tmo_visibility_score,
        profound_att_citation_share, profound_att_visibility_score,
        profound_verizon_citation_share, profound_verizon_visibility_score
    ))

    UNION ALL

    SELECT 'GOFISH' AS data_source, 'WEEKLY' AS time_granularity, 'SUN_SAT' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM profound_weekly_base
    UNPIVOT (metric_value FOR metric_name IN (
        gofish_tmo_citation_share, gofish_tmo_visibility_score,
        gofish_att_citation_share, gofish_att_visibility_score,
        gofish_verizon_citation_share, gofish_verizon_visibility_score
    ))
),

profound_monthly_long AS (
    SELECT 'PROFOUND' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM profound_monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        profound_tmo_citation_share, profound_tmo_visibility_score,
        profound_att_citation_share, profound_att_visibility_score,
        profound_verizon_citation_share, profound_verizon_visibility_score
    ))

    UNION ALL

    SELECT 'GOFISH' AS data_source, 'MONTHLY' AS time_granularity, 'MONTH_END' AS time_granularity_type, date, lob, channel, metric_name, metric_value
    FROM profound_monthly_base
    UNPIVOT (metric_value FOR metric_name IN (
        gofish_tmo_citation_share, gofish_tmo_visibility_score,
        gofish_att_citation_share, gofish_att_visibility_score,
        gofish_verizon_citation_share, gofish_verizon_visibility_score
    ))
)

SELECT * FROM daily_adobe_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM daily_sa360_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM daily_gsc_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM daily_spend_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM daily_gmb_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM weekly_sunsat_adobe_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM weekly_sunsat_sa360_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM weekly_sunsat_gsc_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM weekly_sunsat_spend_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM weekly_sunsat_gmb_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM monthly_adobe_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM monthly_sa360_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM monthly_gsc_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM monthly_spend_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM monthly_gmb_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM profound_weekly_long WHERE metric_value IS NOT NULL
UNION ALL
SELECT * FROM profound_monthly_long WHERE metric_value IS NOT NULL
;