/* =================================================================================================
FILE: 07_vw_sdi_tsd_gold_long.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly
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
  - Uses the already-built gold views as the source of truth
  - Does not re-aggregate metrics
  - Unpivots each source family separately to preserve data lineage and avoid value mixing
  - Uses actual source-family values in data_source:
      ADOBE, SA360, GSC, PLATFORM_SPEND, GMB, PROFOUND
  - Weekly ProFound is tagged as WEEKLY_MON_SUN because the weekly source is Sunday-ending
  - time_granularity_type makes weekly calendar filtering explicit:
      MON_SUN, SUN_SAT, NOT_APPLICABLE
  - metric_value is standardized to FLOAT64

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_long`
AS

/* -------------------------------------------------------------------------------------------------
   1) DAILY — ADOBE
------------------------------------------------------------------------------------------------- */
WITH daily_adobe_base AS (
    SELECT
        'ADOBE' AS data_source,
        'DAILY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
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
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

daily_adobe_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM daily_adobe_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            adobe_entries,
            adobe_pspv_actuals,
            adobe_cart_starts,
            adobe_cart_start_plus,
            adobe_cart_checkout_visits,
            adobe_checkout_review_visits,
            adobe_postpaid_orders_tsr,
            adobe_orders_web_unassisted,
            adobe_orders_web_assisted,
            adobe_orders_app_unassisted,
            adobe_orders_app_assisted,
            adobe_orders_web_all,
            adobe_orders_app_all,
            adobe_orders_fully_unassisted,
            adobe_orders_fully_assisted,
            adobe_orders_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   2) DAILY — SA360
------------------------------------------------------------------------------------------------- */
daily_sa360_base AS (
    SELECT
        'SA360' AS data_source,
        'DAILY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        event_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

daily_sa360_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM daily_sa360_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            sa360_clicks_brand,
            sa360_clicks_nonbrand,
            sa360_clicks_all,
            sa360_cart_start_plus_brand,
            sa360_cart_start_plus_nonbrand,
            sa360_cart_start_plus_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   3) DAILY — GSC
------------------------------------------------------------------------------------------------- */
daily_gsc_base AS (
    SELECT
        'GSC' AS data_source,
        'DAILY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        event_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

daily_gsc_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM daily_gsc_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gsc_clicks_brand,
            gsc_clicks_nonbrand,
            gsc_clicks_all,
            gsc_impressions_brand,
            gsc_impressions_nonbrand,
            gsc_impressions_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   4) DAILY — PLATFORM SPEND
------------------------------------------------------------------------------------------------- */
daily_spend_long AS (
    SELECT
        'PLATFORM_SPEND' AS data_source,
        'DAILY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        event_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        'platform_spend' AS metric_name,
        CAST(platform_spend AS FLOAT64) AS metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

/* -------------------------------------------------------------------------------------------------
   5) DAILY — GMB
------------------------------------------------------------------------------------------------- */
daily_gmb_base AS (
    SELECT
        'GMB' AS data_source,
        'DAILY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        event_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
),

daily_gmb_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM daily_gmb_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gmb_search_impressions_all,
            gmb_maps_impressions_all,
            gmb_impressions_all,
            gmb_call_clicks,
            gmb_website_clicks,
            gmb_directions_clicks
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   6) WEEKLY_MON_SUN — ADOBE
------------------------------------------------------------------------------------------------- */
weekly_monsun_adobe_base AS (
    SELECT
        'ADOBE' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        weekMonToSun AS date,
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
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly`
),

weekly_monsun_adobe_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_monsun_adobe_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            adobe_entries,
            adobe_pspv_actuals,
            adobe_cart_starts,
            adobe_cart_start_plus,
            adobe_cart_checkout_visits,
            adobe_checkout_review_visits,
            adobe_postpaid_orders_tsr,
            adobe_orders_web_unassisted,
            adobe_orders_web_assisted,
            adobe_orders_app_unassisted,
            adobe_orders_app_assisted,
            adobe_orders_web_all,
            adobe_orders_app_all,
            adobe_orders_fully_unassisted,
            adobe_orders_fully_assisted,
            adobe_orders_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   7) WEEKLY_MON_SUN — SA360
------------------------------------------------------------------------------------------------- */
weekly_monsun_sa360_base AS (
    SELECT
        'SA360' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        weekMonToSun AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly`
),

weekly_monsun_sa360_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_monsun_sa360_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            sa360_clicks_brand,
            sa360_clicks_nonbrand,
            sa360_clicks_all,
            sa360_cart_start_plus_brand,
            sa360_cart_start_plus_nonbrand,
            sa360_cart_start_plus_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   8) WEEKLY_MON_SUN — GSC
------------------------------------------------------------------------------------------------- */
weekly_monsun_gsc_base AS (
    SELECT
        'GSC' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        weekMonToSun AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly`
),

weekly_monsun_gsc_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_monsun_gsc_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gsc_clicks_brand,
            gsc_clicks_nonbrand,
            gsc_clicks_all,
            gsc_impressions_brand,
            gsc_impressions_nonbrand,
            gsc_impressions_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   9) WEEKLY_MON_SUN — PLATFORM SPEND
------------------------------------------------------------------------------------------------- */
weekly_monsun_spend_long AS (
    SELECT
        'PLATFORM_SPEND' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        weekMonToSun AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        'platform_spend' AS metric_name,
        CAST(platform_spend AS FLOAT64) AS metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly`
),

/* -------------------------------------------------------------------------------------------------
   10) WEEKLY_MON_SUN — GMB
------------------------------------------------------------------------------------------------- */
weekly_monsun_gmb_base AS (
    SELECT
        'GMB' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        weekMonToSun AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedMonSun_weekly`
),

weekly_monsun_gmb_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_monsun_gmb_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gmb_search_impressions_all,
            gmb_maps_impressions_all,
            gmb_impressions_all,
            gmb_call_clicks,
            gmb_website_clicks,
            gmb_directions_clicks
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   11) WEEKLY_SUN_SAT — ADOBE
------------------------------------------------------------------------------------------------- */
weekly_sunsat_adobe_base AS (
    SELECT
        'ADOBE' AS data_source,
        'WEEKLY_SUN_SAT' AS time_granularity,
        'SUN_SAT' AS time_granularity_type,
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
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

weekly_sunsat_adobe_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_sunsat_adobe_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            adobe_entries,
            adobe_pspv_actuals,
            adobe_cart_starts,
            adobe_cart_start_plus,
            adobe_cart_checkout_visits,
            adobe_checkout_review_visits,
            adobe_postpaid_orders_tsr,
            adobe_orders_web_unassisted,
            adobe_orders_web_assisted,
            adobe_orders_app_unassisted,
            adobe_orders_app_assisted,
            adobe_orders_web_all,
            adobe_orders_app_all,
            adobe_orders_fully_unassisted,
            adobe_orders_fully_assisted,
            adobe_orders_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   12) WEEKLY_SUN_SAT — SA360
------------------------------------------------------------------------------------------------- */
weekly_sunsat_sa360_base AS (
    SELECT
        'SA360' AS data_source,
        'WEEKLY_SUN_SAT' AS time_granularity,
        'SUN_SAT' AS time_granularity_type,
        weekSunToSat AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

weekly_sunsat_sa360_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_sunsat_sa360_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            sa360_clicks_brand,
            sa360_clicks_nonbrand,
            sa360_clicks_all,
            sa360_cart_start_plus_brand,
            sa360_cart_start_plus_nonbrand,
            sa360_cart_start_plus_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   13) WEEKLY_SUN_SAT — GSC
------------------------------------------------------------------------------------------------- */
weekly_sunsat_gsc_base AS (
    SELECT
        'GSC' AS data_source,
        'WEEKLY_SUN_SAT' AS time_granularity,
        'SUN_SAT' AS time_granularity_type,
        weekSunToSat AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

weekly_sunsat_gsc_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_sunsat_gsc_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gsc_clicks_brand,
            gsc_clicks_nonbrand,
            gsc_clicks_all,
            gsc_impressions_brand,
            gsc_impressions_nonbrand,
            gsc_impressions_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   14) WEEKLY_SUN_SAT — PLATFORM SPEND
------------------------------------------------------------------------------------------------- */
weekly_sunsat_spend_long AS (
    SELECT
        'PLATFORM_SPEND' AS data_source,
        'WEEKLY_SUN_SAT' AS time_granularity,
        'SUN_SAT' AS time_granularity_type,
        weekSunToSat AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        'platform_spend' AS metric_name,
        CAST(platform_spend AS FLOAT64) AS metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

/* -------------------------------------------------------------------------------------------------
   15) WEEKLY_SUN_SAT — GMB
------------------------------------------------------------------------------------------------- */
weekly_sunsat_gmb_base AS (
    SELECT
        'GMB' AS data_source,
        'WEEKLY_SUN_SAT' AS time_granularity,
        'SUN_SAT' AS time_granularity_type,
        weekSunToSat AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

weekly_sunsat_gmb_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM weekly_sunsat_gmb_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gmb_search_impressions_all,
            gmb_maps_impressions_all,
            gmb_impressions_all,
            gmb_call_clicks,
            gmb_website_clicks,
            gmb_directions_clicks
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   16) MONTHLY — ADOBE
------------------------------------------------------------------------------------------------- */
monthly_adobe_base AS (
    SELECT
        'ADOBE' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        monthStart AS date,
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
        CAST(adobe_orders_all AS FLOAT64) AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

monthly_adobe_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM monthly_adobe_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            adobe_entries,
            adobe_pspv_actuals,
            adobe_cart_starts,
            adobe_cart_start_plus,
            adobe_cart_checkout_visits,
            adobe_checkout_review_visits,
            adobe_postpaid_orders_tsr,
            adobe_orders_web_unassisted,
            adobe_orders_web_assisted,
            adobe_orders_app_unassisted,
            adobe_orders_app_assisted,
            adobe_orders_web_all,
            adobe_orders_app_all,
            adobe_orders_fully_unassisted,
            adobe_orders_fully_assisted,
            adobe_orders_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   17) MONTHLY — SA360
------------------------------------------------------------------------------------------------- */
monthly_sa360_base AS (
    SELECT
        'SA360' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        monthStart AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(sa360_clicks_brand AS FLOAT64) AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64) AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64) AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64) AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64) AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64) AS sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

monthly_sa360_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM monthly_sa360_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            sa360_clicks_brand,
            sa360_clicks_nonbrand,
            sa360_clicks_all,
            sa360_cart_start_plus_brand,
            sa360_cart_start_plus_nonbrand,
            sa360_cart_start_plus_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   18) MONTHLY — GSC
------------------------------------------------------------------------------------------------- */
monthly_gsc_base AS (
    SELECT
        'GSC' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        monthStart AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gsc_clicks_brand AS FLOAT64) AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64) AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64) AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64) AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64) AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64) AS gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

monthly_gsc_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM monthly_gsc_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gsc_clicks_brand,
            gsc_clicks_nonbrand,
            gsc_clicks_all,
            gsc_impressions_brand,
            gsc_impressions_nonbrand,
            gsc_impressions_all
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   19) MONTHLY — PLATFORM SPEND
------------------------------------------------------------------------------------------------- */
monthly_spend_long AS (
    SELECT
        'PLATFORM_SPEND' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        monthStart AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        'platform_spend' AS metric_name,
        CAST(platform_spend AS FLOAT64) AS metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

/* -------------------------------------------------------------------------------------------------
   20) MONTHLY — GMB
------------------------------------------------------------------------------------------------- */
monthly_gmb_base AS (
    SELECT
        'GMB' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        monthStart AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(gmb_search_impressions_all AS FLOAT64) AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64) AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64) AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64) AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64) AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

monthly_gmb_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM monthly_gmb_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            gmb_search_impressions_all,
            gmb_maps_impressions_all,
            gmb_impressions_all,
            gmb_call_clicks,
            gmb_website_clicks,
            gmb_directions_clicks
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   21) WEEKLY_MON_SUN — PROFOUND
------------------------------------------------------------------------------------------------- */
profound_weekly_base AS (
    SELECT
        'PROFOUND' AS data_source,
        'WEEKLY_MON_SUN' AS time_granularity,
        'MON_SUN' AS time_granularity_type,
        period_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(profound_tmo_citation_share_brand AS FLOAT64) AS profound_tmo_citation_share_brand,
        CAST(profound_tmo_citation_share_nonbrand AS FLOAT64) AS profound_tmo_citation_share_nonbrand,
        CAST(profound_tmo_visibility_score_brand AS FLOAT64) AS profound_tmo_visibility_score_brand,
        CAST(profound_tmo_visibility_score_nonbrand AS FLOAT64) AS profound_tmo_visibility_score_nonbrand,

        CAST(profound_att_citation_share_brand AS FLOAT64) AS profound_att_citation_share_brand,
        CAST(profound_att_citation_share_nonbrand AS FLOAT64) AS profound_att_citation_share_nonbrand,
        CAST(profound_att_visibility_score_brand AS FLOAT64) AS profound_att_visibility_score_brand,
        CAST(profound_att_visibility_score_nonbrand AS FLOAT64) AS profound_att_visibility_score_nonbrand,

        CAST(profound_verizon_citation_share_brand AS FLOAT64) AS profound_verizon_citation_share_brand,
        CAST(profound_verizon_citation_share_nonbrand AS FLOAT64) AS profound_verizon_citation_share_nonbrand,
        CAST(profound_verizon_visibility_score_brand AS FLOAT64) AS profound_verizon_visibility_score_brand,
        CAST(profound_verizon_visibility_score_nonbrand AS FLOAT64) AS profound_verizon_visibility_score_nonbrand
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_weekly`
),

profound_weekly_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM profound_weekly_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            profound_tmo_citation_share_brand,
            profound_tmo_citation_share_nonbrand,
            profound_tmo_visibility_score_brand,
            profound_tmo_visibility_score_nonbrand,
            profound_att_citation_share_brand,
            profound_att_citation_share_nonbrand,
            profound_att_visibility_score_brand,
            profound_att_visibility_score_nonbrand,
            profound_verizon_citation_share_brand,
            profound_verizon_citation_share_nonbrand,
            profound_verizon_visibility_score_brand,
            profound_verizon_visibility_score_nonbrand
        )
    )
),

/* -------------------------------------------------------------------------------------------------
   22) MONTHLY — PROFOUND
------------------------------------------------------------------------------------------------- */
profound_monthly_base AS (
    SELECT
        'PROFOUND' AS data_source,
        'MONTHLY' AS time_granularity,
        'NOT_APPLICABLE' AS time_granularity_type,
        period_date AS date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        CAST(profound_tmo_citation_share_brand AS FLOAT64) AS profound_tmo_citation_share_brand,
        CAST(profound_tmo_citation_share_nonbrand AS FLOAT64) AS profound_tmo_citation_share_nonbrand,
        CAST(profound_tmo_visibility_score_brand AS FLOAT64) AS profound_tmo_visibility_score_brand,
        CAST(profound_tmo_visibility_score_nonbrand AS FLOAT64) AS profound_tmo_visibility_score_nonbrand,

        CAST(profound_att_citation_share_brand AS FLOAT64) AS profound_att_citation_share_brand,
        CAST(profound_att_citation_share_nonbrand AS FLOAT64) AS profound_att_citation_share_nonbrand,
        CAST(profound_att_visibility_score_brand AS FLOAT64) AS profound_att_visibility_score_brand,
        CAST(profound_att_visibility_score_nonbrand AS FLOAT64) AS profound_att_visibility_score_nonbrand,

        CAST(profound_verizon_citation_share_brand AS FLOAT64) AS profound_verizon_citation_share_brand,
        CAST(profound_verizon_citation_share_nonbrand AS FLOAT64) AS profound_verizon_citation_share_nonbrand,
        CAST(profound_verizon_visibility_score_brand AS FLOAT64) AS profound_verizon_visibility_score_brand,
        CAST(profound_verizon_visibility_score_nonbrand AS FLOAT64) AS profound_verizon_visibility_score_nonbrand
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly`
),

profound_monthly_long AS (
    SELECT
        data_source,
        time_granularity,
        time_granularity_type,
        date,
        lob,
        channel,
        metric_name,
        metric_value
    FROM profound_monthly_base
    UNPIVOT (
        metric_value FOR metric_name IN (
            profound_tmo_citation_share_brand,
            profound_tmo_citation_share_nonbrand,
            profound_tmo_visibility_score_brand,
            profound_tmo_visibility_score_nonbrand,
            profound_att_citation_share_brand,
            profound_att_citation_share_nonbrand,
            profound_att_visibility_score_brand,
            profound_att_visibility_score_nonbrand,
            profound_verizon_citation_share_brand,
            profound_verizon_citation_share_nonbrand,
            profound_verizon_visibility_score_brand,
            profound_verizon_visibility_score_nonbrand
        )
    )
)

SELECT * FROM daily_adobe_long
UNION ALL
SELECT * FROM daily_sa360_long
UNION ALL
SELECT * FROM daily_gsc_long
UNION ALL
SELECT * FROM daily_spend_long
UNION ALL
SELECT * FROM daily_gmb_long

UNION ALL
SELECT * FROM weekly_monsun_adobe_long
UNION ALL
SELECT * FROM weekly_monsun_sa360_long
UNION ALL
SELECT * FROM weekly_monsun_gsc_long
UNION ALL
SELECT * FROM weekly_monsun_spend_long
UNION ALL
SELECT * FROM weekly_monsun_gmb_long

UNION ALL
SELECT * FROM weekly_sunsat_adobe_long
UNION ALL
SELECT * FROM weekly_sunsat_sa360_long
UNION ALL
SELECT * FROM weekly_sunsat_gsc_long
UNION ALL
SELECT * FROM weekly_sunsat_spend_long
UNION ALL
SELECT * FROM weekly_sunsat_gmb_long

UNION ALL
SELECT * FROM monthly_adobe_long
UNION ALL
SELECT * FROM monthly_sa360_long
UNION ALL
SELECT * FROM monthly_gsc_long
UNION ALL
SELECT * FROM monthly_spend_long
UNION ALL
SELECT * FROM monthly_gmb_long

UNION ALL
SELECT * FROM profound_weekly_long
UNION ALL
SELECT * FROM profound_monthly_long
;
