/* =================================================================================================
FILE: 06_vw_sdi_tsd_gold_long.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_long

PURPOSE:
  Final long-form Gold reporting mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      data_source
      time_granularity
      time_granularity_type
      date
      lob
      channel
      metric_name

KEY MODELING NOTES:
  - Final output includes source_max_available_date
  - source_max_available_date is calculated from the relevant Silver source
  - Freshness is joined by data_source and time_granularity to avoid duplicate rows
  - For Adobe, SA360, GSC, platform spend, GMB, and MAIS, weekly and monthly use the same Silver daily source max event_date
  - For ProFound and GoFish, weekly and monthly use their respective Silver weekly and monthly source max period_date
  - PAID SEARCH MAIS: sub-channel metrics only, no total
  - Non-Paid Search MAIS: mais_platform_spend total only
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_long`
AS

WITH
weekly_sunsat_base AS (
    SELECT
        weekSunToSat                                            AS date,
        UPPER(TRIM(lob))                                        AS lob,
        UPPER(TRIM(channel))                                    AS channel,

        CAST(adobe_entries AS FLOAT64)                          AS adobe_entries,
        CAST(adobe_pspv_actuals AS FLOAT64)                     AS adobe_pspv_actuals,
        CAST(adobe_cart_starts AS FLOAT64)                      AS adobe_cart_starts,
        CAST(adobe_cart_start_plus AS FLOAT64)                  AS adobe_cart_start_plus,
        CAST(adobe_cart_checkout_visits AS FLOAT64)             AS adobe_cart_checkout_visits,
        CAST(adobe_checkout_review_visits AS FLOAT64)           AS adobe_checkout_review_visits,
        CAST(adobe_postpaid_orders_tsr AS FLOAT64)              AS adobe_postpaid_orders_tsr,
        CAST(adobe_orders_web_unassisted AS FLOAT64)            AS adobe_orders_web_unassisted,
        CAST(adobe_orders_web_assisted AS FLOAT64)              AS adobe_orders_web_assisted,
        CAST(adobe_orders_app_unassisted AS FLOAT64)            AS adobe_orders_app_unassisted,
        CAST(adobe_orders_app_assisted AS FLOAT64)              AS adobe_orders_app_assisted,
        CAST(adobe_orders_web_all AS FLOAT64)                   AS adobe_orders_web_all,
        CAST(adobe_orders_app_all AS FLOAT64)                   AS adobe_orders_app_all,
        CAST(adobe_orders_fully_unassisted AS FLOAT64)          AS adobe_orders_fully_unassisted,
        CAST(adobe_orders_fully_assisted AS FLOAT64)            AS adobe_orders_fully_assisted,
        CAST(adobe_orders_all AS FLOAT64)                       AS adobe_orders_all,
        CAST(adobe_storelocator_visits AS FLOAT64)              AS adobe_storelocator_visits,
        CAST(adobeTLifeAppVisits AS FLOAT64)                    AS adobeTLifeAppVisits,

        CAST(sa360_clicks_brand AS FLOAT64)                     AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64)                  AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64)                       AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64)            AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64)         AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64)              AS sa360_cart_start_plus_all,

        CAST(gsc_clicks_brand AS FLOAT64)                       AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64)                    AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64)                         AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64)                  AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64)               AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64)                    AS gsc_impressions_all,

        CAST(platform_spend AS FLOAT64)                         AS platform_spend,

        CAST(gmb_search_impressions_all AS FLOAT64)             AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64)               AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64)                    AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64)                        AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64)                     AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64)                  AS gmb_directions_clicks,

        CAST(mais_platform_spend AS FLOAT64)                    AS mais_platform_spend,
        CAST(mais_platform_spend_branded AS FLOAT64)            AS mais_platform_spend_branded,
        CAST(mais_platform_spend_nonbranded AS FLOAT64)         AS mais_platform_spend_nonbranded,
        CAST(mais_platform_spend_pla AS FLOAT64)                AS mais_platform_spend_pla,
        CAST(mais_platform_spend_pmax AS FLOAT64)               AS mais_platform_spend_pmax

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
),

monthly_base AS (
    SELECT
        monthEnd                                                AS date,
        UPPER(TRIM(lob))                                        AS lob,
        UPPER(TRIM(channel))                                    AS channel,

        CAST(adobe_entries AS FLOAT64)                          AS adobe_entries,
        CAST(adobe_pspv_actuals AS FLOAT64)                     AS adobe_pspv_actuals,
        CAST(adobe_cart_starts AS FLOAT64)                      AS adobe_cart_starts,
        CAST(adobe_cart_start_plus AS FLOAT64)                  AS adobe_cart_start_plus,
        CAST(adobe_cart_checkout_visits AS FLOAT64)             AS adobe_cart_checkout_visits,
        CAST(adobe_checkout_review_visits AS FLOAT64)           AS adobe_checkout_review_visits,
        CAST(adobe_postpaid_orders_tsr AS FLOAT64)              AS adobe_postpaid_orders_tsr,
        CAST(adobe_orders_web_unassisted AS FLOAT64)            AS adobe_orders_web_unassisted,
        CAST(adobe_orders_web_assisted AS FLOAT64)              AS adobe_orders_web_assisted,
        CAST(adobe_orders_app_unassisted AS FLOAT64)            AS adobe_orders_app_unassisted,
        CAST(adobe_orders_app_assisted AS FLOAT64)              AS adobe_orders_app_assisted,
        CAST(adobe_orders_web_all AS FLOAT64)                   AS adobe_orders_web_all,
        CAST(adobe_orders_app_all AS FLOAT64)                   AS adobe_orders_app_all,
        CAST(adobe_orders_fully_unassisted AS FLOAT64)          AS adobe_orders_fully_unassisted,
        CAST(adobe_orders_fully_assisted AS FLOAT64)            AS adobe_orders_fully_assisted,
        CAST(adobe_orders_all AS FLOAT64)                       AS adobe_orders_all,
        CAST(adobe_storelocator_visits AS FLOAT64)              AS adobe_storelocator_visits,
        CAST(adobeTLifeAppVisits AS FLOAT64)                    AS adobeTLifeAppVisits,

        CAST(sa360_clicks_brand AS FLOAT64)                     AS sa360_clicks_brand,
        CAST(sa360_clicks_nonbrand AS FLOAT64)                  AS sa360_clicks_nonbrand,
        CAST(sa360_clicks_all AS FLOAT64)                       AS sa360_clicks_all,
        CAST(sa360_cart_start_plus_brand AS FLOAT64)            AS sa360_cart_start_plus_brand,
        CAST(sa360_cart_start_plus_nonbrand AS FLOAT64)         AS sa360_cart_start_plus_nonbrand,
        CAST(sa360_cart_start_plus_all AS FLOAT64)              AS sa360_cart_start_plus_all,

        CAST(gsc_clicks_brand AS FLOAT64)                       AS gsc_clicks_brand,
        CAST(gsc_clicks_nonbrand AS FLOAT64)                    AS gsc_clicks_nonbrand,
        CAST(gsc_clicks_all AS FLOAT64)                         AS gsc_clicks_all,
        CAST(gsc_impressions_brand AS FLOAT64)                  AS gsc_impressions_brand,
        CAST(gsc_impressions_nonbrand AS FLOAT64)               AS gsc_impressions_nonbrand,
        CAST(gsc_impressions_all AS FLOAT64)                    AS gsc_impressions_all,

        CAST(platform_spend AS FLOAT64)                         AS platform_spend,

        CAST(gmb_search_impressions_all AS FLOAT64)             AS gmb_search_impressions_all,
        CAST(gmb_maps_impressions_all AS FLOAT64)               AS gmb_maps_impressions_all,
        CAST(gmb_impressions_all AS FLOAT64)                    AS gmb_impressions_all,
        CAST(gmb_call_clicks AS FLOAT64)                        AS gmb_call_clicks,
        CAST(gmb_website_clicks AS FLOAT64)                     AS gmb_website_clicks,
        CAST(gmb_directions_clicks AS FLOAT64)                  AS gmb_directions_clicks,

        CAST(mais_platform_spend AS FLOAT64)                    AS mais_platform_spend,
        CAST(mais_platform_spend_branded AS FLOAT64)            AS mais_platform_spend_branded,
        CAST(mais_platform_spend_nonbranded AS FLOAT64)         AS mais_platform_spend_nonbranded,
        CAST(mais_platform_spend_pla AS FLOAT64)                AS mais_platform_spend_pla,
        CAST(mais_platform_spend_pmax AS FLOAT64)               AS mais_platform_spend_pmax

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
),

profound_weekly_base AS (
    SELECT
        period_date                                             AS date,
        UPPER(TRIM(lob))                                        AS lob,
        UPPER(TRIM(channel))                                    AS channel,

        CAST(profound_tmo_executions AS FLOAT64)                AS profound_tmo_executions,
        CAST(profound_tmo_citation_count AS FLOAT64)            AS profound_tmo_citation_count,
        CAST(profound_tmo_citation_share AS FLOAT64)            AS profound_tmo_citation_share,
        CAST(profound_tmo_visibility_score AS FLOAT64)          AS profound_tmo_visibility_score,

        CAST(profound_att_executions AS FLOAT64)                AS profound_att_executions,
        CAST(profound_att_citation_count AS FLOAT64)            AS profound_att_citation_count,
        CAST(profound_att_citation_share AS FLOAT64)            AS profound_att_citation_share,
        CAST(profound_att_visibility_score AS FLOAT64)          AS profound_att_visibility_score,

        CAST(profound_verizon_executions AS FLOAT64)            AS profound_verizon_executions,
        CAST(profound_verizon_citation_count AS FLOAT64)        AS profound_verizon_citation_count,
        CAST(profound_verizon_citation_share AS FLOAT64)        AS profound_verizon_citation_share,
        CAST(profound_verizon_visibility_score AS FLOAT64)      AS profound_verizon_visibility_score,

        CAST(gofish_tmo_executions AS FLOAT64)                  AS gofish_tmo_executions,
        CAST(gofish_tmo_citation_count AS FLOAT64)              AS gofish_tmo_citation_count,
        CAST(gofish_tmo_citation_share AS FLOAT64)              AS gofish_tmo_citation_share,
        CAST(gofish_tmo_visibility_score AS FLOAT64)            AS gofish_tmo_visibility_score,

        CAST(gofish_att_executions AS FLOAT64)                  AS gofish_att_executions,
        CAST(gofish_att_citation_count AS FLOAT64)              AS gofish_att_citation_count,
        CAST(gofish_att_citation_share AS FLOAT64)              AS gofish_att_citation_share,
        CAST(gofish_att_visibility_score AS FLOAT64)            AS gofish_att_visibility_score,

        CAST(gofish_verizon_executions AS FLOAT64)              AS gofish_verizon_executions,
        CAST(gofish_verizon_citation_count AS FLOAT64)          AS gofish_verizon_citation_count,
        CAST(gofish_verizon_citation_share AS FLOAT64)          AS gofish_verizon_citation_share,
        CAST(gofish_verizon_visibility_score AS FLOAT64)        AS gofish_verizon_visibility_score

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_weekly`
),

profound_monthly_base AS (
    SELECT
        period_date                                             AS date,
        UPPER(TRIM(lob))                                        AS lob,
        UPPER(TRIM(channel))                                    AS channel,

        CAST(profound_tmo_executions AS FLOAT64)                AS profound_tmo_executions,
        CAST(profound_tmo_citation_count AS FLOAT64)            AS profound_tmo_citation_count,
        CAST(profound_tmo_citation_share AS FLOAT64)            AS profound_tmo_citation_share,
        CAST(profound_tmo_visibility_score AS FLOAT64)          AS profound_tmo_visibility_score,

        CAST(profound_att_executions AS FLOAT64)                AS profound_att_executions,
        CAST(profound_att_citation_count AS FLOAT64)            AS profound_att_citation_count,
        CAST(profound_att_citation_share AS FLOAT64)            AS profound_att_citation_share,
        CAST(profound_att_visibility_score AS FLOAT64)          AS profound_att_visibility_score,

        CAST(profound_verizon_executions AS FLOAT64)            AS profound_verizon_executions,
        CAST(profound_verizon_citation_count AS FLOAT64)        AS profound_verizon_citation_count,
        CAST(profound_verizon_citation_share AS FLOAT64)        AS profound_verizon_citation_share,
        CAST(profound_verizon_visibility_score AS FLOAT64)      AS profound_verizon_visibility_score,

        CAST(gofish_tmo_executions AS FLOAT64)                  AS gofish_tmo_executions,
        CAST(gofish_tmo_citation_count AS FLOAT64)              AS gofish_tmo_citation_count,
        CAST(gofish_tmo_citation_share AS FLOAT64)              AS gofish_tmo_citation_share,
        CAST(gofish_tmo_visibility_score AS FLOAT64)            AS gofish_tmo_visibility_score,

        CAST(gofish_att_executions AS FLOAT64)                  AS gofish_att_executions,
        CAST(gofish_att_citation_count AS FLOAT64)              AS gofish_att_citation_count,
        CAST(gofish_att_citation_share AS FLOAT64)              AS gofish_att_citation_share,
        CAST(gofish_att_visibility_score AS FLOAT64)            AS gofish_att_visibility_score,

        CAST(gofish_verizon_executions AS FLOAT64)              AS gofish_verizon_executions,
        CAST(gofish_verizon_citation_count AS FLOAT64)          AS gofish_verizon_citation_count,
        CAST(gofish_verizon_citation_share AS FLOAT64)          AS gofish_verizon_citation_share,
        CAST(gofish_verizon_visibility_score AS FLOAT64)        AS gofish_verizon_visibility_score

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly`
),

/* ================================================================================================
SOURCE FRESHNESS
- This creates one source_max_available_date per data_source and time_granularity
- Joining by both fields prevents row duplication for PROFOUND and GOFISH
================================================================================================ */

source_max_dates AS (
    SELECT
        'ADOBE' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'SA360' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'GSC' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'PLATFORM_SPEND' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'GMB' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'MAIS_PLATFORM_SPEND' AS data_source,
        tg AS time_granularity,
        MAX(event_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_maisSpend_daily`
    CROSS JOIN UNNEST(['WEEKLY', 'MONTHLY']) AS tg
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'PROFOUND' AS data_source,
        'WEEKLY' AS time_granularity,
        MAX(period_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_weekly`

    UNION ALL

    SELECT
        'PROFOUND' AS data_source,
        'MONTHLY' AS time_granularity,
        MAX(period_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly`

    UNION ALL

    SELECT
        'GOFISH' AS data_source,
        'WEEKLY' AS time_granularity,
        MAX(period_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_weekly`

    UNION ALL

    SELECT
        'GOFISH' AS data_source,
        'MONTHLY' AS time_granularity,
        MAX(period_date) AS source_max_available_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly`
),

weekly_sunsat_adobe_long AS (
    SELECT
        'ADOBE'             AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM weekly_sunsat_base
        WHERE channel IN ('PAID SEARCH', 'ORGANIC SEARCH')
    )
    UNPIVOT (metric_value FOR metric_name IN (
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
        adobe_orders_all,
        adobe_storelocator_visits,
        adobeTLifeAppVisits
    ))
),

weekly_sunsat_sa360_long AS (
    SELECT
        'SA360'             AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM weekly_sunsat_base
        WHERE channel = 'PAID SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        sa360_clicks_brand,
        sa360_clicks_nonbrand,
        sa360_clicks_all,
        sa360_cart_start_plus_brand,
        sa360_cart_start_plus_nonbrand,
        sa360_cart_start_plus_all
    ))
),

weekly_sunsat_gsc_long AS (
    SELECT
        'GSC'               AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM weekly_sunsat_base
        WHERE channel = 'ORGANIC SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gsc_clicks_brand,
        gsc_clicks_nonbrand,
        gsc_clicks_all,
        gsc_impressions_brand,
        gsc_impressions_nonbrand,
        gsc_impressions_all
    ))
),

weekly_sunsat_spend_long AS (
    SELECT
        'PLATFORM_SPEND'    AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel,
        'platform_spend'    AS metric_name,
        platform_spend      AS metric_value
    FROM weekly_sunsat_base
    WHERE channel = 'PAID SEARCH'
),

weekly_sunsat_gmb_long AS (
    SELECT
        'GMB'               AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM weekly_sunsat_base
        WHERE channel = 'MAPS & LOCAL SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gmb_search_impressions_all,
        gmb_maps_impressions_all,
        gmb_impressions_all,
        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks
    ))
),

weekly_sunsat_mais_spend_long AS (
    SELECT
        'MAIS_PLATFORM_SPEND'   AS data_source,
        'WEEKLY'                AS time_granularity,
        'SUN_SAT'               AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM weekly_sunsat_base
        WHERE channel = 'PAID SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        mais_platform_spend_branded,
        mais_platform_spend_nonbranded,
        mais_platform_spend_pla,
        mais_platform_spend_pmax
    ))

    UNION ALL

    SELECT
        'MAIS_PLATFORM_SPEND'   AS data_source,
        'WEEKLY'                AS time_granularity,
        'SUN_SAT'               AS time_granularity_type,
        date, lob, channel,
        'mais_platform_spend'   AS metric_name,
        mais_platform_spend     AS metric_value
    FROM weekly_sunsat_base
    WHERE channel != 'PAID SEARCH'
      AND mais_platform_spend IS NOT NULL
),

monthly_adobe_long AS (
    SELECT
        'ADOBE'             AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM monthly_base
        WHERE channel IN ('PAID SEARCH', 'ORGANIC SEARCH')
    )
    UNPIVOT (metric_value FOR metric_name IN (
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
        adobe_orders_all,
        adobe_storelocator_visits,
        adobeTLifeAppVisits
    ))
),

monthly_sa360_long AS (
    SELECT
        'SA360'             AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM monthly_base
        WHERE channel = 'PAID SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        sa360_clicks_brand,
        sa360_clicks_nonbrand,
        sa360_clicks_all,
        sa360_cart_start_plus_brand,
        sa360_cart_start_plus_nonbrand,
        sa360_cart_start_plus_all
    ))
),

monthly_gsc_long AS (
    SELECT
        'GSC'               AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM monthly_base
        WHERE channel = 'ORGANIC SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gsc_clicks_brand,
        gsc_clicks_nonbrand,
        gsc_clicks_all,
        gsc_impressions_brand,
        gsc_impressions_nonbrand,
        gsc_impressions_all
    ))
),

monthly_spend_long AS (
    SELECT
        'PLATFORM_SPEND'    AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel,
        'platform_spend'    AS metric_name,
        platform_spend      AS metric_value
    FROM monthly_base
    WHERE channel = 'PAID SEARCH'
),

monthly_gmb_long AS (
    SELECT
        'GMB'               AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM monthly_base
        WHERE channel = 'MAPS & LOCAL SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gmb_search_impressions_all,
        gmb_maps_impressions_all,
        gmb_impressions_all,
        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks
    ))
),

monthly_mais_spend_long AS (
    SELECT
        'MAIS_PLATFORM_SPEND'   AS data_source,
        'MONTHLY'               AS time_granularity,
        'MONTH_END'             AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM monthly_base
        WHERE channel = 'PAID SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        mais_platform_spend_branded,
        mais_platform_spend_nonbranded,
        mais_platform_spend_pla,
        mais_platform_spend_pmax
    ))

    UNION ALL

    SELECT
        'MAIS_PLATFORM_SPEND'   AS data_source,
        'MONTHLY'               AS time_granularity,
        'MONTH_END'             AS time_granularity_type,
        date, lob, channel,
        'mais_platform_spend'   AS metric_name,
        mais_platform_spend     AS metric_value
    FROM monthly_base
    WHERE channel != 'PAID SEARCH'
      AND mais_platform_spend IS NOT NULL
),

profound_weekly_long AS (
    SELECT
        'PROFOUND'          AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM profound_weekly_base
        WHERE channel = 'AI SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        profound_tmo_executions,
        profound_tmo_citation_count,
        profound_tmo_citation_share,
        profound_tmo_visibility_score,
        profound_att_executions,
        profound_att_citation_count,
        profound_att_citation_share,
        profound_att_visibility_score,
        profound_verizon_executions,
        profound_verizon_citation_count,
        profound_verizon_citation_share,
        profound_verizon_visibility_score
    ))

    UNION ALL

    SELECT
        'GOFISH'            AS data_source,
        'WEEKLY'            AS time_granularity,
        'SUN_SAT'           AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM profound_weekly_base
        WHERE channel = 'AI SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gofish_tmo_executions,
        gofish_tmo_citation_count,
        gofish_tmo_citation_share,
        gofish_tmo_visibility_score,
        gofish_att_executions,
        gofish_att_citation_count,
        gofish_att_citation_share,
        gofish_att_visibility_score,
        gofish_verizon_executions,
        gofish_verizon_citation_count,
        gofish_verizon_citation_share,
        gofish_verizon_visibility_score
    ))
),

profound_monthly_long AS (
    SELECT
        'PROFOUND'          AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM profound_monthly_base
        WHERE channel = 'AI SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        profound_tmo_executions,
        profound_tmo_citation_count,
        profound_tmo_citation_share,
        profound_tmo_visibility_score,
        profound_att_executions,
        profound_att_citation_count,
        profound_att_citation_share,
        profound_att_visibility_score,
        profound_verizon_executions,
        profound_verizon_citation_count,
        profound_verizon_citation_share,
        profound_verizon_visibility_score
    ))

    UNION ALL

    SELECT
        'GOFISH'            AS data_source,
        'MONTHLY'           AS time_granularity,
        'MONTH_END'         AS time_granularity_type,
        date, lob, channel, metric_name, metric_value
    FROM (
        SELECT * FROM profound_monthly_base
        WHERE channel = 'AI SEARCH'
    )
    UNPIVOT (metric_value FOR metric_name IN (
        gofish_tmo_executions,
        gofish_tmo_citation_count,
        gofish_tmo_citation_share,
        gofish_tmo_visibility_score,
        gofish_att_executions,
        gofish_att_citation_count,
        gofish_att_citation_share,
        gofish_att_visibility_score,
        gofish_verizon_executions,
        gofish_verizon_citation_count,
        gofish_verizon_citation_share,
        gofish_verizon_visibility_score
    ))
),

all_long AS (
    SELECT * FROM weekly_sunsat_adobe_long          WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM weekly_sunsat_sa360_long          WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM weekly_sunsat_gsc_long            WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM weekly_sunsat_spend_long          WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM weekly_sunsat_gmb_long            WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM weekly_sunsat_mais_spend_long     WHERE metric_value IS NOT NULL

    UNION ALL
    SELECT * FROM monthly_adobe_long                WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM monthly_sa360_long                WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM monthly_gsc_long                  WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM monthly_spend_long                WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM monthly_gmb_long                  WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM monthly_mais_spend_long           WHERE metric_value IS NOT NULL

    UNION ALL
    SELECT * FROM profound_weekly_long              WHERE metric_value IS NOT NULL
    UNION ALL
    SELECT * FROM profound_monthly_long             WHERE metric_value IS NOT NULL
)

SELECT
    al.data_source,
    al.time_granularity,
    al.time_granularity_type,
    al.date,
    al.lob,
    al.channel,
    al.metric_name,
    al.metric_value,
    smd.source_max_available_date

FROM all_long al
LEFT JOIN source_max_dates smd
  ON al.data_source = smd.data_source
 AND al.time_granularity = smd.time_granularity;