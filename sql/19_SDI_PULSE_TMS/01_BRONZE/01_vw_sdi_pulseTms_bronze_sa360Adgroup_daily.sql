/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_bronze_sa360Adgroup_daily.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_sa360Adgroup_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_ps_sa360_adgroup_daily_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_sa360Adgroup_daily

PURPOSE:
  Source-close Bronze view for SA360 paid search performance at ad group level.
  Identical logic to vw_sdi_pulseTms_bronze_sa360Adgroup_daily.
  Renamed pulseTms to support the full TMS pipeline (BYOD + Postpaid + HSI).
  BYOD/BYOP ad group filtering is applied in Silver.
  Brand vs nonbrand classification via ad_group_name regex is applied in Silver.
  Deduplicates the daily snapshot and preserves relevant raw fields as-is.

BUSINESS GRAIN:
  One row per:
    account_id + ad_group_id + date_yyyymmdd

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

DOWNSTREAM:
  Silver : vw_sdi_pulseTms_silver_sa360_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_sa360Adgroup_daily`
AS

WITH ranked AS (
    SELECT
        SAFE_CAST(raw.account_id    AS STRING)   AS account_id,
        SAFE_CAST(raw.account_name  AS STRING)   AS account_name,
        SAFE_CAST(raw.ad_group_id   AS STRING)   AS ad_group_id,
        SAFE_CAST(raw.ad_group_name AS STRING)   AS ad_group_name,
        SAFE_CAST(raw.campaign_id   AS STRING)   AS campaign_id,
        SAFE_CAST(raw.campaign_name AS STRING)   AS campaign_name,
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date,
        SAFE_CAST(raw.impressions AS FLOAT64)                           AS impressions,
        SAFE_CAST(raw.clicks      AS FLOAT64)                           AS clicks,
        SAFE_CAST(raw.cost_micros AS FLOAT64)                           AS cost_micros,
        SAFE_CAST(raw.cost_micros AS FLOAT64) / 1000000                 AS cost,
        SAFE_CAST(raw.postpaid__prospect__web__order AS FLOAT64)        AS postpaid_prospect_web_order,
        SAFE_CAST(raw.postpaid__cart__start_         AS FLOAT64)        AS postpaid_cart_start,
        SAFE_CAST(raw.postpaid_pspv_                 AS FLOAT64)        AS postpaid_pspv,
        SAFE_CAST(raw.cart__start_                   AS FLOAT64)        AS cart_start,
        SAFE_CAST(raw.__insert_date AS INT64)                           AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,
        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id  AS STRING),
                SAFE_CAST(raw.ad_group_id AS STRING),
                CAST(raw.date_yyyymmdd    AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)     DESC,
                raw.Filename                          DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_ps_sa360_adgroup_daily_tmo` raw
    WHERE raw.account_id    IS NOT NULL
      AND raw.ad_group_id   IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id, account_name, ad_group_id, ad_group_name,
    campaign_id, campaign_name, date_yyyymmdd, event_date,
    impressions, clicks, cost_micros, cost,
    postpaid_prospect_web_order, postpaid_cart_start, postpaid_pspv, cart_start,
    insert_date, file_load_datetime, filename
FROM ranked
WHERE rn = 1
;