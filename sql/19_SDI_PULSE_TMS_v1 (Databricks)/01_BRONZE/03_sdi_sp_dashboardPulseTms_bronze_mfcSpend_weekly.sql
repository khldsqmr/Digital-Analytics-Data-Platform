/* =================================================================================================
FILE:         03_sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly.sql   (Databricks port)
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly.
  Called as part of the weekly refresh.

  Grain: one row per qgp_week x lob x channel_group x channel x tactic x message_type x agency.
  qgp_week is the authoritative date key — all date attributes (quarter, week_type, etc.)
  are resolved downstream by joining to sdi_vw_dashboardPulseTms_dim_qgp_calendar on qgp_week = qgp_date.

CHANNEL GROUPS (standard vocabulary):
  'Paid Search' | 'Paid Social' | 'Programmatic' | 'Other'
  (Organic Search and Direct are Adobe-only; not present in MFC)

LOB CANONICAL VALUES:
  'POSTPAID'   — source: 'CONSUMER POSTPAID', 'POSTPAID'
  'BROADBAND'  — source: 'HSI', 'BROADBAND'
  'TFB'        — source: 'TFB', 'TBG' (TBG is a legacy source code, normalized to TFB)

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - SAFE_CAST(x AS FLOAT64)      -> TRY_CAST(x AS DOUBLE)
  - CREATE TABLE ... PARTITION BY x CLUSTER BY y ... OPTIONS(description=...)
                                  -> CREATE TABLE ... CLUSTER BY (x, y) COMMENT '...'
  - OPTIONS(strict_mode=false)   -> dropped; no Databricks equivalent
  - CREATE OR REPLACE PROCEDURE ... BEGIN...END -> needs explicit LANGUAGE SQL clause

  ⚠ SOURCE SCHEMA FLAG — please verify before running:
  This procedure reads from the MFC Gold granular view (prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly below).
  In the BigQuery version that source was sdi_vw_mfc_gold_spendGranular_weekly, exposing exactly
  the 9 flat columns referenced in the SELECT below (QGP_Week, LOB_Supported, Channel, Tactic,
  Message_Type, Agency, spend_actual, spend_forecast, FileLoad_Date) with no WoW/YoY of its own.
  Per your Databricks MFC pipeline notes, the finalized Databricks MFC Gold output is structured
  differently — Spend_Actual/Spend_Forecast as independent metrics, a Spend_Status column, and
  separate actual/forecast WoW/YoY numerator-denominator pairs. This procedure doesn't need any of
  that WoW/YoY/status detail (PulseTMS computes its own downstream), but the exact column names/
  casing on the real Databricks Gold granular view may not match what's used below 1:1 — please
  confirm and adjust the SELECT list (and prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly placeholder) to match.

CHANGE LOG:
  - Removed spend_display filter (referenced non-existent column — copy-paste bug).
  - Replaced with spend_actual / spend_forecast null+zero filter.
  - Dropped passthrough date columns from source (quarter_raw, quarter_end_date,
    period_start, period_end, week_type) — QGP calendar dim is authoritative for these.
  - Added TBG → TFB LOB remap comment.
  - Updated channel group mapping: Programmatic now only includes DISPLAY and OLV.
    AUDIO, OTT, and OOH (previously partially Programmatic) now fall through to Other.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly
  USING DELTA
  CLUSTER BY (qgp_week, lob, channel_group)
  COMMENT 'PulseTMS Bronze — MFC spend granular. One row per qgp_week x lob x channel_group x channel x tactic x message_type x agency. Clustered by qgp_week, lob, channel_group. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly. LOBs: POSTPAID, BROADBAND, TFB. Programmatic: DISPLAY, OLV only. AUDIO, OTT, OOH map to Other.'
  AS
  SELECT
    TRY_CAST(raw.QGP_Week AS DATE)                                        AS qgp_week,

    -- LOB canonical mapping:
    --   CONSUMER POSTPAID / POSTPAID → POSTPAID
    --   HSI / BROADBAND              → BROADBAND
    --   TFB / TBG                    → TFB (TBG is a legacy source code for the same LOB)
    CASE UPPER(TRIM(raw.LOB_Supported))
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE UPPER(TRIM(raw.LOB_Supported))
    END                                                                   AS lob,

    -- Channel group mapping to standard vocabulary:
    --   Programmatic : DISPLAY, OLV only
    --   Other        : everything else including AUDIO, OTT, OOH
    CASE
      WHEN UPPER(TRIM(raw.Channel)) = 'PAID SEARCH'                       THEN 'Paid Search'
      WHEN UPPER(TRIM(raw.Channel)) = 'PAID SOCIAL'                       THEN 'Paid Social'
      WHEN UPPER(TRIM(raw.Channel)) IN ('DISPLAY', 'OLV')                 THEN 'Programmatic'
      ELSE                                                                     'Other'
    END                                                                   AS channel_group,

    UPPER(TRIM(raw.Channel))                                              AS channel,
    UPPER(TRIM(raw.Tactic))                                               AS tactic,
    UPPER(TRIM(raw.Message_Type))                                         AS message_type,
    UPPER(TRIM(raw.Agency))                                               AS agency,

    TRY_CAST(raw.spend_actual   AS DOUBLE)                                AS spend_actual,
    TRY_CAST(raw.spend_forecast AS DOUBLE)                                AS spend_forecast,
    TRY_CAST(raw.FileLoad_Date  AS DATE)                                  AS file_load_date

  FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly raw
  WHERE raw.Channel IS NOT NULL
    AND UPPER(TRIM(raw.Channel)) NOT IN (
      'OTHER (DO NOT USE)',
      'NON-WORKING',
      --'UNALLOCATED',
      'BUDGET HELD'
    )
    --AND UPPER(TRIM(raw.Message_Type)) != 'MICRO'
    -- Keep rows that have meaningful spend in at least one of actual or forecast.
    -- A row may have forecast but no actual (future weeks) or actual but no forecast (past weeks).
    AND (
      (raw.spend_actual   IS NOT NULL AND raw.spend_actual   != 0)
      OR (raw.spend_forecast IS NOT NULL AND raw.spend_forecast != 0)
    );

END;