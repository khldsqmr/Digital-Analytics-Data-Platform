/* =================================================================================================
FILE:         03_sp_sdi_pulseTms_bronze_mfcSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_bronze_mfcSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_bronze_mfcSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  Grain: one row per qgp_week x lob x channel_group x channel x tactic x message_type x agency.
  qgp_week is the authoritative date key — all date attributes (quarter, week_type, etc.)
  are resolved downstream by joining to vw_sdi_pulseTms_dim_qgp_calendar on qgp_week = qgp_date.

CHANNEL GROUPS (standard vocabulary):
  'Paid Search' | 'Paid Social' | 'Programmatic' | 'Other'
  (Organic Search and Direct are Adobe-only; not present in MFC)

LOB CANONICAL VALUES:
  'POSTPAID'   — source: 'CONSUMER POSTPAID', 'POSTPAID'
  'BROADBAND'  — source: 'HSI', 'BROADBAND'
  'TFB'        — source: 'TFB', 'TBG' (TBG is a legacy source code, normalized to TFB)

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
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_mfcSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_mfcSpend_weekly`
  PARTITION BY qgp_week
  CLUSTER BY lob, channel_group
  OPTIONS (
    description = 'PulseTMS Bronze — MFC spend granular. One row per qgp_week x lob x channel_group x channel x tactic x message_type x agency. Partitioned by qgp_week, clustered by lob and channel_group. Refreshed weekly via sp_sdi_pulseTms_bronze_mfcSpend_weekly. LOBs: POSTPAID, BROADBAND, TFB. Programmatic: DISPLAY, OLV only. AUDIO, OTT, OOH map to Other.'
  )
  AS
  SELECT
    SAFE_CAST(raw.QGP_Week AS DATE)                                       AS qgp_week,

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

    SAFE_CAST(raw.spend_actual   AS FLOAT64)                              AS spend_actual,
    SAFE_CAST(raw.spend_forecast AS FLOAT64)                              AS spend_forecast,
    SAFE_CAST(raw.FileLoad_Date  AS DATE)                                 AS file_load_date

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` raw
  WHERE raw.Channel IS NOT NULL
    AND UPPER(TRIM(raw.Channel)) NOT IN (
      'OTHER (DO NOT USE)', 'NON-WORKING', 'UNALLOCATED', 'BUDGET HELD'
    )
    AND UPPER(TRIM(raw.Message_Type)) != 'MICRO'
    -- Keep rows that have meaningful spend in at least one of actual or forecast.
    -- A row may have forecast but no actual (future weeks) or actual but no forecast (past weeks).
    AND (
      (raw.spend_actual   IS NOT NULL AND raw.spend_actual   != 0)
      OR (raw.spend_forecast IS NOT NULL AND raw.spend_forecast != 0)
    );

END;
