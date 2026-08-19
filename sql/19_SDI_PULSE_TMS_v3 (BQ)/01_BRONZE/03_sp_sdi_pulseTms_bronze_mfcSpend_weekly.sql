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

FORECAST FALLBACK (week-level, all-or-nothing):
  For each qgp_week, check whether ANY row in that week has a non-null, non-zero
  spend_forecast. If yes — real forecast data exists for that week — spend_forecast is
  left exactly as it comes from the source for every row in that week, NULLs included.
  If no — no forecast at all for that week — spend_forecast is filled with spend_actual
  for every row in that week.

  Rule: if even one combo in a week has a real forecast, no substitution happens anywhere
  in that week. Substitution is all-or-nothing at the week grain.

  Implemented via a window function (week_has_forecast flag) in the typed_raw CTE,
  applied in the final SELECT. Silver and Gold require no changes.

CHANGE LOG:
  - Removed spend_display filter (referenced non-existent column — copy-paste bug).
  - Replaced with spend_actual / spend_forecast null+zero filter.
  - Dropped passthrough date columns from source (quarter_raw, quarter_end_date,
    period_start, period_end, week_type) — QGP calendar dim is authoritative for these.
  - Added TBG -> TFB LOB remap comment.
  - Updated channel group mapping: Programmatic now only includes DISPLAY and OLV.
    AUDIO, OTT, and OOH (previously partially Programmatic) now fall through to Other.
  - Added week-level forecast fallback: if no row in a qgp_week has any forecast,
    spend_forecast is filled with spend_actual for that entire week. If any forecast
    exists in the week, all rows (including nulls) are left as-is from the source.
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
    description = 'PulseTMS Bronze — MFC spend granular. One row per qgp_week x lob x channel_group x channel x tactic x message_type x agency. Partitioned by qgp_week, clustered by lob and channel_group. Refreshed weekly via sp_sdi_pulseTms_bronze_mfcSpend_weekly. LOBs: POSTPAID, BROADBAND, TFB. Programmatic: DISPLAY, OLV only. AUDIO, OTT, OOH map to Other. spend_forecast: if no forecast exists anywhere in a qgp_week, filled with spend_actual for that entire week; if any forecast exists in the week, all rows including nulls are left as-is.'
  )
  AS

  -- Step 1: cast and clean all columns from the source, apply channel/LOB mappings,
  -- and compute the week_has_forecast flag in one pass via a window function.
  -- The flag checks the raw spend_forecast before any substitution.
  WITH typed_raw AS (
    SELECT
      SAFE_CAST(raw.QGP_Week AS DATE)                                     AS qgp_week,

      -- LOB canonical mapping
      CASE UPPER(TRIM(raw.LOB_Supported))
        WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
        WHEN 'POSTPAID'          THEN 'POSTPAID'
        WHEN 'HSI'               THEN 'BROADBAND'
        WHEN 'BROADBAND'         THEN 'BROADBAND'
        WHEN 'TBG'               THEN 'TFB'
        WHEN 'TFB'               THEN 'TFB'
        ELSE UPPER(TRIM(raw.LOB_Supported))
      END                                                                  AS lob,

      -- Channel group mapping: Programmatic = DISPLAY, OLV only
      CASE
        WHEN UPPER(TRIM(raw.Channel)) = 'PAID SEARCH'                     THEN 'Paid Search'
        WHEN UPPER(TRIM(raw.Channel)) = 'PAID SOCIAL'                     THEN 'Paid Social'
        WHEN UPPER(TRIM(raw.Channel)) IN ('DISPLAY', 'OLV')               THEN 'Programmatic'
        ELSE                                                                   'Other'
      END                                                                  AS channel_group,

      UPPER(TRIM(raw.Channel))                                             AS channel,
      UPPER(TRIM(raw.Tactic))                                              AS tactic,
      UPPER(TRIM(raw.Message_Type))                                        AS message_type,
      UPPER(TRIM(raw.Agency))                                              AS agency,

      SAFE_CAST(raw.spend_actual   AS FLOAT64)                             AS spend_actual,
      SAFE_CAST(raw.spend_forecast AS FLOAT64)                             AS spend_forecast,
      SAFE_CAST(raw.FileLoad_Date  AS DATE)                                AS file_load_date,

      -- Week-level forecast flag: 1 if any row in this qgp_week has a real forecast,
      -- 0 if no row in this qgp_week has any forecast at all.
      -- Evaluated on the raw cast value before any substitution.
      MAX(
        CASE
          WHEN SAFE_CAST(raw.spend_forecast AS FLOAT64) IS NOT NULL
           AND SAFE_CAST(raw.spend_forecast AS FLOAT64) != 0
          THEN 1 ELSE 0
        END
      ) OVER (PARTITION BY SAFE_CAST(raw.QGP_Week AS DATE))               AS week_has_forecast

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` raw
    WHERE raw.Channel IS NOT NULL
      AND UPPER(TRIM(raw.Channel)) NOT IN (
        'OTHER (DO NOT USE)',
        'NON-WORKING',
        --'UNALLOCATED',
        'BUDGET HELD'
      )
      --AND UPPER(TRIM(raw.Message_Type)) != 'MICRO'
      -- Keep rows that have meaningful spend in at least one of actual or forecast.
      -- Evaluated on raw values before substitution.
      AND (
        (raw.spend_actual   IS NOT NULL AND raw.spend_actual   != 0)
        OR (raw.spend_forecast IS NOT NULL AND raw.spend_forecast != 0)
      )
  )

  -- Step 2: apply the week-level fallback.
  -- week_has_forecast = 1 -> real forecast exists somewhere in this week -> leave as-is.
  -- week_has_forecast = 0 -> no forecast at all in this week -> use spend_actual instead.
  SELECT
    qgp_week,
    lob,
    channel_group,
    channel,
    tactic,
    message_type,
    agency,
    spend_actual,
    CASE
      WHEN week_has_forecast = 1 THEN spend_forecast
      ELSE                            spend_actual
    END                                                                    AS spend_forecast,
    file_load_date
  FROM typed_raw;

END;