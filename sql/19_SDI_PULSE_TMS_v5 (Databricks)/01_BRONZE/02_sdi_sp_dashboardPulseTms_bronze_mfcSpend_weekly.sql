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
  'POSTPAID'         — source: 'CONSUMER POSTPAID', 'POSTPAID'
  'BROADBAND'        — source: 'HSI', 'BROADBAND'
  'TFB'              — source: 'TFB', 'TBG' (TBG is a legacy source code, normalized to TFB)
  'CONSUMER PREPAID' — source: 'CONSUMER PREPAID' (passed through uppercased; no remap needed)

FORECAST FALLBACK (per qgp_week x lob x message_type, all-or-nothing):
  For each qgp_week x lob x message_type, check whether ANY row in that group has a non-null,
  non-zero spend_forecast. If yes — a plan exists for that message type on that qgp_week —
  spend_forecast is left exactly as it comes from the source for every row in the group, NULLs
  included. If no — the message type has no plan on that qgp_week — spend_forecast is filled with
  spend_actual for every row in the group.

  Quarter-boundary weeks: a boundary stub (for example Wed 2026-09-30) and the first-week row after it
  (Sat 2026-10-03) are different qgp_week values, so each is evaluated on its own. The source books a
  boundary week's plan to the stub (Q3 tag) while most of its actuals land on the first-week row (Q4 tag).
  Example, Postpaid NETWORK, week ending 2026-10-03: stub forecast 2.09M, first-week forecast NULL,
  first-week actual 3.43M. The first-week row therefore shows forecast = actual (3.43M) and the stub keeps
  its 2.09M plan.
  Known effect: Silver adds stub + first-week row to build the boundary-week WoW / YoY numerators, so the
  FORECAST metric's WoW / YoY on those rows includes the filled value (about 5.5M against a booked plan of
  2.09M). Actual WoW / YoY and every displayed forecast value are unaffected.

  Why not row by row: filling every row that lacks a forecast would double count whenever a plan
  line and an actual line describe the same spend under different channel / tactic / agency labels
  (for example Prepaid OOH actuals under OOH - STATIC while the plan sits under OOH - DIGITAL).

  History: this was originally all-or-nothing per qgp_week across ALL LOBs. A week that had any
  forecast anywhere therefore showed NULL forecast for message types with no plan at all that week.
  It is now evaluated per qgp_week x lob x message_type.

  Implemented via a window function (week_has_forecast flag) in the typed_raw CTE,
  applied in the final SELECT. Silver and Gold require no changes. The validation procedure
  (STEP 2B, MfcSourceTyped) repeats the same partition so source and Bronze stay consistent.

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - SAFE_CAST(x AS FLOAT64)      -> TRY_CAST(x AS DOUBLE)
  - CREATE TABLE ... PARTITION BY x CLUSTER BY y ... OPTIONS(description=...)
                                  -> CREATE TABLE ... CLUSTER BY (x, y) COMMENT '...'
  - OPTIONS(strict_mode=false)   -> dropped; no Databricks equivalent
  - CREATE OR REPLACE PROCEDURE ... BEGIN...END -> LANGUAGE SQL / SQL SECURITY INVOKER

  ⚠ SOURCE SCHEMA FLAG — please verify before running:
  This procedure reads from the MFC Gold granular view
  (prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly). Confirm the column names
  QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency, spend_actual,
  spend_forecast, FileLoad_Date match the actual Databricks Gold view exactly.

CHANGE LOG:
  - Removed spend_display filter (referenced non-existent column — copy-paste bug).
  - Replaced with spend_actual / spend_forecast null+zero filter.
  - Dropped passthrough date columns from source — QGP calendar dim is authoritative.
  - Added TBG -> TFB LOB remap comment.
  - Updated channel group mapping: Programmatic now only includes DISPLAY and OLV.
    AUDIO, OTT, and OOH (previously partially Programmatic) now fall through to Other.
  - Added week-level forecast fallback: if no row in a qgp_week has any forecast,
    spend_forecast is filled with spend_actual for that entire week. If any forecast
    exists in the week, all rows (including nulls) are left as-is from the source.
  - 2026-11-10: the forecast fallback is now evaluated per qgp_week x lob x
    message_type instead of per qgp_week across all LOBs, so a message type with no plan at
    all that week shows its actuals as forecast even when other message types in the same week do
    have forecast. Documented CONSUMER PREPAID as a passed-through LOB (the MFC chain now includes it).
  - 2026-11-10: output is aggregated to the documented grain (SUM by key). UPPER / TRIM and the LOB
    mapping can make two source rows share a key; without this, Silver's join on the key would fan out
    and double count them.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly
  USING DELTA
  CLUSTER BY (qgp_week, lob, channel_group)
  COMMENT 'PulseTMS Bronze — MFC spend granular. One row per qgp_week x lob x channel_group x channel x tactic x message_type x agency. Clustered by qgp_week, lob, channel_group. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly. LOBs: POSTPAID, BROADBAND, TFB, CONSUMER PREPAID. Programmatic: DISPLAY, OLV only. AUDIO, OTT, OOH map to Other. spend_forecast: if no forecast exists anywhere for a qgp_week x lob x message_type, filled with spend_actual for that whole group; if any forecast exists in the group, all rows including nulls are left as-is.'
  AS

  -- Step 1: cast and clean all columns from the source, apply channel/LOB mappings,
  -- and compute the week_has_forecast flag in one pass via a window function.
  -- The flag checks the raw spend_forecast before any substitution.
  WITH typed_raw AS (
    SELECT
      TRY_CAST(raw.QGP_Week AS DATE)                                      AS qgp_week,

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

      TRY_CAST(raw.spend_actual   AS DOUBLE)                               AS spend_actual,
      TRY_CAST(raw.spend_forecast AS DOUBLE)                               AS spend_forecast,
      TRY_CAST(raw.FileLoad_Date  AS DATE)                                 AS file_load_date,

      -- Forecast flag per qgp_week x lob x message_type: 1 if any row in this group has a real
      -- forecast, 0 if no row in this group has any forecast at all. A boundary stub and its first-week
      -- row are different qgp_week values, so each is its own group.
      -- Evaluated on the raw cast value before any substitution. The LOB expression in the
      -- PARTITION BY is the same mapping as the lob column above.
      MAX(
        CASE
          WHEN TRY_CAST(raw.spend_forecast AS DOUBLE) IS NOT NULL
           AND TRY_CAST(raw.spend_forecast AS DOUBLE) != 0
          THEN 1 ELSE 0
        END
      ) OVER (
        PARTITION BY
          TRY_CAST(raw.QGP_Week AS DATE),
          CASE UPPER(TRIM(raw.LOB_Supported))
            WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
            WHEN 'POSTPAID'          THEN 'POSTPAID'
            WHEN 'HSI'               THEN 'BROADBAND'
            WHEN 'BROADBAND'         THEN 'BROADBAND'
            WHEN 'TBG'               THEN 'TFB'
            WHEN 'TFB'               THEN 'TFB'
            ELSE UPPER(TRIM(raw.LOB_Supported))
          END,
          UPPER(TRIM(raw.Message_Type))
      )                                                                    AS week_has_forecast

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
      -- Evaluated on raw values before substitution.
      AND (
        (raw.spend_actual   IS NOT NULL AND raw.spend_actual   != 0)
        OR (raw.spend_forecast IS NOT NULL AND raw.spend_forecast != 0)
      )
  )

  -- Step 2: apply the fallback.
  -- week_has_forecast = 1 -> real forecast exists for this qgp_week x lob x message_type -> leave as-is.
  -- week_has_forecast = 0 -> no forecast at all for this group -> use spend_actual instead.
  -- Step 3: one row per key. The key is built with UPPER / TRIM and the LOB mapping, so two source rows
  -- that differ only by case, spaces or a legacy LOB code now share a key. Silver joins on this key, and a
  -- duplicated key would fan out and double count, so the rows are summed here. Output is identical
  -- when every key is already unique. NULL keys group together.
  SELECT
    qgp_week,
    lob,
    channel_group,
    channel,
    tactic,
    message_type,
    agency,
    SUM(spend_actual)                                                      AS spend_actual,
    SUM(
      CASE
        WHEN week_has_forecast = 1 THEN spend_forecast
        ELSE                            spend_actual
      END
    )                                                                      AS spend_forecast,
    MAX(file_load_date)                                                    AS file_load_date
  FROM typed_raw
  GROUP BY
    qgp_week,
    lob,
    channel_group,
    channel,
    tactic,
    message_type,
    agency;

END;