/* =================================================================================================
FILE:         04_vw_sdi_pulseTms_bronze_mfc_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_mfc_weekly

SOURCE:
  prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfc_weekly

PURPOSE:
  Source-close Bronze view for MFC (Media Forecast & Control) weekly spend data.
  The source is already a Gold view in the MFC pipeline — this Bronze serves as
  the ingestion boundary into the pulseTms pipeline, standardizing column names
  to snake_case and applying SAFE_CAST for type safety.

  No deduplication needed — source is already deduplicated.
  NULL channel rows excluded — no meaningful channel assignment possible.

  Channel mapping applied here — MFC media buying channels mapped to the
  standard pipeline taxonomy (PAID SEARCH, SOCIAL, PROGRAMMATIC, OTHER)
  used by Adobe, SA360, and GSC. Output column is simply 'channel' so all
  downstream views (Silver, Gold Long, reporting) require no changes.

BUSINESS GRAIN:
  One row per:
    qgp_week × lob_supported × channel × tactic × message_type × agency

DATE CONVENTION:
  qgp_week = QGP_Week from source
  Contains week-ending Saturdays for NORMAL weeks (e.g. 2026-03-28)
  Contains quarter-end dates for BOUNDARY_WEEK rows (e.g. 2026-03-31, 2026-04-04)
  Used directly as week_sun_to_sat in Silver

WEEK_TYPE VALUES:
  NORMAL        — regular complete week, all 7 days in one quarter
  BOUNDARY_WEEK — week straddles a quarter boundary, spend already apportioned
                  appears as TWO rows per straddling week:
                    qgp_week = quarter_end_date (e.g. Mar-31) → prior quarter portion
                    qgp_week = actual Saturday  (e.g. Apr-04) → current quarter portion
                  spend values already reflect proportional days — do NOT re-apply
                  partial_weight from dim_date on top of these values

CHANNEL MAPPING:
  MFC channel (source)    → channel (output)
  ──────────────────────────────────────────
  PAID SEARCH             → PAID SEARCH
  PAID SOCIAL             → SOCIAL
  DISPLAY                 → PROGRAMMATIC
  OLV                     → PROGRAMMATIC
  OTT                     → PROGRAMMATIC
  DJS                     → PROGRAMMATIC
  AUDIO                   → OTHER
  CINEMA                  → OTHER
  OOH                     → OTHER
  PRINT                   → OTHER
  RADIO                   → OTHER
  SPOT TV                 → OTHER
  TV                      → OTHER
  AFFILIATE               → OTHER
  NATIONAL RETAIL         → OTHER
  NWT                     → OTHER
  NON-WORKING             → OTHER
  HARVEST FUND            → OTHER
  BUDGET HELD             → OTHER
  UNALLOCATED             → OTHER
  OTHER (DO NOT USE)      → OTHER
  NULL                    → EXCLUDED

SPEND COLUMNS:
  spend_actual   — actual executed spend; NULL for planned-but-not-yet-executed
                   NULL actuals are expected business behavior (not a data quality issue)
  spend_forecast — forecasted/planned spend; always populated
  spend_display  — spend_actual when available, spend_forecast otherwise
                   this is the primary display metric for the dashboard
  For BOUNDARY_WEEK rows: all values already apportioned by days in quarter
  Do NOT multiply by partial_weight from dim_date

KEY MODELING NOTES:
  - LOB_Supported values: BROADBAND, CONSUMER POSTPAID (no BYOD in MFC)
  - Quarter format in source: '2026 Q1' (space, not hyphen) — standardized in Silver
  - QGP_Week appears twice in source SELECT (duplicate column) — deduplicated here
  - FileLoad_Date preserved for lineage but not used for dedup
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Silver : vw_sdi_pulseTms_silver_mfc_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfc_weekly`
AS

SELECT
    -- -----------------------------------------------------------------------
    -- QUARTER CONTEXT
    -- Preserved as-is — standardized '2026 Q1' → '2026-Q1' in Silver
    -- -----------------------------------------------------------------------
    SAFE_CAST(raw.Quarter          AS STRING)                           AS quarter_raw,
    SAFE_CAST(raw.Quarter_End_Date AS DATE)                             AS quarter_end_date,

    -- -----------------------------------------------------------------------
    -- DATE FIELDS
    -- period_start : Monday of the MFC week
    -- period_end   : Sunday of the MFC week (MFC uses Mon-Sun convention)
    -- qgp_week     : Saturday (aligns to our Sun-Sat convention)
    --                OR quarter_end_date for BOUNDARY_WEEK rows
    -- Used as week_sun_to_sat in Silver
    -- -----------------------------------------------------------------------
    SAFE_CAST(raw.Period_Start     AS DATE)                             AS period_start,
    SAFE_CAST(raw.Period_End       AS DATE)                             AS period_end,
    SAFE_CAST(raw.QGP_Week         AS DATE)                             AS qgp_week,

    -- -----------------------------------------------------------------------
    -- WEEK TYPE
    -- -----------------------------------------------------------------------
    SAFE_CAST(UPPER(TRIM(raw.week_type)) AS STRING)                     AS week_type,

    -- -----------------------------------------------------------------------
    -- DIMENSION COLUMNS
    -- channel: MFC source channel mapped to standard pipeline taxonomy
    --   Output is simply 'channel' — no downstream changes needed
    -- -----------------------------------------------------------------------
    SAFE_CAST(UPPER(TRIM(raw.LOB_Supported)) AS STRING)                 AS lob_supported,

    -- Channel mapping: MFC media buying channels → standard pipeline channels
    CASE UPPER(TRIM(raw.Channel))
        WHEN 'PAID SEARCH'  THEN 'PAID SEARCH'
        WHEN 'PAID SOCIAL'  THEN 'SOCIAL'
        WHEN 'DISPLAY'      THEN 'PROGRAMMATIC'
        WHEN 'OLV'          THEN 'PROGRAMMATIC'
        WHEN 'OTT'          THEN 'PROGRAMMATIC'
        WHEN 'DJS'          THEN 'PROGRAMMATIC'
        -- All other channels: AUDIO, CINEMA, OOH, PRINT, RADIO,
        -- SPOT TV, TV, AFFILIATE, NATIONAL RETAIL, NWT, NON-WORKING,
        -- HARVEST FUND, BUDGET HELD, UNALLOCATED, OTHER (DO NOT USE)
        ELSE 'OTHER'
    END                                                                 AS channel,

    SAFE_CAST(UPPER(TRIM(raw.Tactic))       AS STRING)                  AS tactic,
    SAFE_CAST(UPPER(TRIM(raw.Message_Type)) AS STRING)                  AS message_type,
    SAFE_CAST(UPPER(TRIM(raw.Agency))       AS STRING)                  AS agency,

    -- -----------------------------------------------------------------------
    -- SPEND METRICS
    -- -----------------------------------------------------------------------
    SAFE_CAST(raw.weekly_actual   AS FLOAT64)                           AS spend_actual,
    SAFE_CAST(raw.weekly_forecast AS FLOAT64)                           AS spend_forecast,
    SAFE_CAST(raw.weekly_display  AS FLOAT64)                           AS spend_display,

    -- -----------------------------------------------------------------------
    -- AUDIT FIELDS
    -- -----------------------------------------------------------------------
    SAFE_CAST(raw.FileLoad_Date AS DATE)                                AS file_load_date

FROM `prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly` raw

-- Exclude NULL channel rows — no meaningful channel assignment possible
WHERE raw.Channel IS NOT NULL
;