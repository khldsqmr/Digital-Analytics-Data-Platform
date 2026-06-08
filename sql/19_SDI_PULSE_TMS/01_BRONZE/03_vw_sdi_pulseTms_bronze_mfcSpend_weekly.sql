/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_bronze_mfcSpend_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_mfcSpend_weekly

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfcSpend_weekly

PURPOSE:
  Source-close Bronze view for MFC (Media Forecast & Control) weekly spend data
  in the PulseTMS pipeline.

  Reads directly from the MFC Gold view in BigQuery. This Bronze serves as the
  ingestion boundary into PulseTMS, standardizing column names to snake_case,
  applying SAFE_CAST for type safety, mapping MFC media channels to the standard
  PulseTMS channel group taxonomy, and normalizing LOB values.

  No deduplication needed — the source is already deduplicated.

BUSINESS GRAIN:
  One row per:
    qgp_week × lob × channel_group × channel × tactic × message_type × agency

DATE CONVENTION:
  qgp_week = QGP_Week from source.
  For NORMAL weeks   : week-ending Saturday (e.g. 2026-03-28)
  For BOUNDARY_WEEK  : two rows per straddling week:
      qgp_week = quarter_end_date (e.g. 2026-03-31) → prior quarter portion
      qgp_week = first Saturday of new quarter (e.g. 2026-04-04) → new quarter portion
  Spend values are already apportioned by the source — do NOT re-apply partial
  weighting from the QGP calendar dim on top of these values.

WEEK_TYPE VALUES (from source):
  NORMAL        — regular complete week, all 7 days within one quarter
  BOUNDARY_WEEK — week straddles a quarter boundary; spend pre-apportioned by source

LOB VALUES (all in scope):
  Source value         → Output value
  CONSUMER POSTPAID    → CONSUMER POSTPAID
  BROADBAND            → BROADBAND
  TFB                  → TFB
  TBG                  → TFB  (TBG is TFB-only; normalized here)
  CONSUMER PREPAID     → CONSUMER PREPAID
  T-MOBILE MONEY       → T-MOBILE MONEY

CHANNEL GROUP MAPPING:
  MFC channel (source)              → channel_group (output)
  ──────────────────────────────────────────────────────────
  PAID SEARCH                       → Paid Search
  PAID SOCIAL                       → Paid Social
  DISPLAY                           → Programmatic
  OLV                               → Programmatic
  AUDIO                             → Programmatic
  OTT  (tactic LIKE %PROGRAMMATIC%) → Programmatic
  OOH  (tactic LIKE %PROGRAMMATIC%) → Programmatic
  All other channels                → Other

EXCLUSION FILTERS:
  - Channel IS NOT NULL
  - Channel NOT IN ('OTHER (DO NOT USE)', 'NON-WORKING', 'UNALLOCATED', 'BUDGET HELD')
  - Message_Type NOT IN ('MICRO')
  - spend_display IS NOT NULL AND spend_display != 0

SPEND COLUMNS:
  spend_actual   — actual executed spend; NULL for planned-but-not-yet-executed periods
                   (NULL actuals are expected business behavior, not a data quality issue)
  spend_forecast — forecasted/planned spend; always populated
  NOTE: spend_display is intentionally excluded from this pipeline.

BUSINESS RULES:
  - TBG LOB is normalized to TFB at this layer.
  - Boundary week spend is already apportioned; do NOT multiply by partial_weight.
  - quarter_raw preserved as-is from source (format: '2026 Q1').
  - No ORDER BY — applied in Gold only.
  - file_load_date preserved for lineage.

DOWNSTREAM:
  06_vw_sdi_pulseTms_silver_mfcSpend_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfcSpend_weekly`
AS

SELECT
  -- -------------------------------------------------------------------------
  -- QUARTER CONTEXT
  -- -------------------------------------------------------------------------
  SAFE_CAST(raw.Quarter          AS STRING)                             AS quarter_raw,
  SAFE_CAST(raw.Quarter_End_Date AS DATE)                               AS quarter_end_date,

  -- -------------------------------------------------------------------------
  -- DATE FIELDS
  -- -------------------------------------------------------------------------
  SAFE_CAST(raw.Period_Start     AS DATE)                               AS period_start,
  SAFE_CAST(raw.Period_End       AS DATE)                               AS period_end,
  SAFE_CAST(raw.QGP_Week         AS DATE)                               AS qgp_week,

  -- -------------------------------------------------------------------------
  -- WEEK TYPE
  -- -------------------------------------------------------------------------
  SAFE_CAST(UPPER(TRIM(raw.week_type)) AS STRING)                       AS week_type,

  -- -------------------------------------------------------------------------
  -- LOB
  -- TBG is normalized to TFB — TBG is a TFB-only entity
  -- -------------------------------------------------------------------------
  CASE UPPER(TRIM(raw.LOB_Supported))
    WHEN 'TBG' THEN 'TFB'
    ELSE UPPER(TRIM(raw.LOB_Supported))
  END                                                                   AS lob,

  -- -------------------------------------------------------------------------
  -- CHANNEL GROUP MAPPING
  -- MFC channels → PulseTMS standard taxonomy
  -- Programmatic: DISPLAY, OLV, AUDIO always;
  -- OTT and OOH only when tactic contains PROGRAMMATIC
  -- -------------------------------------------------------------------------
  CASE
    WHEN UPPER(TRIM(raw.Channel)) = 'PAID SEARCH'
      THEN 'Paid Search'
    WHEN UPPER(TRIM(raw.Channel)) = 'PAID SOCIAL'
      THEN 'Paid Social'
    WHEN UPPER(TRIM(raw.Channel)) IN ('DISPLAY', 'OLV', 'AUDIO')
      THEN 'Programmatic'
    WHEN UPPER(TRIM(raw.Channel)) = 'OTT'
      AND UPPER(TRIM(raw.Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'Programmatic'
    WHEN UPPER(TRIM(raw.Channel)) = 'OOH'
      AND UPPER(TRIM(raw.Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'Programmatic'
    ELSE 'Other'
  END                                                                   AS channel_group,

  -- -------------------------------------------------------------------------
  -- GRANULAR MFC DIMENSIONS
  -- Preserved for MFC_SPEND_GRANULAR reporting in Silver/Gold
  -- -------------------------------------------------------------------------
  SAFE_CAST(UPPER(TRIM(raw.Channel))      AS STRING)                    AS channel,
  SAFE_CAST(UPPER(TRIM(raw.Tactic))       AS STRING)                    AS tactic,
  SAFE_CAST(UPPER(TRIM(raw.Message_Type)) AS STRING)                    AS message_type,
  SAFE_CAST(UPPER(TRIM(raw.Agency))       AS STRING)                    AS agency,

  -- -------------------------------------------------------------------------
  -- SPEND METRICS
  -- spend_actual   : NULL for future/unexecuted periods (expected behavior)
  -- spend_forecast : always populated
  -- spend_display intentionally excluded from PulseTMS pipeline
  -- For BOUNDARY_WEEK rows: values already apportioned — do NOT re-weight
  -- -------------------------------------------------------------------------
  SAFE_CAST(raw.spend_actual   AS FLOAT64)                              AS spend_actual,
  SAFE_CAST(raw.spend_forecast AS FLOAT64)                              AS spend_forecast,

  -- -------------------------------------------------------------------------
  -- AUDIT FIELDS
  -- -------------------------------------------------------------------------
  SAFE_CAST(raw.FileLoad_Date  AS DATE)                                 AS file_load_date

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` raw

-- -------------------------------------------------------------------------
-- EXCLUSION FILTERS
-- Scoped explicitly so pipeline stays correct when source view broadens
-- -------------------------------------------------------------------------
WHERE raw.Channel IS NOT NULL
  AND UPPER(TRIM(raw.Channel)) NOT IN (
    'OTHER (DO NOT USE)', 'NON-WORKING', 'UNALLOCATED', 'BUDGET HELD'
  )
  AND UPPER(TRIM(raw.Message_Type)) != 'MICRO'
  AND raw.spend_display IS NOT NULL
  AND raw.spend_display != 0
;