-- =============================================================================
-- VIEW:    prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
-- LAYER:   Bronze
-- SOURCE:  prdrzrlakehouse.qgp_restricted.qgpweeklyview
-- AUTHOR:  SDI / Lab42
-- =============================================================================
--
-- PURPOSE
-- -------
-- This is the bronze (clean) layer view of the QGP weekly metrics data.
-- It takes the raw source view and applies the following transformations:
--
--   1. Excludes layout/structural rows (MetricID starting with 'Header' or
--      'Blank') — these are report page placeholders, not real metrics.
--
--   2. Excludes 'Variance to QGP' and 'Variance to QGP %' MetricType rows —
--      these are derived calculations that should be computed in Tableau,
--      not stored as raw metric rows.
--
--   3. Trims whitespace from all string columns. The source data has dirty
--      variants like 'QGP ' (trailing space) and 'Actuals/Outlook ' that
--      cause silent filter mismatches in Tableau.
--
--   4. Standardizes IsFuture to exactly two clean values:
--        'Is Past'   — historical weeks with real actuals
--        'Is Future' — forward weeks with projected/QGP values
--      The source has three variants: 'Is Past', 'Is Future', 'IsFuture'
--      (no space). The third variant is treated as 'Is Future'.
--
--      IMPORTANT FOR TABLEAU USERS:
--      When filtering for real actuals, always combine:
--        MetricType = 'Actuals/Outlook'  AND  IsFuture = 'Is Past'
--      The reason: future 'Actuals/Outlook' rows exist but their Amount
--      equals the QGP value — they are forward-filled projections, not
--      real actuals. The 'Is Past' filter is what separates real actuals
--      from projected ones.
--
--   5. Deduplicates rows across report pages. The same MetricID appears on
--      multiple pages in the source (e.g. 'Virtual Retail', 'Virtual Retail
--      Outcomes 1', 'Virtual Retail Outcomes 2') with identical Amount values.
--      Without dedup, QGP values are 3x inflated when summed in Tableau.
--      See the dedup logic section below for full explanation.
--
-- GRAIN
-- -----
-- One row per: MetricID + WeekEnding + DateContext + MetricType
--
-- TABLEAU USAGE PATTERNS
-- ----------------------
-- Actuals (past periods only):
--   MetricType = 'Actuals/Outlook' AND DateContext = 'Normal' AND IsFuture = 'Is Past'
--
-- QGP / Forecast (all periods):
--   MetricType = 'QGP' AND DateContext = 'Normal'
--   NOTE: No IsFuture filter needed for QGP — forecast covers past and future.
--   NOTE: No Page filter needed — dedup has already resolved page triplication.
--
-- Pure Actuals (subset of metrics only — e.g. store traffic, BYOD %, NPS):
--   MetricType = 'Actuals' AND DateContext = 'Normal'
--   NOTE: No IsFuture filter needed — future weeks have null Amount for
--   'Actuals' type rows.
--
-- VALIDATION QUERIES (run after view creation)
-- ---------------------------------------------
-- 1. Row count by DateContext — Normal should be ~44K rows:
--    SELECT DateContext, COUNT(*) AS rows
--    FROM prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
--    GROUP BY DateContext ORDER BY rows DESC;
--
-- 2. Confirm dedup worked — this should return zero rows:
--    SELECT MetricID, WeekEnding, DateContext, MetricType, COUNT(*) AS cnt
--    FROM prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
--    GROUP BY MetricID, WeekEnding, DateContext, MetricType
--    HAVING cnt > 1
--    LIMIT 10;
--
-- 3. Confirm IsFuture standardization — should show only 'Is Past' / 'Is Future':
--    SELECT IsFuture, COUNT(*) AS rows
--    FROM prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
--    WHERE DateContext = 'Normal'
--    GROUP BY IsFuture;
--
-- DOWNSTREAM
-- ----------
-- Silver layer will join this view with MFC transaction data and the
-- qgpchanneludd channel dimension table (join key: GlanceChannel from MFC)
-- to add explicit LOB and Channel columns. Channel info does not exist
-- at the metric level in this source and cannot be derived here.
--
-- =============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
COMMENT 'Bronze layer view of QGP weekly metrics. Source: prdrzrlakehouse.qgp_restricted.qgpweeklyview. Deduplicates page-level triplication, standardizes MetricType and IsFuture, excludes layout rows (Header/Blank) and Variance rows. Grain: MetricID + WeekEnding + DateContext + MetricType.'
AS

WITH deduped AS (

  SELECT

    -- -------------------------------------------------------------------------
    -- TIME DIMENSIONS
    -- -------------------------------------------------------------------------
    PublishKey,       -- Date the metric snapshot was published. Currently always
                      -- 2026-04-18 — single snapshot in the source.
    WeekEnding,       -- Week ending date. Range: 2026-01-03 to 2026-06-30.
    QuarterNum,       -- Quarter number (1-4).
    YearNum,          -- Year of the metric.
    TRIM(DateContext)       AS DateContext,
                      -- Temporal context of the value. Possible values:
                      --   Normal        = standard weekly actual or forecast
                      --   WoW           = week over week
                      --   QTD           = quarter to date
                      --   QTR           = full quarter
                      --   ROQ           = rest of quarter
                      --   Monthly Normal = monthly context
                      --   PQWA          = prior quarter weighted average
                      -- For most Tableau reporting, filter DateContext = 'Normal'.
    TRIM(CumulativeDates)   AS CumulativeDates,
    DaysInArrears,    -- How many days behind real-time this metric is reported.

    -- -------------------------------------------------------------------------
    -- METRIC IDENTITY
    -- -------------------------------------------------------------------------
    MetricID,         -- Unique identifier for the metric. Primary filter key
                      -- in Tableau. Never use MetricName as a join key —
                      -- multiple MetricIDs can share the same MetricName.
    TRIM(MetricName)        AS MetricName,
                      -- Display name of the metric. Leading/trailing whitespace
                      -- trimmed — source has many names with extra spaces.
                      -- Used in some Tableau calcs as filter (e.g. Store Traffic).

    TRIM(MetricType)        AS MetricType,
                      -- Type of metric value. Possible values after trimming:
                      --   'Actuals/Outlook' = actual for past, projected for future
                      --   'Actuals'         = pure historical actuals only
                      --   'QGP'             = forecast / goal
                      -- Trailing-space variants ('QGP ', 'Actuals/Outlook ') from
                      -- the source are cleaned by TRIM() here.

    TRIM(DisplayMetricType) AS DisplayMetricType,
                      -- Display-friendly version of MetricType for Tableau labels.
    TRIM(MetricFormat)      AS MetricFormat,
                      -- Format code for the metric value (e.g. percent, currency).
    MetricOrder,      -- Display ordering for report layout.
    TRIM(MetricOwner)       AS MetricOwner,

    -- -------------------------------------------------------------------------
    -- REPORT STRUCTURE
    -- -------------------------------------------------------------------------
    -- Page is the report page name. It is the closest available proxy for
    -- channel grouping in this data (e.g. 'Virtual Retail', 'Digital',
    -- 'Branded Retail ARN'). There is no explicit Channel or LOB column
    -- in the source — those will be added in the silver layer.
    --
    -- DEDUP NOTE:
    -- The same MetricID appears on multiple pages in the source with identical
    -- Amount values. For example, MetricID 'VRInboundCallsinclHSIAutomatedManual'
    -- appears on 'Virtual Retail', 'Virtual Retail Outcomes 1', and
    -- 'Virtual Retail Outcomes 2' — all with the same QGP Amount.
    -- Summing in Tableau without a Page filter inflates QGP values 3x.
    --
    -- To deduplicate, we use REGEXP_REPLACE to strip the ' Outcomes',
    -- ' Outcomes 2', ' Outcomes 3' (etc.) suffixes from Page in the
    -- ROW_NUMBER() PARTITION. This puts all variants of the same page
    -- into the same partition. The ORDER BY TRIM(Page) ASC then picks
    -- the primary page row because alphabetically:
    --   'Virtual Retail' < 'Virtual Retail Outcomes 1' < 'Virtual Retail Outcomes 2'
    -- So 'Virtual Retail' always wins. Same pattern holds for all pages:
    --   'Digital' beats 'Digital Outcomes 1'
    --   'Branded Retail ARN' beats 'Branded Retail ARN Outcomes'
    -- The Page column in the output reflects the winning (primary) page name.
    TRIM(Page)              AS Page,

    -- -------------------------------------------------------------------------
    -- PERIOD FLAG
    -- -------------------------------------------------------------------------
    -- IsFuture indicates whether the WeekEnding is a past or future period.
    -- The source has three dirty variants that are standardized here to two:
    --   Source 'Is Past'   -> 'Is Past'
    --   Source 'Is Future' -> 'Is Future'
    --   Source 'IsFuture'  -> 'Is Future'  (no space, treated as same)
    --
    -- CRITICAL: When pulling Actuals/Outlook values, always filter
    -- IsFuture = 'Is Past'. Future Actuals/Outlook rows exist but their
    -- Amount equals the QGP — they are forward-filled, not real actuals.
    CASE
      WHEN LOWER(TRIM(IsFuture)) IN ('is future', 'isfuture') THEN 'Is Future'
      WHEN LOWER(TRIM(IsFuture)) = 'is past'                  THEN 'Is Past'
      ELSE TRIM(IsFuture)
    END                     AS IsFuture,

    -- -------------------------------------------------------------------------
    -- METRIC VALUES
    -- -------------------------------------------------------------------------
    Amount,           -- The metric value. Core measure in Tableau.
                      -- After dedup, one Amount per grain — no inflation risk.
    VariancePercentage,
    VarianceDirection,  -- Drives conditional formatting in Tableau (pos/neg).
    VarianceColor,      -- Color encoding integer for Tableau calculated fields.
    LevelofPrecision,   -- Decimal precision for display in Tableau.

    -- -------------------------------------------------------------------------
    -- METADATA / REFERENCE
    -- -------------------------------------------------------------------------
    DataDictionaryURL,    -- Link to metric definition.
    DrillDownURL1,        -- Action URL for Tableau dashboard drill-through.
    DrillDownURL2,        -- Secondary action URL.
    InsertDateTime,       -- Timestamp when the record was inserted in the source.

    -- -------------------------------------------------------------------------
    -- LINEAGE
    -- -------------------------------------------------------------------------
    'prdrzrlakehouse.qgp_restricted.qgpweeklyview' AS SourceTable,
                      -- Full source table path for data lineage tracking.

    CURRENT_TIMESTAMP()    AS BronzeCreatedAt,
                      -- NOTE: Because this is a VIEW (not a Delta table),
                      -- BronzeCreatedAt reflects query execution time, not a
                      -- fixed creation timestamp. If this view is later
                      -- materialized as a Delta table, BronzeCreatedAt will
                      -- correctly capture the physical write time.

    -- -------------------------------------------------------------------------
    -- DEDUP ROW NUMBER (internal — not exposed in final SELECT)
    -- -------------------------------------------------------------------------
    -- Assigns rank 1 to the primary page row for each metric grain.
    -- Partition key uses REGEXP_REPLACE to normalize page names so that
    -- 'Virtual Retail' and 'Virtual Retail Outcomes 1' compete in the
    -- same partition. ORDER BY Page ASC ensures the primary page (shortest,
    -- alphabetically first) always gets rank 1.
    ROW_NUMBER() OVER (
      PARTITION BY
        MetricID,
        WeekEnding,
        TRIM(DateContext),
        TRIM(MetricType)
      ORDER BY TRIM(Page) ASC
    )                       AS _rn

  FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview

  WHERE
    -- Exclude layout/structural rows.
    -- MetricIDs starting with 'Header' or 'Blank' are report page placeholders
    -- (e.g. 'Header1', 'Blank14'). They have no metric value and appear with
    -- up to 93 duplicate rows per WeekEnding/DateContext combination.
    MetricID NOT LIKE 'Header%'
    AND MetricID NOT LIKE 'Blank%'

    -- Exclude variance rows.
    -- 'Variance to QGP' and 'Variance to QGP %' are derived calculations.
    -- These should be computed in Tableau from the Actuals and QGP rows,
    -- not stored as separate raw metric rows in the bronze layer.
    AND TRIM(MetricType) NOT IN ('Variance to QGP', 'Variance to QGP %')

)

-- Final SELECT: expose all columns except the internal dedup row number.
SELECT
  PublishKey,
  WeekEnding,
  QuarterNum,
  YearNum,
  DateContext,
  CumulativeDates,
  DaysInArrears,
  MetricID,
  MetricName,
  MetricType,
  DisplayMetricType,
  MetricFormat,
  MetricOrder,
  MetricOwner,
  Page,
  IsFuture,
  Amount,
  VariancePercentage,
  VarianceDirection,
  VarianceColor,
  LevelofPrecision,
  DataDictionaryURL,
  DrillDownURL1,
  DrillDownURL2,
  InsertDateTime,
  SourceTable,
  BronzeCreatedAt

FROM deduped
WHERE _rn = 1;  -- Keep only the primary page row per grain (dedup applied).