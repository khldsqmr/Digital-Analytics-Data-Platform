/* =================================================================================================
FILE:         07_sp_sdi_pulseTms_silver_platformSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_platformSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  Platform spend is actuals only — no forecast column available from this source.
  Grain: one row per qgp_date x lob x channel_group x metric_name.
  Includes 'All Channels' rollup per lob.
  LOBs: POSTPAID and BROADBAND (both present in agg_day_media_and_outcomes source).

BOUNDARY WEEK PRORATION:
  Platform Bronze only has Saturday rows (week_sun_sat). QGP BOUNDARY_STUB dates are
  non-Saturday quarter-end dates (e.g. Mar 31 Tuesday) — no direct Bronze row exists.
  To prorate correctly, the same pattern as Adobe Silver is used:
    BOUNDARY_STUB  : join to the next Saturday's (BOUNDARY_FIRST) Bronze row,
                     prorate by days_in_period / 7
    BOUNDARY_FIRST : join to its own Bronze row (the Saturday),
                     prorate by days_in_period / 7
    NORMAL         : full Bronze value, NULL if period incomplete

WoW LOGIC:
  NORMAL week      : numerator = current value
                     denominator = prior QGP date value
                     (if prior was BOUNDARY_FIRST: denominator = BOUNDARY_FIRST + its stub)
  BOUNDARY_STUB    : numerator = NULL, denominator = NULL (never a WoW point)
  BOUNDARY_FIRST   : numerator = current + preceding stub (COALESCE stub to 0 if NULL)
                     denominator = last NORMAL week before the stub

CHANGE LOG:
  - Removed lob = 'POSTPAID' filter from CROSS JOIN — now includes POSTPAID and BROADBAND.
  - Added BOUNDARY_STUB proration join to next Saturday's Bronze row (mirrors Adobe Silver).
    Previously BOUNDARY_STUB rows were always NULL for Platform spend.
  - Fixed BOUNDARY_FIRST wow/yoy numerator to use COALESCE(stub_lookup.metric_value, 0).
  - dim calendar column 'quarter' aliased as 'qgp_quarter' in output.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_platformSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly`
  PARTITION BY qgp_date
  CLUSTER BY lob, channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Platform spend long format with WoW/YoY. One row per qgp_date x lob x channel_group x metric_name. LOBs: POSTPAID, BROADBAND. Includes All Channels rollup per lob. Actuals only. BOUNDARY_STUB rows prorated from next Saturday Bronze row. Partitioned by qgp_date, clustered by lob, channel_group, metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_platformSpend_weekly.'
  )
  AS
  WITH
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                                         AS qgp_quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.prior_year_qgp_date,
      cal.prior_year_days_in_period,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.lob,
      channels.channel_group,

      -- Metric value logic mirrors Adobe Silver:
      --   BOUNDARY_STUB  : prorate full-week value (from next Saturday's Bronze row) × stub_days / 7
      --   BOUNDARY_FIRST : prorate full-week value (its own Saturday Bronze row) × first_days / 7
      --   NORMAL         : full Bronze value, NULL if period not yet complete
      CASE
        WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period
          THEN bf.spend * cal.days_in_period / 7
        WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period
          THEN b.spend  * cal.days_in_period / 7
        WHEN cal.is_complete_period
          THEN b.spend
        ELSE NULL
      END                                                                 AS spend

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal

    -- Cross join on POSTPAID and BROADBAND — no LOB filter so both flow through
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly`
      WHERE lob IN ('POSTPAID', 'BROADBAND')
    ) channels

    -- NORMAL and BOUNDARY_FIRST: join directly on the Saturday date
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly` b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.lob           = channels.lob
      AND b.channel_group = channels.channel_group

    -- BOUNDARY_STUB: join to the next Saturday (BOUNDARY_FIRST date) to get full week for proration
    -- Next Saturday from a non-Saturday date: add (7 - DAYOFWEEK) days
    -- DAYOFWEEK: Sun=1, Mon=2, ..., Sat=7; for a Tuesday (3): 7-3=4 days forward → Saturday ✅
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly` bf
      ON  cal.week_type   = 'BOUNDARY_STUB'
      AND bf.week_sun_sat = DATE_ADD(cal.qgp_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date)) DAY)
      AND bf.lob          = channels.lob
      AND bf.channel_group = channels.channel_group

    WHERE
      -- All historical quarters (fully complete)
      cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
      -- Full current quarter spine including future weeks so Tableau shows complete quarter
      OR (
        cal.qgp_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
        AND cal.qgp_date <= DATE_SUB(
              DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 3 MONTH),
              INTERVAL 1 DAY
            )
      )
  ),

  -- ---------------------------------------------------------------------------
  -- Channel-level unpivot (single metric: platformSpend)
  -- ---------------------------------------------------------------------------
  UnpivotedBase AS (
    SELECT
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter,
      wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year,
      lob, channel_group,
      'platformSpend' AS metric_name,
      spend           AS metric_value
    FROM BronzeWithCalendar
    WHERE lob IS NOT NULL
  ),

  -- ---------------------------------------------------------------------------
  -- All Channels rollup per lob
  -- Sums across channel_groups for each qgp_date x lob
  -- ---------------------------------------------------------------------------
  AllChannelsRollup AS (
    SELECT
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter,
      wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year,
      lob,
      'All Channels'    AS channel_group,
      metric_name,
      SUM(metric_value) AS metric_value
    FROM UnpivotedBase
    GROUP BY
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter,
      wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year,
      lob, metric_name
  ),

  Unpivoted AS (
    SELECT * FROM UnpivotedBase
    UNION ALL SELECT * FROM AllChannelsRollup
  ),

  -- ---------------------------------------------------------------------------
  -- Metric lookup for WoW/YoY self-joins
  -- ---------------------------------------------------------------------------
  MetricLookup AS (
    SELECT qgp_date, lob, channel_group, metric_name, metric_value
    FROM Unpivoted
  ),

  -- ---------------------------------------------------------------------------
  -- WoW / YoY computation
  -- ---------------------------------------------------------------------------
  WithWowYoy AS (
    SELECT
      u.qgp_date,
      u.week_type,
      u.qgp_quarter,
      u.days_in_period,
      u.is_complete_period,
      u.lob,
      u.channel_group,
      u.metric_name,
      u.metric_value,
      -- metric_value_ly normalized to current year days_in_period
      CASE
        WHEN ly_lookup.metric_value IS NULL                              THEN NULL
        WHEN ly_cal.prior_year_days_in_period IS NULL
          OR ly_cal.prior_year_days_in_period = 0                       THEN ly_lookup.metric_value
        ELSE ROUND(
          ly_lookup.metric_value
          * u.days_in_period
          / ly_cal.prior_year_days_in_period,
          2
        )
      END                                                                 AS metric_value_ly,

      -- WoW numerator:
      --   BOUNDARY_STUB  : NULL (never a WoW comparison point)
      --   BOUNDARY_FIRST : current + stub (combined 7-day equivalent)
      --                    COALESCE stub to 0 so NULL stub doesn't null the whole numerator
      --   NORMAL         : current value
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS wow_numerator,

      -- WoW denominator:
      --   BOUNDARY_STUB  : NULL
      --   If prior week was BOUNDARY_FIRST: prior BOUNDARY_FIRST + its stub
      --   Otherwise      : prior QGP date value
      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN wow_prior_stub.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                                 AS wow_denominator,

      -- YoY numerator: same logic as WoW numerator
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS yoy_numerator,

      -- YoY denominator:
      --   BOUNDARY_FIRST : prior year BOUNDARY_FIRST + its stub
      --   NORMAL         : prior year same ISO week value
      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN u.week_type = 'BOUNDARY_FIRST'
          THEN COALESCE(yoy_bf_lookup.metric_value, 0) + COALESCE(yoy_stub_lookup.metric_value, 0)
        ELSE COALESCE(ly_lookup.metric_value, 0)
      END                                                                 AS yoy_denominator

    FROM Unpivoted u

    -- WoW prior week value
    LEFT JOIN MetricLookup wow_prior_lookup
      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.lob           = u.lob
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name

    -- Prior year same ISO week value
    LEFT JOIN MetricLookup ly_lookup
      ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
      AND ly_lookup.lob           = u.lob
      AND ly_lookup.channel_group = u.channel_group
      AND ly_lookup.metric_name   = u.metric_name

    -- Stub value for BOUNDARY_FIRST rows (current period's preceding stub)
    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name

    -- Prior year BOUNDARY_FIRST value (for YoY on BOUNDARY_FIRST rows)
    LEFT JOIN MetricLookup yoy_bf_lookup
      ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
      AND yoy_bf_lookup.lob           = u.lob
      AND yoy_bf_lookup.channel_group = u.channel_group
      AND yoy_bf_lookup.metric_name   = u.metric_name

    -- Prior year calendar row — for yoy_stub_lookup and prior_year_days_in_period normalization
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
      ON  ly_cal.qgp_date = u.prior_year_qgp_date

    -- Prior year stub value (for YoY denominator on BOUNDARY_FIRST rows)
    LEFT JOIN MetricLookup yoy_stub_lookup
      ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
      AND yoy_stub_lookup.lob           = u.lob
      AND yoy_stub_lookup.channel_group = u.channel_group
      AND yoy_stub_lookup.metric_name   = u.metric_name

    -- Prior week calendar row — needed to check if prior week was BOUNDARY_FIRST
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
      ON  prior_cal.qgp_date = u.wow_prior_qgp_date

    -- If prior week was BOUNDARY_FIRST, also look up its stub for correct WoW denominator
    LEFT JOIN MetricLookup wow_prior_stub
      ON  wow_prior_stub.qgp_date      = prior_cal.boundary_stub_date
      AND wow_prior_stub.lob           = u.lob
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name   = u.metric_name
  )

  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    lob,
    channel_group,
    metric_name,
    metric_value,
    metric_value_ly,
    wow_numerator,
    wow_denominator,
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
         ELSE wow_numerator / wow_denominator - 1
    END                                                                   AS wow_pct,
    yoy_numerator,
    yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
         ELSE yoy_numerator / yoy_denominator - 1
    END                                                                   AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
      OVER (PARTITION BY lob, metric_name)                                AS max_date
  FROM WithWowYoy;

END;