
-- ###########################################################################
-- BEGIN gold/01_sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Overview cards, topline trend and funnel comparison ingredients.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- GOLD PRINCIPLE
-- Persist comparison INGREDIENTS, not precomputed percentages.
-- WoW / 4-week / LY comparisons join the appropriate weekly aggregates at the same
-- dimensional grain; do not cross-join raw visitors across weeks.
--
-- ----------------------------------------------------------------------------
-- OVERVIEW
-- Cards, topline trend, funnel cards, funnel over time.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long (
  targetWeekStartDate         DATE,
  targetWeekEndDate           DATE,
  fiscalQuarterLabel          STRING,
  fiscalWeekCode              STRING,
  weekLabel                   STRING,

  filterLob                   STRING COMMENT 'All for now; schema reserved for whole-report LOB filter contexts',
  filterPlatform              STRING COMMENT 'All for now; schema reserved for whole-report Platform filter contexts',

  metricName                  STRING,
  metricLabel                 STRING,
  metricKind                  STRING,
  displayFormat               STRING,
  changeUnit                  STRING,

  thisWeekNumerator           DOUBLE,
  thisWeekDenominator         DOUBLE,

  priorWeekNumerator          DOUBLE,
  priorWeekDenominator        DOUBLE,

  fourWeekTrendNumerator      DOUBLE,
  fourWeekTrendDenominator    DOUBLE,

  sameWeekLyNumerator         DOUBLE,
  sameWeekLyDenominator       DOUBLE,

  thisWeekDataAvailable       BOOLEAN,
  priorWeekDataAvailable      BOOLEAN,
  fourWeekTrendWeekCount      INT,
  sameWeekLyDataAvailable     BOOLEAN,

  peerSetNumerator            DOUBLE COMMENT 'Reserved; NULL until peer-set business definition is approved',
  peerSetDenominator          DOUBLE COMMENT 'Reserved; NULL until peer-set business definition is approved',

  _runId                      STRING,
  goldProcessedAt             TIMESTAMP
)
USING DELTA
CLUSTER BY (targetWeekStartDate, metricName)
COMMENT 'Gold Overview: comparison ingredients by target week and metric. Final percentages/pp are calculated later.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
  p_runId          STRING,
  p_asOfDate       DATE DEFAULT NULL,
  p_weeksToRebuild INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_weekTo      DATE DEFAULT date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
  DECLARE v_weekFrom    DATE DEFAULT date_add(v_weekTo, -7 * (p_weeksToRebuild - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  INSERT INTO sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
  REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
  WITH weeklyWide AS (
    SELECT
      weekStartDate,
      max(weekEndDate) AS weekEndDate,

      sum(nbv) AS nbv,
      sum(sessionCount) AS sessionCount,
      sum(pageViews) AS pageViews,
      sum(nbvBuyFlow) AS nbvBuyFlow,
      sum(nbvConfigure) AS nbvConfigure,
      sum(nbvCheckoutStart) AS nbvCheckoutStart,
      sum(orders) AS orders,
      sum(ordersAcquisition) AS ordersAcquisition,
      sum(ordersBase) AS ordersBase,
      sum(ordersUnassisted) AS ordersUnassisted,
      sum(ordersAssisted) AS ordersAssisted,
      sum(vrCalls) AS vrCalls,
      sum(vrChats) AS vrChats,
      sum(storeLocator) AS storeLocator,
      sum(orderCount) AS orderCount
    FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
    GROUP BY weekStartDate
  ),
  weeklyCounts AS (
    SELECT
      weekStartDate,
      weekEndDate,
      metricName,
      metricValue
    FROM weeklyWide
    LATERAL VIEW stack(
      15,
      'nbv',               cast(nbv AS DOUBLE),
      'sessionCount',      cast(sessionCount AS DOUBLE),
      'pageViews',         cast(pageViews AS DOUBLE),
      'nbvBuyFlow',        cast(nbvBuyFlow AS DOUBLE),
      'nbvConfigure',      cast(nbvConfigure AS DOUBLE),
      'nbvCheckoutStart',  cast(nbvCheckoutStart AS DOUBLE),
      'orders',            cast(orders AS DOUBLE),
      'ordersAcquisition', cast(ordersAcquisition AS DOUBLE),
      'ordersBase',        cast(ordersBase AS DOUBLE),
      'ordersUnassisted',  cast(ordersUnassisted AS DOUBLE),
      'ordersAssisted',    cast(ordersAssisted AS DOUBLE),
      'vrCalls',           cast(vrCalls AS DOUBLE),
      'vrChats',           cast(vrChats AS DOUBLE),
      'storeLocator',      cast(storeLocator AS DOUBLE),
      'orderCount',        cast(orderCount AS DOUBLE)
    ) s AS metricName, metricValue
  ),
  weeklyIngredients AS (
    -- Counts: denominator stays NULL; four-week count trends use fourWeekTrendWeekCount.
    SELECT
      c.weekStartDate,
      c.weekEndDate,
      c.metricName,
      c.metricValue AS numeratorValue,
      cast(NULL AS DOUBLE) AS denominatorValue
    FROM weeklyCounts c
    JOIN sdi_tbl_mip_control_metricCatalog_static m
      ON  m.metricName = c.metricName
      AND m.metricKind = 'count'
      AND m.isActive

    UNION ALL

    -- Ratios: keep the underlying count numerator/denominator.
    SELECT
      n.weekStartDate,
      max(n.weekEndDate) AS weekEndDate,
      r.metricName,
      max(CASE WHEN n.metricName = r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
      max(CASE WHEN n.metricName = r.denominatorMetric THEN n.metricValue END) AS denominatorValue
    FROM weeklyCounts n
    JOIN sdi_tbl_mip_control_metricCatalog_static r
      ON  r.metricKind = 'ratio'
      AND r.isActive
      AND n.metricName IN (r.numeratorMetric, r.denominatorMetric)
    GROUP BY n.weekStartDate, r.metricName
  ),
  availableWeeks AS (
    SELECT DISTINCT weekStartDate
    FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
  ),
  fourWeekAvailability AS (
    SELECT
      t.weekStartDate AS targetWeekStartDate,
      count(a.weekStartDate) AS fourWeekTrendWeekCount
    FROM sdi_tbl_mip_control_fiscalCalendar_static t
    LEFT JOIN availableWeeks a
      ON a.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
    WHERE t.weekStartDate BETWEEN v_weekFrom AND v_weekTo
    GROUP BY t.weekStartDate
  ),
  targetWeeks AS (
    SELECT
      t.*,
      (cur.weekStartDate IS NOT NULL) AS thisWeekDataAvailable,
      (pw.weekStartDate IS NOT NULL) AS priorWeekDataAvailable,
      cast(coalesce(fwa.fourWeekTrendWeekCount, 0) AS INT) AS fourWeekTrendWeekCount,
      (ly.weekStartDate IS NOT NULL) AS sameWeekLyDataAvailable
    FROM sdi_tbl_mip_control_fiscalCalendar_static t
    LEFT JOIN availableWeeks cur
      ON cur.weekStartDate = t.weekStartDate
    LEFT JOIN availableWeeks pw
      ON pw.weekStartDate = t.priorWeekStartDate
    LEFT JOIN fourWeekAvailability fwa
      ON fwa.targetWeekStartDate = t.weekStartDate
    LEFT JOIN availableWeeks ly
      ON ly.weekStartDate = t.sameWeekLastYearStartDate
    WHERE t.weekStartDate BETWEEN v_weekFrom AND v_weekTo
  )
  SELECT
    t.weekStartDate AS targetWeekStartDate,
    t.weekEndDate AS targetWeekEndDate,
    t.fiscalQuarterLabel,
    t.fiscalWeekCode,
    t.weekLabel,

    'All' AS filterLob,
    'All' AS filterPlatform,

    m.metricName,
    m.metricLabel,
    m.metricKind,
    m.displayFormat,
    m.changeUnit,

    CASE
      WHEN t.thisWeekDataAvailable THEN coalesce(cur.numeratorValue, 0D)
      ELSE NULL
    END AS thisWeekNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.thisWeekDataAvailable
        THEN coalesce(cur.denominatorValue, 0D)
      ELSE NULL
    END AS thisWeekDenominator,

    CASE
      WHEN t.priorWeekDataAvailable THEN coalesce(pw.numeratorValue, 0D)
      ELSE NULL
    END AS priorWeekNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.priorWeekDataAvailable
        THEN coalesce(pw.denominatorValue, 0D)
      ELSE NULL
    END AS priorWeekDenominator,

    CASE
      WHEN t.fourWeekTrendWeekCount > 0 THEN coalesce(sum(fw.numeratorValue), 0D)
      ELSE NULL
    END AS fourWeekTrendNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.fourWeekTrendWeekCount > 0
        THEN coalesce(sum(fw.denominatorValue), 0D)
      ELSE NULL
    END AS fourWeekTrendDenominator,

    CASE
      WHEN t.sameWeekLyDataAvailable THEN coalesce(ly.numeratorValue, 0D)
      ELSE NULL
    END AS sameWeekLyNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.sameWeekLyDataAvailable
        THEN coalesce(ly.denominatorValue, 0D)
      ELSE NULL
    END AS sameWeekLyDenominator,

    t.thisWeekDataAvailable,
    t.priorWeekDataAvailable,
    t.fourWeekTrendWeekCount,
    t.sameWeekLyDataAvailable,

    cast(NULL AS DOUBLE) AS peerSetNumerator,
    cast(NULL AS DOUBLE) AS peerSetDenominator,

    p_runId AS _runId,
    v_processedAt AS goldProcessedAt
  FROM targetWeeks t
  JOIN sdi_tbl_mip_control_metricCatalog_static m
    ON m.isActive
  LEFT JOIN weeklyIngredients cur
    ON  cur.weekStartDate = t.weekStartDate
    AND cur.metricName = m.metricName
  LEFT JOIN weeklyIngredients pw
    ON  pw.weekStartDate = t.priorWeekStartDate
    AND pw.metricName = m.metricName
  LEFT JOIN weeklyIngredients fw
    ON  fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
    AND fw.metricName = m.metricName
  LEFT JOIN weeklyIngredients ly
    ON  ly.weekStartDate = t.sameWeekLastYearStartDate
    AND ly.metricName = m.metricName
  GROUP BY
    t.weekStartDate,
    t.weekEndDate,
    t.fiscalQuarterLabel,
    t.fiscalWeekCode,
    t.weekLabel,
    m.metricName,
    m.metricLabel,
    m.metricKind,
    m.displayFormat,
    m.changeUnit,
    t.thisWeekDataAvailable,
    t.priorWeekDataAvailable,
    t.fourWeekTrendWeekCount,
    t.sameWeekLyDataAvailable,
    cur.numeratorValue,
    cur.denominatorValue,
    pw.numeratorValue,
    pw.denominatorValue,
    ly.numeratorValue,
    ly.denominatorValue;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END gold/01_sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long.sql
-- ###########################################################################

