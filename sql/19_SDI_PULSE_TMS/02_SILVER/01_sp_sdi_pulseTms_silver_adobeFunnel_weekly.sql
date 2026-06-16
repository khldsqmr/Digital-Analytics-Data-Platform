/* =================================================================================================
FILE:         05_sp_sdi_pulseTms_silver_adobeFunnel_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_adobeFunnel_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_adobeFunnel_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  Split into two steps to avoid BigQuery "query too complex" / resource exceeded errors:
    STEP 1 — Materialize unpivoted long-format rows into a TEMP TABLE.
             Joins Bronze to QGP calendar, applies completeness filter, unpivots 18 metrics.
             BOUNDARY_STUB rows: metric values prorated by days_in_period / 7
             using the next Saturday's (BOUNDARY_FIRST) full-week Bronze value.
             BOUNDARY_FIRST rows: metric values prorated by days_in_period / 7
             using its own full-week Bronze value.
    STEP 2 — Read from TEMP TABLE, compute WoW/YoY via self-joins, write final Silver table.

WoW LOGIC:
  NORMAL week      : numerator = current value
                     denominator = prior QGP date value
                     (if prior was BOUNDARY_FIRST: denominator = BOUNDARY_FIRST + its stub)
  BOUNDARY_STUB    : numerator = NULL, denominator = NULL (never a WoW comparison point)
  BOUNDARY_FIRST   : numerator = current value + preceding stub value (combined 7-day equivalent)
                     denominator = last NORMAL week before the stub

CHANGE LOG:
  - dim calendar column 'quarter' aliased as 'qgp_quarter' in output for consistency.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  -- ===========================================================================
  -- STEP 1: Materialize unpivoted rows into a session-scoped TEMP TABLE.
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE tmp_silver_adobe_unpivoted AS
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
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.channel_group,

      -- Metric value logic:
      --   BOUNDARY_STUB  : prorate full-week value (from BOUNDARY_FIRST Saturday) × stub_days / 7
      --   BOUNDARY_FIRST : prorate full-week value × first_days / 7
      --   NORMAL         : full Bronze value (NULL if period not yet complete)
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.upvTotalAdobe            * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.upvTotalAdobe             * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.upvTotalAdobe             END AS upvTotalAdobe,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.upvPostpaid              * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.upvPostpaid               * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.upvPostpaid               END AS upvPostpaid,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.upvHsi                   * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.upvHsi                    * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.upvHsi                    END AS upvHsi,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.upvByod                  * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.upvByod                   * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.upvByod                   END AS upvByod,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.upvFlowTotal             * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.upvFlowTotal              * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.upvFlowTotal              END AS upvFlowTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.cartstartPostpaid        * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.cartstartPostpaid         * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.cartstartPostpaid         END AS cartstartPostpaid,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.cartstartHsi             * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.cartstartHsi              * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.cartstartHsi              END AS cartstartHsi,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.cartstartByod            * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.cartstartByod             * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.cartstartByod             END AS cartstartByod,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersUnassistedPostpaid * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersUnassistedPostpaid  * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersUnassistedPostpaid  END AS ordersUnassistedPostpaid,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersUnassistedHsi      * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersUnassistedHsi       * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersUnassistedHsi       END AS ordersUnassistedHsi,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersUnassistedByod     * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersUnassistedByod      * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersUnassistedByod      END AS ordersUnassistedByod,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersAssistedPostpaid   * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersAssistedPostpaid    * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersAssistedPostpaid    END AS ordersAssistedPostpaid,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersAssistedHsi        * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersAssistedHsi         * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersAssistedHsi         END AS ordersAssistedHsi,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.ordersAssistedByod       * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.ordersAssistedByod        * cal.days_in_period / 7
           WHEN cal.is_complete_period                                       THEN b.ordersAssistedByod        END AS ordersAssistedByod,
      -- Derived totals
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.cartstartPostpaid + bf.cartstartHsi + bf.cartstartByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.cartstartPostpaid + b.cartstartHsi + b.cartstartByod) * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.cartstartPostpaid + b.cartstartHsi + b.cartstartByod           END AS cartstartTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersUnassistedPostpaid + bf.ordersUnassistedHsi + bf.ordersUnassistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod) * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod END AS ordersUnassistedTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersAssistedPostpaid + bf.ordersAssistedHsi + bf.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod END AS ordersAssistedTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersUnassistedPostpaid + bf.ordersUnassistedHsi + bf.ordersUnassistedByod + bf.ordersAssistedPostpaid + bf.ordersAssistedHsi + bf.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod + b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod) + (b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod) END AS ordersTotal

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    -- Cross join on distinct channel_groups so every QGP date has a row per channel
    -- even for future weeks with no Bronze data yet
    CROSS JOIN (
      SELECT DISTINCT channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly`
    ) channels
    -- NORMAL and BOUNDARY_FIRST: join on the Saturday date directly
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.channel_group = channels.channel_group
    -- BOUNDARY_STUB: join on the next Saturday (BOUNDARY_FIRST date) to get full week for proration
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` bf
      ON  cal.week_type   = 'BOUNDARY_STUB'
      AND bf.week_sun_sat = DATE_ADD(cal.qgp_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date)) DAY)
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
  )
  -- Unpivot all 18 metrics to long format
  SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvPostpaid'             AS metric_name, upvPostpaid             AS metric_value FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvHsi',                  upvHsi                  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvByod',                 upvByod                 FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvFlowTotal',            upvFlowTotal            FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvTotalAdobe',           upvTotalAdobe           FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartPostpaid',       cartstartPostpaid       FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartHsi',            cartstartHsi            FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartByod',           cartstartByod           FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartTotal',          cartstartTotal          FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedPostpaid',ordersUnassistedPostpaid FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedHsi',     ordersUnassistedHsi     FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedByod',    ordersUnassistedByod    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedTotal',   ordersUnassistedTotal   FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedPostpaid',  ordersAssistedPostpaid  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedHsi',       ordersAssistedHsi       FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedByod',      ordersAssistedByod      FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedTotal',     ordersAssistedTotal     FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersTotal',             ordersTotal             FROM BronzeWithCalendar WHERE channel_group IS NOT NULL;


  -- ===========================================================================
  -- STEP 2: WoW / YoY joins against the TEMP TABLE, write final Silver table.
  -- ===========================================================================
  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Adobe UPV funnel metrics in long format with WoW/YoY. One row per qgp_date x channel_group x metric_name. Partitioned by qgp_date, clustered by channel_group and metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_adobeFunnel_weekly.'
  )
  AS
  WITH
  MetricLookup AS (
    SELECT qgp_date, channel_group, metric_name, metric_value
    FROM tmp_silver_adobe_unpivoted
  )
  SELECT
    u.qgp_date,
    u.week_type,
    u.qgp_quarter,
    u.days_in_period,
    u.is_complete_period,
    u.channel_group,
    u.metric_name,
    u.metric_value,
    ly_lookup.metric_value                                                AS metric_value_ly,

    -- WoW numerator:
    --   BOUNDARY_STUB  : NULL (never a WoW comparison point)
    --   BOUNDARY_FIRST : current + stub (forms complete 7-day equivalent)
    --   NORMAL         : current value
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS wow_numerator,

    -- WoW denominator:
    --   BOUNDARY_STUB  : NULL
    --   If prior week was BOUNDARY_FIRST: prior BOUNDARY_FIRST + its stub (full 7-day equivalent)
    --   Otherwise     : prior QGP date value
    CASE
      WHEN u.metric_value IS NULL         THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB'  THEN NULL
      WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
        THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_lookup.metric_value, 0)
      ELSE COALESCE(wow_prior_lookup.metric_value, 0)
    END                                                                   AS wow_denominator,

    -- WoW pct
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN
        CASE WHEN wow_prior_lookup.metric_value IS NULL OR wow_prior_lookup.metric_value = 0 THEN NULL
             ELSE (u.metric_value + COALESCE(stub_lookup.metric_value, 0)) / wow_prior_lookup.metric_value - 1
        END
      ELSE
        CASE
          WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
            THEN CASE WHEN (wow_prior_lookup.metric_value + wow_prior_stub_lookup.metric_value) = 0 THEN NULL
                      ELSE u.metric_value / (wow_prior_lookup.metric_value + wow_prior_stub_lookup.metric_value) - 1
                 END
          WHEN wow_prior_lookup.metric_value IS NULL OR wow_prior_lookup.metric_value = 0 THEN NULL
          ELSE u.metric_value / wow_prior_lookup.metric_value - 1
        END
    END                                                                   AS wow_pct,

    -- YoY numerator: same as WoW numerator logic
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS yoy_numerator,

    -- YoY denominator:
    --   BOUNDARY_FIRST : prior year BOUNDARY_FIRST + its stub
    --   NORMAL         : prior year same ISO week value
    CASE
      WHEN u.metric_value IS NULL         THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB'  THEN NULL
      WHEN u.week_type = 'BOUNDARY_FIRST' THEN COALESCE(yoy_bf_lookup.metric_value, 0) + COALESCE(yoy_stub_lookup.metric_value, 0)
      ELSE COALESCE(ly_lookup.metric_value, 0)
    END                                                                   AS yoy_denominator,

    -- YoY pct
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN
        CASE WHEN (yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value) IS NULL
                  OR (yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value) = 0 THEN NULL
             ELSE (u.metric_value + COALESCE(stub_lookup.metric_value, 0))
                  / (yoy_bf_lookup.metric_value + COALESCE(yoy_stub_lookup.metric_value, 0)) - 1
        END
      ELSE
        CASE WHEN ly_lookup.metric_value IS NULL OR ly_lookup.metric_value = 0 THEN NULL
             ELSE u.metric_value / ly_lookup.metric_value - 1
        END
    END                                                                   AS yoy_pct,

    MAX(CASE WHEN u.metric_value IS NOT NULL THEN u.qgp_date END)
      OVER (PARTITION BY u.metric_name)                                   AS max_date

  FROM tmp_silver_adobe_unpivoted u

  -- WoW prior week value
  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  -- Prior week calendar row — needed to check if prior week was BOUNDARY_FIRST
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
    ON  prior_cal.qgp_date = u.wow_prior_qgp_date

  -- If prior week was BOUNDARY_FIRST, also look up its stub value for correct denominator
  LEFT JOIN MetricLookup wow_prior_stub_lookup
    ON  wow_prior_stub_lookup.qgp_date      = prior_cal.boundary_stub_date
    AND wow_prior_stub_lookup.channel_group  = u.channel_group
    AND wow_prior_stub_lookup.metric_name    = u.metric_name

  -- Prior year same ISO week value
  LEFT JOIN MetricLookup ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name

  -- Stub value for BOUNDARY_FIRST rows (current period's preceding stub)
  LEFT JOIN MetricLookup stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  -- Prior year BOUNDARY_FIRST value (for YoY on BOUNDARY_FIRST rows)
  LEFT JOIN MetricLookup yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.metric_name   = u.metric_name

  -- Prior year calendar row — needed to find prior year stub date
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date

  -- Prior year stub value (for YoY denominator on BOUNDARY_FIRST rows)
  LEFT JOIN MetricLookup yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.metric_name   = u.metric_name;

END;