/* =================================================================================================
FILE:         05_sp_sdi_pulseTms_silver_adobeFunnel_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_adobeFunnel_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_adobeFunnel_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  All heavy processing happens here — Gold is a pure pass-through view.

STRUCTURE:
  STEP 1 — Bronze + Calendar join → prorate metrics → unpivot 18 volume metrics to long
           Materialized into tmp_silver_adobe_unpivoted (session-scoped TEMP TABLE).
           Using a temp table avoids BigQuery "query too complex" errors on the self-joins
           in STEP 2 and STEP 3.

  STEP 2 — WoW / YoY computation on volume metrics via self-joins against the temp table.
           Produces volume rows (metric_type = ADOBE_VOLUME).

  STEP 3 — CVR computation from the same temp table.
           Pivot wide per qgp_date x channel_group → compute 17 CVRs → unpivot to long.
           CVR rows carry adobe_cvr_numerator and adobe_cvr_denominator so Tableau can
           compute weekly, QTD, or quarterly CVR correctly as:
             SUM(adobe_cvr_numerator) / SUM(adobe_cvr_denominator)
           at any aggregation grain. metric_value is NULL for CVR rows.

  STEP 4 — UNION volume rows (STEP 2) + CVR rows (STEP 3) → write final Silver table.

BOUNDARY WEEK HANDLING:
  BOUNDARY_STUB  : metric prorated from next Saturday Bronze row × stub_days / 7
  BOUNDARY_FIRST : metric prorated from its own Saturday Bronze row × first_days / 7
  NORMAL         : full Bronze value, NULL if period not yet complete
  For CVRs: proration cancels in the ratio, so CVR is meaningful for all week types.
  WoW/YoY NULL for BOUNDARY_STUB and all CVR rows.

WoW LOGIC (volume metrics only):
  NORMAL         : numerator = current value
                   denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub

CVR DEFINITIONS:
  cvrUpvFlow                = upvFlowTotal      / upvTotalAdobe
  cvrUpvPostpaid            = upvPostpaid       / upvFlowTotal
  cvrUpvHsi                 = upvHsi            / upvFlowTotal
  cvrUpvByod                = upvByod           / upvFlowTotal
  cvrCartstartTotal         = cartstartTotal    / upvFlowTotal
  cvrCartstartPostpaid      = cartstartPostpaid / upvPostpaid
  cvrCartstartHsi           = cartstartHsi      / upvHsi
  cvrCartstartByod          = cartstartByod     / upvByod
  cvrOrdersTotal            = ordersTotal       / upvFlowTotal
  cvrOrdersUnassistedTotal  = ordersUnassistedTotal  / upvFlowTotal
  cvrOrdersAssistedTotal    = ordersAssistedTotal    / upvFlowTotal
  cvrOrdersUnassistedPostpaid = ordersUnassistedPostpaid / upvPostpaid
  cvrOrdersAssistedPostpaid   = ordersAssistedPostpaid   / upvPostpaid
  cvrOrdersUnassistedHsi      = ordersUnassistedHsi      / upvHsi
  cvrOrdersAssistedHsi        = ordersAssistedHsi        / upvHsi
  cvrOrdersUnassistedByod     = ordersUnassistedByod     / upvByod
  cvrOrdersAssistedByod       = ordersAssistedByod       / upvByod

CHANGE LOG:
  - Added STEP 3: CVR computation with adobe_cvr_numerator / adobe_cvr_denominator columns.
  - Added metric_type column: 'ADOBE_VOLUME' for volume rows, 'CVR' for CVR rows.
  - Added adobe_cvr_numerator, adobe_cvr_denominator columns (NULL for volume rows).
  - dim calendar column 'quarter' aliased as 'qgp_quarter'.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  -- ===========================================================================
  -- STEP 1: Bronze + Calendar join → prorate → unpivot 18 volume metrics.
  --         Materialized as a TEMP TABLE so STEP 2 and STEP 3 self-joins
  --         are resolved in one pass — no CTE fan-out, no planner explosion.
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE tmp_silver_adobe_unpivoted AS
  WITH

  -- ---------------------------------------------------------------------------
  -- 1A: Join Bronze to QGP calendar, prorate metrics for boundary weeks.
  --     One wide row per qgp_date x channel_group.
  -- ---------------------------------------------------------------------------
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                                          AS qgp_quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.prior_year_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.channel_group,

      -- Proration logic:
      --   BOUNDARY_STUB  : use next Saturday (bf) Bronze row × stub_days / 7
      --   BOUNDARY_FIRST : use own Saturday (b) Bronze row × first_days / 7
      --   NORMAL         : full Bronze value, NULL if period not yet complete
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
      -- Derived totals (sum of sub-metrics, same proration pattern)
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.cartstartPostpaid + bf.cartstartHsi + bf.cartstartByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.cartstartPostpaid  + b.cartstartHsi  + b.cartstartByod)  * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.cartstartPostpaid + b.cartstartHsi + b.cartstartByod           END AS cartstartTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersUnassistedPostpaid + bf.ordersUnassistedHsi + bf.ordersUnassistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersUnassistedPostpaid  + b.ordersUnassistedHsi  + b.ordersUnassistedByod)  * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod END AS ordersUnassistedTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersAssistedPostpaid + bf.ordersAssistedHsi + bf.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersAssistedPostpaid  + b.ordersAssistedHsi  + b.ordersAssistedByod)  * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod END AS ordersAssistedTotal,
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN (bf.ordersUnassistedPostpaid + bf.ordersUnassistedHsi + bf.ordersUnassistedByod + bf.ordersAssistedPostpaid + bf.ordersAssistedHsi + bf.ordersAssistedByod) * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN (b.ordersUnassistedPostpaid  + b.ordersUnassistedHsi  + b.ordersUnassistedByod  + b.ordersAssistedPostpaid  + b.ordersAssistedHsi  + b.ordersAssistedByod)  * cal.days_in_period / 7
           WHEN cal.is_complete_period THEN (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod) + (b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod) END AS ordersTotal

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    -- Every QGP date gets a row per channel_group, even future weeks with no Bronze data
    CROSS JOIN (
      SELECT DISTINCT channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly`
    ) channels
    -- NORMAL / BOUNDARY_FIRST: direct join on Saturday date
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.channel_group = channels.channel_group
    -- BOUNDARY_STUB: join to next Saturday Bronze row for proration
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` bf
      ON  cal.week_type    = 'BOUNDARY_STUB'
      AND bf.week_sun_sat  = DATE_ADD(cal.qgp_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date)) DAY)
      AND bf.channel_group = channels.channel_group
    WHERE
      cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
      OR (
        cal.qgp_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
        AND cal.qgp_date <= DATE_SUB(
              DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 3 MONTH),
              INTERVAL 1 DAY
            )
      )
  ),

  -- ---------------------------------------------------------------------------
  -- 1B: Unpivot 18 volume metrics from wide to long format.
  --     One row per qgp_date x channel_group x metric_name.
  -- ---------------------------------------------------------------------------
  VolumeUnpivoted AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvPostpaid'              AS metric_name, upvPostpaid              AS metric_value FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvHsi',                   upvHsi                   FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvByod',                  upvByod                  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvFlowTotal',             upvFlowTotal             FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvTotalAdobe',            upvTotalAdobe            FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartPostpaid',        cartstartPostpaid        FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartHsi',             cartstartHsi             FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartByod',            cartstartByod            FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartTotal',           cartstartTotal           FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedPostpaid', ordersUnassistedPostpaid FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedHsi',      ordersUnassistedHsi      FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedByod',     ordersUnassistedByod     FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedTotal',    ordersUnassistedTotal    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedPostpaid',   ordersAssistedPostpaid   FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedHsi',        ordersAssistedHsi        FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedByod',       ordersAssistedByod       FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedTotal',      ordersAssistedTotal      FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersTotal',              ordersTotal              FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  )

  SELECT * FROM VolumeUnpivoted;


  -- ===========================================================================
  -- STEP 2: WoW / YoY computation on volume metrics.
  --         Self-joins against the materialized temp table.
  --         Produces rows with metric_type = 'ADOBE_VOLUME'.
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE tmp_silver_adobe_volume AS
  WITH

  -- ---------------------------------------------------------------------------
  -- 2A: Metric lookup — used for all self-joins below.
  --     Simple projection from temp table for join clarity.
  -- ---------------------------------------------------------------------------
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
    'ADOBE_VOLUME'                                                        AS metric_type,
    u.metric_value,
    ly_lookup.metric_value                                                AS metric_value_ly,

    -- -----------------------------------------------------------------
    -- WoW numerator:
    --   BOUNDARY_STUB  : NULL — never a WoW comparison point
    --   BOUNDARY_FIRST : current + stub → combined 7-day equivalent
    --   NORMAL         : current value
    -- -----------------------------------------------------------------
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS wow_numerator,

    -- -----------------------------------------------------------------
    -- WoW denominator:
    --   BOUNDARY_STUB  : NULL
    --   If prior week was BOUNDARY_FIRST: prior BF + its stub
    --   Otherwise      : prior QGP date value
    -- -----------------------------------------------------------------
    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
        THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_lookup.metric_value, 0)
      ELSE COALESCE(wow_prior_lookup.metric_value, 0)
    END                                                                   AS wow_denominator,

    -- -----------------------------------------------------------------
    -- WoW pct: pre-computed for Tableau convenience
    -- -----------------------------------------------------------------
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

    -- -----------------------------------------------------------------
    -- YoY numerator: same logic as WoW numerator
    -- -----------------------------------------------------------------
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS yoy_numerator,

    -- -----------------------------------------------------------------
    -- YoY denominator:
    --   BOUNDARY_FIRST : prior year BF + its stub
    --   NORMAL         : prior year same ISO week value
    -- -----------------------------------------------------------------
    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      WHEN u.week_type = 'BOUNDARY_FIRST'
        THEN COALESCE(yoy_bf_lookup.metric_value, 0) + COALESCE(yoy_stub_lookup.metric_value, 0)
      ELSE COALESCE(ly_lookup.metric_value, 0)
    END                                                                   AS yoy_denominator,

    -- -----------------------------------------------------------------
    -- YoY pct: pre-computed for Tableau convenience
    -- -----------------------------------------------------------------
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
      OVER (PARTITION BY u.metric_name)                                   AS max_date,

    -- CVR columns NULL for volume rows
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_numerator,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_denominator

  FROM tmp_silver_adobe_unpivoted u

  -- WoW prior week value
  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  -- Prior week calendar row — to check if prior week was BOUNDARY_FIRST
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
    ON  prior_cal.qgp_date = u.wow_prior_qgp_date

  -- If prior week was BOUNDARY_FIRST, also look up its stub for correct WoW denominator
  LEFT JOIN MetricLookup wow_prior_stub_lookup
    ON  wow_prior_stub_lookup.qgp_date      = prior_cal.boundary_stub_date
    AND wow_prior_stub_lookup.channel_group = u.channel_group
    AND wow_prior_stub_lookup.metric_name   = u.metric_name

  -- Prior year same ISO week value
  LEFT JOIN MetricLookup ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name

  -- Stub value for BOUNDARY_FIRST rows (preceding stub in same quarter)
  LEFT JOIN MetricLookup stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  -- Prior year BOUNDARY_FIRST value
  LEFT JOIN MetricLookup yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.metric_name   = u.metric_name

  -- Prior year calendar row — to find prior year stub date
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date

  -- Prior year stub value (for YoY denominator on BOUNDARY_FIRST rows)
  LEFT JOIN MetricLookup yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.metric_name   = u.metric_name;


  -- ===========================================================================
  -- STEP 3: CVR computation from the same temp table.
  --         Pivot wide → compute 17 CVRs → unpivot to long.
  --         metric_type = 'CVR'.
  --         metric_value = NULL — Tableau computes the rate as:
  --           SUM(adobe_cvr_numerator) / SUM(adobe_cvr_denominator)
  --         This works correctly at weekly, QTD, or quarterly grain.
  --         WoW/YoY columns NULL — rates are trended visually in Tableau.
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE tmp_silver_adobe_cvr AS
  WITH

  -- ---------------------------------------------------------------------------
  -- 3A: Pivot volume temp table wide.
  --     One row per qgp_date x channel_group with one column per metric.
  --     MAX() safely collapses the 18 long rows back to one wide row.
  -- ---------------------------------------------------------------------------
  AdobeWide AS (
    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      channel_group,
      MAX(CASE WHEN metric_name = 'upvTotalAdobe'            THEN metric_value END) AS upvTotalAdobe,
      MAX(CASE WHEN metric_name = 'upvFlowTotal'             THEN metric_value END) AS upvFlowTotal,
      MAX(CASE WHEN metric_name = 'upvPostpaid'              THEN metric_value END) AS upvPostpaid,
      MAX(CASE WHEN metric_name = 'upvHsi'                   THEN metric_value END) AS upvHsi,
      MAX(CASE WHEN metric_name = 'upvByod'                  THEN metric_value END) AS upvByod,
      MAX(CASE WHEN metric_name = 'cartstartTotal'           THEN metric_value END) AS cartstartTotal,
      MAX(CASE WHEN metric_name = 'cartstartPostpaid'        THEN metric_value END) AS cartstartPostpaid,
      MAX(CASE WHEN metric_name = 'cartstartHsi'             THEN metric_value END) AS cartstartHsi,
      MAX(CASE WHEN metric_name = 'cartstartByod'            THEN metric_value END) AS cartstartByod,
      MAX(CASE WHEN metric_name = 'ordersTotal'              THEN metric_value END) AS ordersTotal,
      MAX(CASE WHEN metric_name = 'ordersUnassistedTotal'    THEN metric_value END) AS ordersUnassistedTotal,
      MAX(CASE WHEN metric_name = 'ordersAssistedTotal'      THEN metric_value END) AS ordersAssistedTotal,
      MAX(CASE WHEN metric_name = 'ordersUnassistedPostpaid' THEN metric_value END) AS ordersUnassistedPostpaid,
      MAX(CASE WHEN metric_name = 'ordersAssistedPostpaid'   THEN metric_value END) AS ordersAssistedPostpaid,
      MAX(CASE WHEN metric_name = 'ordersUnassistedHsi'      THEN metric_value END) AS ordersUnassistedHsi,
      MAX(CASE WHEN metric_name = 'ordersAssistedHsi'        THEN metric_value END) AS ordersAssistedHsi,
      MAX(CASE WHEN metric_name = 'ordersUnassistedByod'     THEN metric_value END) AS ordersUnassistedByod,
      MAX(CASE WHEN metric_name = 'ordersAssistedByod'       THEN metric_value END) AS ordersAssistedByod,
      MAX(max_date)                                                                  AS max_date
    FROM tmp_silver_adobe_volume
    GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group
  ),

  -- ---------------------------------------------------------------------------
  -- 3B: Compute 17 CVRs from the wide table.
  --     Each CVR row stores numerator and denominator separately.
  --     metric_value = NULL — Tableau divides SUM(num) / SUM(denom).
  --     NULLIF on denominator prevents divide-by-zero.
  -- ---------------------------------------------------------------------------
  CvrComputed AS (
    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      channel_group,
      max_date,
      -- Flow entry rate: upvFlowTotal / upvTotalAdobe
      'cvrUpvFlow'                  AS metric_name,
      upvFlowTotal                  AS adobe_cvr_numerator,
      upvTotalAdobe                 AS adobe_cvr_denominator
    FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Flow mix: upvPostpaid / upvFlowTotal
      'cvrUpvPostpaid', upvPostpaid, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Flow mix: upvHsi / upvFlowTotal
      'cvrUpvHsi', upvHsi, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Flow mix: upvByod / upvFlowTotal
      'cvrUpvByod', upvByod, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Cartstart rate: cartstartTotal / upvFlowTotal
      'cvrCartstartTotal', cartstartTotal, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Cartstart by product: cartstartPostpaid / upvPostpaid
      'cvrCartstartPostpaid', cartstartPostpaid, upvPostpaid FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Cartstart by product: cartstartHsi / upvHsi
      'cvrCartstartHsi', cartstartHsi, upvHsi FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Cartstart by product: cartstartByod / upvByod
      'cvrCartstartByod', cartstartByod, upvByod FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate: ordersTotal / upvFlowTotal
      'cvrOrdersTotal', ordersTotal, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate: ordersUnassistedTotal / upvFlowTotal
      'cvrOrdersUnassistedTotal', ordersUnassistedTotal, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate: ordersAssistedTotal / upvFlowTotal
      'cvrOrdersAssistedTotal', ordersAssistedTotal, upvFlowTotal FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersUnassistedPostpaid / upvPostpaid
      'cvrOrdersUnassistedPostpaid', ordersUnassistedPostpaid, upvPostpaid FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersAssistedPostpaid / upvPostpaid
      'cvrOrdersAssistedPostpaid', ordersAssistedPostpaid, upvPostpaid FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersUnassistedHsi / upvHsi
      'cvrOrdersUnassistedHsi', ordersUnassistedHsi, upvHsi FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersAssistedHsi / upvHsi
      'cvrOrdersAssistedHsi', ordersAssistedHsi, upvHsi FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersUnassistedByod / upvByod
      'cvrOrdersUnassistedByod', ordersUnassistedByod, upvByod FROM AdobeWide

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group, max_date,
      -- Order rate by product: ordersAssistedByod / upvByod
      'cvrOrdersAssistedByod', ordersAssistedByod, upvByod FROM AdobeWide
  )

  -- ---------------------------------------------------------------------------
  -- 3C: Shape CVR rows to match Silver table schema.
  --     WoW/YoY columns all NULL — rates are not compared period-over-period.
  --     metric_value NULL — Tableau computes SUM(num)/SUM(denom) directly.
  -- ---------------------------------------------------------------------------
  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    channel_group,
    metric_name,
    'CVR'                         AS metric_type,
    CAST(NULL AS FLOAT64)         AS metric_value,
    CAST(NULL AS FLOAT64)         AS metric_value_ly,
    CAST(NULL AS FLOAT64)         AS wow_numerator,
    CAST(NULL AS FLOAT64)         AS wow_denominator,
    CAST(NULL AS FLOAT64)         AS wow_pct,
    CAST(NULL AS FLOAT64)         AS yoy_numerator,
    CAST(NULL AS FLOAT64)         AS yoy_denominator,
    CAST(NULL AS FLOAT64)         AS yoy_pct,
    CAST(max_date AS DATE)        AS max_date,
    adobe_cvr_numerator,
    adobe_cvr_denominator
  FROM CvrComputed;


  -- ===========================================================================
  -- STEP 4: Write final Silver table.
  --         UNION volume rows (STEP 2) + CVR rows (STEP 3).
  --         Gold is a pure pass-through view — no further computation.
  -- ===========================================================================
  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Adobe UPV funnel metrics + CVRs in long format. One row per qgp_date x channel_group x metric_name. metric_type = ADOBE_VOLUME for volume rows (with WoW/YoY); metric_type = CVR for conversion rate rows (with adobe_cvr_numerator/denominator, no WoW/YoY). Partitioned by qgp_date, clustered by channel_group, metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_adobeFunnel_weekly.'
  )
  AS
  -- Volume rows: all WoW/YoY computed, adobe_cvr_* NULL
  SELECT * FROM tmp_silver_adobe_volume
  UNION ALL
  -- CVR rows: adobe_cvr_numerator/denominator populated, all WoW/YoY NULL
  SELECT * FROM tmp_silver_adobe_cvr;

END;