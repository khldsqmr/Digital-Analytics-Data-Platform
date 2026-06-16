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
  STEP 1 — Bronze + Calendar join → prorate metrics → unpivot 18 volume metrics to long.
           Each long row also carries its CVR numerator and denominator inline
           (same pattern as wow_numerator / wow_denominator).
           Materialized into tmp_silver_adobe_unpivoted (session-scoped TEMP TABLE).

  STEP 2 — WoW / YoY computation via self-joins against the temp table.
           CVR columns passed through unchanged.
           Writes final Silver table.

BOUNDARY WEEK HANDLING:
  BOUNDARY_STUB  : metric prorated from next Saturday Bronze row × stub_days / 7
  BOUNDARY_FIRST : metric prorated from its own Saturday Bronze row × first_days / 7
  NORMAL         : full Bronze value, NULL if period not yet complete
  CVR numerator/denominator follow the same proration — ratio is still meaningful.
  WoW/YoY NULL for BOUNDARY_STUB rows.

WoW LOGIC:
  NORMAL         : numerator = current value
                   denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub

CVR COLUMNS (inline on each volume row — same philosophy as wow_numerator/denominator):
  adobe_cvr_numerator   — the volume metric for this row (numerator of the CVR)
  adobe_cvr_denominator — the denominator metric for this row's CVR
  Both NULL for upvTotalAdobe (top-level metric, no CVR definition)

  In Tableau:
    Weekly CVR  = AVG([adobe_cvr_value])  -- pre-computed ratio, one value per row
    QTD CVR     = SUM([adobe_cvr_numerator]) / SUM([adobe_cvr_denominator])
    Both work correctly at any channel_group grain since values are pre-computed
    at qgp_date x channel_group level in Silver.

CVR DEFINITIONS (numerator / denominator):
  upvFlowTotal             : upvFlowTotal      / upvTotalAdobe
  upvPostpaid              : upvPostpaid       / upvFlowTotal
  upvHsi                   : upvHsi            / upvFlowTotal
  upvByod                  : upvByod           / upvFlowTotal
  cartstartTotal           : cartstartTotal    / upvFlowTotal
  cartstartPostpaid        : cartstartPostpaid / upvPostpaid
  cartstartHsi             : cartstartHsi      / upvHsi
  cartstartByod            : cartstartByod     / upvByod
  ordersTotal              : ordersTotal       / upvFlowTotal
  ordersUnassistedTotal    : ordersUnassistedTotal  / upvFlowTotal
  ordersAssistedTotal      : ordersAssistedTotal    / upvFlowTotal
  ordersUnassistedPostpaid : ordersUnassistedPostpaid / upvPostpaid
  ordersAssistedPostpaid   : ordersAssistedPostpaid   / upvPostpaid
  ordersUnassistedHsi      : ordersUnassistedHsi      / upvHsi
  ordersAssistedHsi        : ordersAssistedHsi        / upvHsi
  ordersUnassistedByod     : ordersUnassistedByod     / upvByod
  ordersAssistedByod       : ordersAssistedByod       / upvByod
  upvTotalAdobe            : NULL / NULL (top-level, no CVR)

CHANGE LOG:
  - Removed separate CVR rows (STEP 3 + STEP 4 UNION).
  - CVR numerator/denominator now inline on each volume row — same pattern as WoW.
  - Added adobe_cvr_value (pre-computed ratio) for direct weekly use in Tableau.
  - metric_type = 'ADOBE_VOLUME' for all rows — no CVR metric_type needed.
  - dim calendar column 'quarter' aliased as 'qgp_quarter'.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  -- ===========================================================================
  -- STEP 1: Bronze + Calendar join → prorate → unpivot 18 volume metrics.
  --         Each row carries inline CVR numerator/denominator and pre-computed
  --         CVR value — same pattern as wow_numerator/denominator.
  --         Materialized as TEMP TABLE so STEP 2 self-joins are one-pass.
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
      -- Derived totals
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
    CROSS JOIN (
      SELECT DISTINCT channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly`
    ) channels
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.channel_group = channels.channel_group
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
  -- 1B: Unpivot 18 volume metrics from wide to long.
  --     Each row carries inline CVR numerator, denominator, and pre-computed
  --     CVR value — same pattern as wow_numerator / wow_denominator.
  --     upvTotalAdobe has no CVR (top-level metric, no denominator above it).
  -- ---------------------------------------------------------------------------
  VolumeUnpivoted AS (

    -- upvTotalAdobe — top-level metric, no CVR
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvTotalAdobe'   AS metric_name,
      upvTotalAdobe     AS metric_value,
      CAST(NULL AS FLOAT64) AS adobe_cvr_numerator,
      CAST(NULL AS FLOAT64) AS adobe_cvr_denominator,
      CAST(NULL AS FLOAT64) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- upvFlowTotal — CVR = upvFlowTotal / upvTotalAdobe
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvFlowTotal', upvFlowTotal,
      upvFlowTotal  AS adobe_cvr_numerator,
      upvTotalAdobe AS adobe_cvr_denominator,
      SAFE_DIVIDE(upvFlowTotal, upvTotalAdobe) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- upvPostpaid — CVR = upvPostpaid / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvPostpaid', upvPostpaid,
      upvPostpaid   AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      SAFE_DIVIDE(upvPostpaid, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- upvHsi — CVR = upvHsi / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvHsi', upvHsi,
      upvHsi        AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      SAFE_DIVIDE(upvHsi, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- upvByod — CVR = upvByod / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvByod', upvByod,
      upvByod       AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      SAFE_DIVIDE(upvByod, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- cartstartTotal — CVR = cartstartTotal / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartTotal', cartstartTotal,
      cartstartTotal AS adobe_cvr_numerator,
      upvFlowTotal   AS adobe_cvr_denominator,
      SAFE_DIVIDE(cartstartTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- cartstartPostpaid — CVR = cartstartPostpaid / upvPostpaid
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartPostpaid', cartstartPostpaid,
      cartstartPostpaid AS adobe_cvr_numerator,
      upvPostpaid       AS adobe_cvr_denominator,
      SAFE_DIVIDE(cartstartPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- cartstartHsi — CVR = cartstartHsi / upvHsi
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartHsi', cartstartHsi,
      cartstartHsi  AS adobe_cvr_numerator,
      upvHsi        AS adobe_cvr_denominator,
      SAFE_DIVIDE(cartstartHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- cartstartByod — CVR = cartstartByod / upvByod
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartByod', cartstartByod,
      cartstartByod AS adobe_cvr_numerator,
      upvByod       AS adobe_cvr_denominator,
      SAFE_DIVIDE(cartstartByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersTotal — CVR = ordersTotal / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersTotal', ordersTotal,
      ordersTotal  AS adobe_cvr_numerator,
      upvFlowTotal AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersUnassistedTotal — CVR = ordersUnassistedTotal / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedTotal', ordersUnassistedTotal,
      ordersUnassistedTotal AS adobe_cvr_numerator,
      upvFlowTotal          AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersUnassistedTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersAssistedTotal — CVR = ordersAssistedTotal / upvFlowTotal
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedTotal', ordersAssistedTotal,
      ordersAssistedTotal AS adobe_cvr_numerator,
      upvFlowTotal        AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersAssistedTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersUnassistedPostpaid — CVR = ordersUnassistedPostpaid / upvPostpaid
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedPostpaid', ordersUnassistedPostpaid,
      ordersUnassistedPostpaid AS adobe_cvr_numerator,
      upvPostpaid              AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersUnassistedPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersAssistedPostpaid — CVR = ordersAssistedPostpaid / upvPostpaid
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedPostpaid', ordersAssistedPostpaid,
      ordersAssistedPostpaid AS adobe_cvr_numerator,
      upvPostpaid            AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersAssistedPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersUnassistedHsi — CVR = ordersUnassistedHsi / upvHsi
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedHsi', ordersUnassistedHsi,
      ordersUnassistedHsi AS adobe_cvr_numerator,
      upvHsi              AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersUnassistedHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersAssistedHsi — CVR = ordersAssistedHsi / upvHsi
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedHsi', ordersAssistedHsi,
      ordersAssistedHsi AS adobe_cvr_numerator,
      upvHsi            AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersAssistedHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersUnassistedByod — CVR = ordersUnassistedByod / upvByod
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedByod', ordersUnassistedByod,
      ordersUnassistedByod AS adobe_cvr_numerator,
      upvByod              AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersUnassistedByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    -- ordersAssistedByod — CVR = ordersAssistedByod / upvByod
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedByod', ordersAssistedByod,
      ordersAssistedByod AS adobe_cvr_numerator,
      upvByod            AS adobe_cvr_denominator,
      SAFE_DIVIDE(ordersAssistedByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  )

  SELECT * FROM VolumeUnpivoted;


  -- ===========================================================================
  -- STEP 2: WoW / YoY computation via self-joins against the temp table.
  --         CVR columns (adobe_cvr_numerator, adobe_cvr_denominator,
  --         adobe_cvr_value) passed through unchanged — computed in STEP 1.
  --         Writes final Silver table.
  -- ===========================================================================
  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Adobe UPV funnel metrics in long format with WoW/YoY and inline CVR. One row per qgp_date x channel_group x metric_name. metric_type = ADOBE_VOLUME for all rows. Each row carries adobe_cvr_numerator, adobe_cvr_denominator, adobe_cvr_value inline — same pattern as wow_numerator/denominator. Partitioned by qgp_date, clustered by channel_group, metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_adobeFunnel_weekly.'
  )
  AS
  WITH

  -- ---------------------------------------------------------------------------
  -- 2A: Metric lookup — used for all WoW/YoY self-joins.
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

    -- WoW numerator
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS wow_numerator,

    -- WoW denominator
    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
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

    -- YoY numerator
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS yoy_numerator,

    -- YoY denominator
    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      WHEN u.week_type = 'BOUNDARY_FIRST'
        THEN COALESCE(yoy_bf_lookup.metric_value, 0) + COALESCE(yoy_stub_lookup.metric_value, 0)
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
      OVER (PARTITION BY u.metric_name)                                   AS max_date,

    -- CVR columns — passed through from STEP 1, no recomputation needed
    u.adobe_cvr_numerator,
    u.adobe_cvr_denominator,
    u.adobe_cvr_value

  FROM tmp_silver_adobe_unpivoted u

  -- WoW prior week value
  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  -- Prior week calendar — to check if prior week was BOUNDARY_FIRST
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
    ON  prior_cal.qgp_date = u.wow_prior_qgp_date

  -- If prior week was BOUNDARY_FIRST, look up its stub for correct WoW denominator
  LEFT JOIN MetricLookup wow_prior_stub_lookup
    ON  wow_prior_stub_lookup.qgp_date      = prior_cal.boundary_stub_date
    AND wow_prior_stub_lookup.channel_group = u.channel_group
    AND wow_prior_stub_lookup.metric_name   = u.metric_name

  -- Prior year same ISO week value
  LEFT JOIN MetricLookup ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name

  -- Stub value for BOUNDARY_FIRST rows
  LEFT JOIN MetricLookup stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  -- Prior year BOUNDARY_FIRST value
  LEFT JOIN MetricLookup yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.metric_name   = u.metric_name

  -- Prior year calendar — to find prior year stub date
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date

  -- Prior year stub value
  LEFT JOIN MetricLookup yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.metric_name   = u.metric_name;

END;