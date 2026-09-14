/* =================================================================================================
FILE:         01_sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly.sql   (Databricks port)
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly.
  Called as part of the weekly refresh.

  All heavy processing happens here — Gold is a pure pass-through view.

STRUCTURE:
  STEP 1 — Bronze + Calendar join → prorate metrics → unpivot 18 volume metrics to long.
           Each long row also carries its CVR numerator and denominator inline
           (same pattern as wow_numerator / wow_denominator).
           Materialized into tmp_silver_adobe_unpivoted.

  STEP 2 — WoW / YoY computation via self-joins against the materialized table.
           CVR columns passed through unchanged.
           Writes final Silver table, then drops the intermediate table.

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

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - SAFE_DIVIDE(a, b)                -> try_divide(a, b)
  - FLOAT64                          -> DOUBLE
  - DATE_ADD(x, INTERVAL n DAY)      -> date_add(x, n)
  - DATE_TRUNC(x, QUARTER)           -> trunc(x, 'QUARTER')
  - DATE_SUB(DATE_ADD(x, INTERVAL 3 MONTH), INTERVAL 1 DAY) -> date_sub(add_months(x, 3), 1)
  - CREATE OR REPLACE TEMP TABLE     -> Databricks has no session-scoped *materialized* temp-table
                                         primitive (CREATE TEMPORARY VIEW exists but is lazily
                                         evaluated, not materialized, so it would recompute the
                                         18-way UNION ALL on every reference). To preserve the
                                         BQ original's "materialize once, self-join many times"
                                         intent, this ports to a genuine CREATE OR REPLACE TABLE
                                         (fully catalog-qualified, so no session-scope resolution
                                         issues inside the procedure body), explicitly dropped at
                                         the end of the procedure so it doesn't persist between runs.
                                         If recompute cost is acceptable, a CREATE OR REPLACE
                                         TEMPORARY VIEW referenced via its session.<name> qualifier
                                         is the lighter-weight alternative — flagging so you can
                                         choose based on actual data volume.
  - CREATE TABLE ... PARTITION BY x CLUSTER BY y ... OPTIONS(description=...)
                                     -> CREATE TABLE ... CLUSTER BY (x, y, z) COMMENT '...'
  - OPTIONS(strict_mode=false)      -> dropped; no Databricks equivalent
  - CREATE OR REPLACE PROCEDURE ... BEGIN...END -> needs explicit LANGUAGE SQL clause

CHANGE LOG:
  - Removed separate CVR rows (STEP 3 + STEP 4 UNION).
  - CVR numerator/denominator now inline on each volume row — same pattern as WoW.
  - Added adobe_cvr_value (pre-computed ratio) for direct weekly use in Tableau.
  - metric_type = 'ADOBE_VOLUME' for all rows — no CVR metric_type needed.
  - dim calendar column 'quarter' aliased as 'qgp_quarter'.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_silver_adobe_unpivoted AS
  WITH

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
      cal.prior_year_days_in_period,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.channel_group,

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

    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
    CROSS JOIN (
      SELECT DISTINCT channel_group
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_adobeFunnel_weekly
    ) channels
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_adobeFunnel_weekly b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.channel_group = channels.channel_group
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_adobeFunnel_weekly bf
      ON  cal.week_type    = 'BOUNDARY_STUB'
      AND bf.week_sun_sat  = date_add(cal.qgp_date, 7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date))
      AND bf.channel_group = channels.channel_group
    WHERE
      cal.qgp_date < trunc(current_date(), 'QUARTER')
      OR (
        cal.qgp_date >= trunc(current_date(), 'QUARTER')
        AND cal.qgp_date <= date_sub(add_months(trunc(current_date(), 'QUARTER'), 3), 1)
      )
  ),

  VolumeUnpivoted AS (

    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvTotalAdobe'   AS metric_name,
      upvTotalAdobe     AS metric_value,
      CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
      CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
      CAST(NULL AS DOUBLE) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvFlowTotal', upvFlowTotal,
      upvFlowTotal  AS adobe_cvr_numerator,
      upvTotalAdobe AS adobe_cvr_denominator,
      try_divide(upvFlowTotal, upvTotalAdobe) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvPostpaid', upvPostpaid,
      upvPostpaid   AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      try_divide(upvPostpaid, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvHsi', upvHsi,
      upvHsi        AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      try_divide(upvHsi, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'upvByod', upvByod,
      upvByod       AS adobe_cvr_numerator,
      upvFlowTotal  AS adobe_cvr_denominator,
      try_divide(upvByod, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartTotal', cartstartTotal,
      cartstartTotal AS adobe_cvr_numerator,
      upvFlowTotal   AS adobe_cvr_denominator,
      try_divide(cartstartTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartPostpaid', cartstartPostpaid,
      cartstartPostpaid AS adobe_cvr_numerator,
      upvPostpaid       AS adobe_cvr_denominator,
      try_divide(cartstartPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartHsi', cartstartHsi,
      cartstartHsi  AS adobe_cvr_numerator,
      upvHsi        AS adobe_cvr_denominator,
      try_divide(cartstartHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'cartstartByod', cartstartByod,
      cartstartByod AS adobe_cvr_numerator,
      upvByod       AS adobe_cvr_denominator,
      try_divide(cartstartByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersTotal', ordersTotal,
      ordersTotal  AS adobe_cvr_numerator,
      upvFlowTotal AS adobe_cvr_denominator,
      try_divide(ordersTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedTotal', ordersUnassistedTotal,
      ordersUnassistedTotal AS adobe_cvr_numerator,
      upvFlowTotal          AS adobe_cvr_denominator,
      try_divide(ordersUnassistedTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedTotal', ordersAssistedTotal,
      ordersAssistedTotal AS adobe_cvr_numerator,
      upvFlowTotal        AS adobe_cvr_denominator,
      try_divide(ordersAssistedTotal, upvFlowTotal) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedPostpaid', ordersUnassistedPostpaid,
      ordersUnassistedPostpaid AS adobe_cvr_numerator,
      upvPostpaid              AS adobe_cvr_denominator,
      try_divide(ordersUnassistedPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedPostpaid', ordersAssistedPostpaid,
      ordersAssistedPostpaid AS adobe_cvr_numerator,
      upvPostpaid            AS adobe_cvr_denominator,
      try_divide(ordersAssistedPostpaid, upvPostpaid) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedHsi', ordersUnassistedHsi,
      ordersUnassistedHsi AS adobe_cvr_numerator,
      upvHsi              AS adobe_cvr_denominator,
      try_divide(ordersUnassistedHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedHsi', ordersAssistedHsi,
      ordersAssistedHsi AS adobe_cvr_numerator,
      upvHsi            AS adobe_cvr_denominator,
      try_divide(ordersAssistedHsi, upvHsi) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersUnassistedByod', ordersUnassistedByod,
      ordersUnassistedByod AS adobe_cvr_numerator,
      upvByod              AS adobe_cvr_denominator,
      try_divide(ordersUnassistedByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, prior_year_days_in_period, boundary_stub_date, iso_week_number, iso_year, channel_group,
      'ordersAssistedByod', ordersAssistedByod,
      ordersAssistedByod AS adobe_cvr_numerator,
      upvByod            AS adobe_cvr_denominator,
      try_divide(ordersAssistedByod, upvByod) AS adobe_cvr_value
    FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  )

  SELECT * FROM VolumeUnpivoted;


  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
  USING DELTA
  CLUSTER BY (qgp_date, channel_group, metric_name)
  COMMENT 'PulseTMS Silver — Adobe UPV funnel metrics in long format with WoW/YoY and inline CVR. One row per qgp_date x channel_group x metric_name. metric_type = ADOBE_VOLUME for all rows. Each row carries adobe_cvr_numerator, adobe_cvr_denominator, adobe_cvr_value inline. Clustered by qgp_date, channel_group, metric_name. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly.'
  AS
  WITH

  MetricLookup AS (
    SELECT qgp_date, channel_group, metric_name, metric_value
    FROM prdrzranalytics.lab42.tmp_silver_adobe_unpivoted
  ),

  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      channel_group,
      metric_name,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM prdrzranalytics.lab42.tmp_silver_adobe_unpivoted
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, channel_group, metric_name
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
    ROUND(
      ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7),
      2
    )                                                                     AS metric_value_ly,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS wow_numerator,

    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
        THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_lookup.metric_value, 0)
      ELSE COALESCE(wow_prior_lookup.metric_value, 0)
    END                                                                   AS wow_denominator,

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

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
      ELSE                       u.metric_value
    END                                                                   AS yoy_numerator,

    CASE
      WHEN u.metric_value IS NULL        THEN NULL
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      ELSE ly_week.ly_weekly_metric_value
    END                                                                   AS yoy_denominator,

    CASE
      WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
      WHEN ly_week.ly_weekly_metric_value IS NULL
        OR ly_week.ly_weekly_metric_value = 0 THEN NULL
      WHEN u.week_type = 'BOUNDARY_FIRST' THEN
        try_divide(
          u.metric_value + COALESCE(stub_lookup.metric_value, 0),
          ly_week.ly_weekly_metric_value
        ) - 1
      ELSE try_divide(u.metric_value, ly_week.ly_weekly_metric_value) - 1
    END                                                                   AS yoy_pct,

    MAX(CASE WHEN u.metric_value IS NOT NULL THEN u.qgp_date END)
      OVER (PARTITION BY u.metric_name)                                   AS max_date,

    u.adobe_cvr_numerator,
    u.adobe_cvr_denominator,
    u.adobe_cvr_value

  FROM prdrzranalytics.lab42.tmp_silver_adobe_unpivoted u

  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal
    ON  prior_cal.qgp_date = u.wow_prior_qgp_date

  LEFT JOIN MetricLookup wow_prior_stub_lookup
    ON  wow_prior_stub_lookup.qgp_date      = prior_cal.boundary_stub_date
    AND wow_prior_stub_lookup.channel_group = u.channel_group
    AND wow_prior_stub_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookup stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  LEFT JOIN LYWeeklyLookup ly_week
    ON  ly_week.iso_year        = u.iso_year - 1
    AND ly_week.iso_week_number = u.iso_week_number
    AND ly_week.channel_group   = u.channel_group
    AND ly_week.metric_name     = u.metric_name;

  DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_silver_adobe_unpivoted;

END;