/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly.sql
LAYER:        Silver Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly

PURPOSE:
  Creates/refreshes:

    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  Converts Bronze Biddable Spend into the common PulseTMS long metric format.

DATA SOURCE:
  BIDDABLE_SPEND_CHANNEL

METRIC:
  biddableSpend

CHANNELS:
  Programmatic
  Paid Social
  Paid Search
  All Channels

LOB CONFORMANCE:
  Bronze intentionally preserves source-native high-level LOB naming.

  Silver canonicalizes equivalent values BEFORE aggregation:

    POSTPAID
    CONSUMER POSTPAID
      -> POSTPAID

    HSI
    BROADBAND
      -> BROADBAND

    TFB
    TBG
      -> TFB

  This is especially important after replacing Paid Social with
  media_analytics_integrated_snapshot because Integrated uses Brand = BROADBAND
  while some other Biddable sources use HSI.

  Performing the mapping before All Channels aggregation prevents separate HSI and
  BROADBAND rollups from being created for the same business LOB.

PLATFORM:
  Bronze retains platform detail for future use.

  This Silver intentionally collapses platform and operates at:

    qgp_date x lob x channel_group

BOUNDARY PRORATION:
  Retains the existing PulseTMS QGP boundary logic.

WOW / YOY:
  Retains the same existing PulseTMS calculation behavior.

PROGRAMMATIC YOY CAVEAT:
  pbi_programmatic_browsers_currentyr is current-year-only.

  Therefore:
    Programmatic YoY remains NULL where no prior-year source data exists.

  All Channels YoY can also have an incomplete LY baseline because current-year
  All Channels contains Programmatic while LY may only contain Paid Search /
  Paid Social.

SOURCE READINESS:
  is_complete_period is a calendar/QGP completeness indicator.

  It MUST NOT be interpreted as confirming that every source has finished backfill.

  Paid Social source readiness remains separately monitored.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  USING DELTA

  CLUSTER BY (
    qgp_date,
    lob,
    channel_group
  )

  COMMENT '
    PulseTMS Silver — Biddable Spend.

    Combines Programmatic, Paid Social, and Paid Search at canonical LOB x
    channel_group grain.

    data_source = BIDDABLE_SPEND_CHANNEL
    metric_name = biddableSpend

    Silver canonicalizes equivalent source LOB values before channel aggregation,
    including HSI/BROADBAND -> BROADBAND.

    Platform detail is retained in Bronze but collapsed in this Channel-grain
    Silver.

    Includes All Channels rollup by canonical LOB.

    Refreshed by:
      sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly
  '

  AS

  WITH


  /* ===============================================================================================
     CANONICALIZE LOB + COLLAPSE PLATFORM
     =============================================================================================== */

  BronzeAgg AS (

    SELECT
      week_sun_sat,

      CASE
        WHEN UPPER(TRIM(lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(lob)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'

        WHEN UPPER(TRIM(lob)) IN (
          'TFB',
          'TBG'
        )
          THEN 'TFB'

        ELSE UPPER(TRIM(lob))

      END                                                      AS lob,

      channel_group,

      SUM(spend)                                               AS spend

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly

    GROUP BY
      week_sun_sat,

      CASE
        WHEN UPPER(TRIM(lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(lob)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'

        WHEN UPPER(TRIM(lob)) IN (
          'TFB',
          'TBG'
        )
          THEN 'TFB'

        ELSE UPPER(TRIM(lob))
      END,

      channel_group
  ),


  /* ===============================================================================================
     ATTACH QGP CALENDAR + QUARTER BOUNDARY PRORATION
     =============================================================================================== */

  BronzeWithCalendar AS (

    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                               AS qgp_quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,

      channels.lob,
      channels.channel_group,

      CASE

        WHEN cal.week_type = 'BOUNDARY_STUB'
         AND cal.is_complete_period
          THEN bf.spend * cal.days_in_period / 7

        WHEN cal.week_type = 'BOUNDARY_FIRST'
         AND cal.is_complete_period
          THEN b.spend * cal.days_in_period / 7

        WHEN cal.is_complete_period
          THEN b.spend

      END                                                       AS spend

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal

    CROSS JOIN (
      SELECT DISTINCT
        lob,
        channel_group
      FROM BronzeAgg
      WHERE lob IS NOT NULL
    ) channels


    LEFT JOIN BronzeAgg b
      ON  b.week_sun_sat = cal.qgp_date
      AND b.lob           = channels.lob
      AND b.channel_group = channels.channel_group


    /*
      Boundary stub points to the same underlying natural Saturday week as
      BOUNDARY_FIRST.
    */
    LEFT JOIN BronzeAgg bf
      ON  cal.week_type = 'BOUNDARY_STUB'

      AND bf.week_sun_sat =
          date_add(
            cal.qgp_date,
            7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date)
          )

      AND bf.lob           = channels.lob
      AND bf.channel_group = channels.channel_group


    WHERE

      cal.qgp_date < trunc(current_date(), 'QUARTER')

      OR (

        cal.qgp_date >= trunc(current_date(), 'QUARTER')

        AND cal.qgp_date <=
            date_sub(
              add_months(
                trunc(current_date(), 'QUARTER'),
                3
              ),
              1
            )
      )
  ),


  /* ===============================================================================================
     LONG METRIC BASE
     =============================================================================================== */

  UnpivotedBase AS (

    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      wow_prior_qgp_date,
      boundary_stub_date,
      iso_week_number,
      iso_year,
      lob,
      channel_group,

      'biddableSpend'                                          AS metric_name,

      spend                                                    AS metric_value

    FROM BronzeWithCalendar

    WHERE lob IS NOT NULL
  ),


  /* ===============================================================================================
     ALL CHANNELS ROLLUP
     =============================================================================================== */

  UnpivotedAllChannels AS (

    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      wow_prior_qgp_date,
      boundary_stub_date,
      iso_week_number,
      iso_year,
      lob,

      'All Channels'                                           AS channel_group,

      metric_name,

      SUM(metric_value)                                        AS metric_value

    FROM UnpivotedBase

    GROUP BY
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      wow_prior_qgp_date,
      boundary_stub_date,
      iso_week_number,
      iso_year,
      lob,
      metric_name
  ),


  Unpivoted AS (

    SELECT *
    FROM UnpivotedBase

    UNION ALL

    SELECT *
    FROM UnpivotedAllChannels
  ),


  /* ===============================================================================================
     LOOKUPS
     =============================================================================================== */

  MetricLookup AS (

    SELECT
      qgp_date,
      lob,
      channel_group,
      metric_name,
      metric_value

    FROM Unpivoted
  ),


  LYWeeklyLookup AS (

    SELECT
      iso_year,
      iso_week_number,
      lob,
      channel_group,
      metric_name,

      SUM(metric_value)                                       AS ly_weekly_metric_value

    FROM Unpivoted

    WHERE metric_value IS NOT NULL

    GROUP BY
      iso_year,
      iso_week_number,
      lob,
      channel_group,
      metric_name
  ),


  /* ===============================================================================================
     WOW / YOY
     =============================================================================================== */

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


      /* -------------------------------------------------------------------------------------------
         LY value prorated to current QGP-period duration.
         ------------------------------------------------------------------------------------------- */

      ROUND(
        ly_week.ly_weekly_metric_value
          * try_divide(u.days_in_period, 7),
        2
      )                                                        AS metric_value_ly,


      /* -------------------------------------------------------------------------------------------
         WOW NUMERATOR
         ------------------------------------------------------------------------------------------- */

      CASE u.week_type

        WHEN 'BOUNDARY_STUB'
          THEN NULL

        WHEN 'BOUNDARY_FIRST'
          THEN
            COALESCE(u.metric_value, 0)
            +
            COALESCE(stub_lookup.metric_value, 0)

        ELSE
          u.metric_value

      END                                                      AS wow_numerator,


      /* -------------------------------------------------------------------------------------------
         WOW DENOMINATOR
         ------------------------------------------------------------------------------------------- */

      CASE

        WHEN u.metric_value IS NULL
          THEN NULL

        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL

        WHEN wow_prior_stub.metric_value IS NOT NULL
          THEN
            COALESCE(wow_prior_lookup.metric_value, 0)
            +
            COALESCE(wow_prior_stub.metric_value, 0)

        ELSE
          COALESCE(wow_prior_lookup.metric_value, 0)

      END                                                      AS wow_denominator,


      /* -------------------------------------------------------------------------------------------
         YOY NUMERATOR
         ------------------------------------------------------------------------------------------- */

      CASE u.week_type

        WHEN 'BOUNDARY_STUB'
          THEN NULL

        WHEN 'BOUNDARY_FIRST'
          THEN
            COALESCE(u.metric_value, 0)
            +
            COALESCE(stub_lookup.metric_value, 0)

        ELSE
          u.metric_value

      END                                                      AS yoy_numerator,


      /* -------------------------------------------------------------------------------------------
         YOY DENOMINATOR
         ------------------------------------------------------------------------------------------- */

      CASE

        WHEN u.metric_value IS NULL
          THEN NULL

        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL

        ELSE
          ly_week.ly_weekly_metric_value

      END                                                      AS yoy_denominator


    FROM Unpivoted u


    LEFT JOIN MetricLookup wow_prior_lookup

      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.lob           = u.lob
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name


    LEFT JOIN
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal

      ON prior_cal.qgp_date = u.wow_prior_qgp_date


    LEFT JOIN MetricLookup wow_prior_stub

      ON  wow_prior_stub.qgp_date      = prior_cal.boundary_stub_date
      AND wow_prior_stub.lob           = u.lob
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name   = u.metric_name


    LEFT JOIN MetricLookup stub_lookup

      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name


    LEFT JOIN LYWeeklyLookup ly_week

      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.lob             = u.lob
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name
  )


  /* ===============================================================================================
     FINAL SILVER
     =============================================================================================== */

  SELECT
    'BIDDABLE_SPEND_CHANNEL'                                  AS data_source,

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

    CASE
      WHEN wow_denominator IS NULL
        OR wow_denominator = 0
        THEN NULL

      ELSE wow_numerator / wow_denominator - 1
    END                                                       AS wow_pct,

    yoy_numerator,
    yoy_denominator,

    CASE
      WHEN yoy_denominator IS NULL
        OR yoy_denominator = 0
        THEN NULL

      ELSE yoy_numerator / yoy_denominator - 1
    END                                                       AS yoy_pct,


    MAX(
      CASE
        WHEN metric_value IS NOT NULL
          THEN qgp_date
      END
    )
    OVER (
      PARTITION BY
        lob,
        channel_group,
        metric_name
    )                                                         AS max_date


  FROM WithWowYoy
  ;

END;