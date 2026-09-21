/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly.sql
PLATFORM:     Databricks
LAYER:        Silver Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly

PURPOSE:
  Creates / refreshes:

    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  Converts atomic Bronze Biddable Spend into the common PulseTMS long metric format.

DATA SOURCE:
  BIDDABLE_SPEND_CHANNEL

METRIC:
  biddableSpend


===================================================================================================
UPSTREAM SOURCE SELECTION
===================================================================================================

Bronze owns raw-source selection.


PROGRAMMATIC:

  Source universe:
    Multiple DSPs, accounts, Channels, Buy Types, campaign types and LOBs.

  Selected:
    Postpaid
    HSI / Broadband

  No DSP / account / Channel / Buy_Type / campaign_type whitelist.


PAID SOCIAL:

  Source universe:
    Multiple Paid Social Channel_Name values, agencies, LOBs and accounts.

  Selected:
    Channel_Group_Name = Paid Social
    Agency             = InHouse
    LOB                = Postpaid / HSI / Broadband

  No individual social-platform whitelist.


PAID SEARCH:

  Source universe:
    Google / Bing
    Postpaid
    HSI / Broadband
    Fiber
    Metro
    TFB
    SEARCH
    SHOPPING
    PERFORMANCE_MAX
    DISCOVERY
    associated campaign types / accounts

  Selected:
    Google + Bing
    Postpaid + HSI/Broadband + Fiber

  Bronze locally maps:
    Fiber -> BROADBAND

  Therefore Fiber contributes to Paid Search Broadband spend but is NOT
  represented as a standalone Silver LOB.


===================================================================================================
LOB CONFORMANCE
===================================================================================================

Silver canonicalizes:

  POSTPAID
  CONSUMER POSTPAID
    -> POSTPAID

  HSI
  BROADBAND
    -> BROADBAND

Expected final Biddable Silver LOB values:

  POSTPAID
  BROADBAND

There is intentionally NO standalone FIBER value.

When dedicated Fiber / TFB reporting is introduced in the future, the
upstream and downstream LOB contracts can be expanded deliberately.


===================================================================================================
REPORTING CHANNEL_GROUP CONTRACT
===================================================================================================

Bronze contains atomic:

  channel_group
    x platform

Silver creates selectable reporting representations.


CHANNEL TOTALS:

  Paid Search - All
  Paid Social - All
  Programmatic - All


PLATFORM DETAIL:

  Paid Search - Google
  Paid Search - Bing

  Paid Social - Facebook
  Paid Social - Instagram
  Paid Social - TikTok
  Paid Social - Snapchat
  Paid Social - Pinterest
  Paid Social - Twitter
  etc.

  Programmatic - Amazon DSP
  Programmatic - The Trade Desk
  Programmatic - DV360
  Programmatic - Blis
  etc.


OVERALL:

  All Channels


IMPORTANT:
  These channel_group values are ALTERNATIVE reporting selections.

  Do NOT add:

    Paid Social - All
      +
    Paid Social - Facebook
      +
    Paid Social - Instagram

  because the platform values are already included in Paid Social - All.

  Similarly, All Channels already represents the total across:

    Programmatic
    Paid Social
    Paid Search

  for the selected LOB.


===================================================================================================
CURRENT REPORTING LOB SCOPE
===================================================================================================

  POSTPAID
  BROADBAND

The Tableau LOB selector can therefore remain:

  Postpaid + Broadband
  Postpaid
  Broadband

No Fiber parameter value is required.


===================================================================================================
BOUNDARY PRORATION
===================================================================================================

Retains the existing PulseTMS QGP quarter-boundary logic.


===================================================================================================
WOW / YOY
===================================================================================================

Retains the existing PulseTMS WoW / YoY calculation behavior.


===================================================================================================
PROGRAMMATIC YOY CAVEAT
===================================================================================================

prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr
is current-year-only.

Therefore Programmatic YoY remains NULL where prior-year Programmatic data
does not exist.

All Channels YoY can consequently have an incomplete LY comparison where
the current-year total includes Programmatic and the prior year does not.


===================================================================================================
SOURCE READINESS
===================================================================================================

is_complete_period is a calendar / QGP-period completeness flag.

It MUST NOT be interpreted as confirmation that all media sources have
completed backfill / settlement.

Source readiness is monitored separately.


===================================================================================================
OUTPUT GRAIN
===================================================================================================

  qgp_date
    x lob
    x reporting channel_group
    x metric_name

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
    PulseTMS Silver - Biddable Spend.

    data_source = BIDDABLE_SPEND_CHANNEL
    metric_name = biddableSpend

    Current canonical LOB scope:
      POSTPAID
      BROADBAND

    Paid Search Fiber is already mapped to BROADBAND in Bronze.

    Reporting selections include:

      All Channels

      Paid Search - All
      Paid Search - <platform>

      Paid Social - All
      Paid Social - <platform>

      Programmatic - All
      Programmatic - <platform>

    Platform detail remains atomic in Bronze and is represented as
    channel_group reporting selections in Silver.

    Silver performs:
      LOB canonicalization
      reporting-selection aggregation
      QGP alignment
      quarter-boundary proration
      WoW
      YoY

    Refreshed by:
      sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     1. CANONICALIZE LOB WHILE RETAINING PLATFORM

     Expected incoming LOB values:

       POSTPAID
       CONSUMER POSTPAID
       HSI
       BROADBAND

     Paid Search Fiber does NOT appear as FIBER here because Bronze has
     already mapped it to BROADBAND.
     =============================================================================================== */

  BronzeCanonical AS (

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


      END                                                       AS lob,


      TRIM(channel_group)                                       AS source_channel_group,


      CASE

        WHEN NULLIF(TRIM(platform), '') IS NOT NULL
          THEN TRIM(platform)

        ELSE 'Unknown'

      END                                                       AS platform,


      SUM(
        COALESCE(spend, 0)
      )                                                         AS spend


    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly


    WHERE
      week_sun_sat IS NOT NULL


      /*
        Defensive enforcement of the current Biddable reporting LOB contract.
      */
      AND UPPER(TRIM(lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )


      /*
        Defensive enforcement of the approved Biddable source groups.
      */
      AND TRIM(channel_group) IN (
        'Programmatic',
        'Paid Social',
        'Paid Search'
      )


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

      END,


      TRIM(channel_group),


      CASE

        WHEN NULLIF(TRIM(platform), '') IS NOT NULL
          THEN TRIM(platform)

        ELSE 'Unknown'

      END

  ),



  /* ===============================================================================================
     2. CHANNEL TOTAL SELECTIONS

     Examples:

       Paid Search - All
       Paid Social - All
       Programmatic - All
     =============================================================================================== */

  ChannelAll AS (

    SELECT

      week_sun_sat,

      lob,


      CONCAT(
        source_channel_group,
        ' - All'
      )                                                         AS channel_group,


      SUM(spend)                                                AS spend


    FROM BronzeCanonical


    WHERE
      lob IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob,
      source_channel_group

  ),



  /* ===============================================================================================
     3. PLATFORM-LEVEL SELECTIONS

     Examples:

       Paid Search - Google
       Paid Search - Bing

       Paid Social - Facebook
       Paid Social - Instagram

       Programmatic - DV360
       Programmatic - Amazon DSP

     No downstream platform whitelist is applied.
     =============================================================================================== */

  PlatformSelections AS (

    SELECT

      week_sun_sat,

      lob,


      CONCAT(
        source_channel_group,
        ' - ',
        platform
      )                                                         AS channel_group,


      SUM(spend)                                                AS spend


    FROM BronzeCanonical


    WHERE
      lob IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob,
      source_channel_group,
      platform

  ),



  /* ===============================================================================================
     4. ALL CHANNELS

     IMPORTANT:

       Calculate All Channels DIRECTLY from atomic BronzeCanonical.

       Do NOT calculate this from ChannelAll + PlatformSelections because
       those are alternate representations of the same spend and would
       double count.

     Current LOB universe:

       POSTPAID
       BROADBAND

     Paid Search Fiber contributes to BROADBAND because it was mapped
     upstream in Bronze.
     =============================================================================================== */

  AllChannels AS (

    SELECT

      week_sun_sat,

      lob,

      'All Channels'                                            AS channel_group,

      SUM(spend)                                                AS spend


    FROM BronzeCanonical


    WHERE
      lob IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob

  ),



  /* ===============================================================================================
     5. COMBINE REPORTING SELECTIONS

     Each channel_group value is independently selectable.

     Do not aggregate across selections unless intentionally required.
     =============================================================================================== */

  ReportingBase AS (

    SELECT
      week_sun_sat,
      lob,
      channel_group,
      spend

    FROM ChannelAll


    UNION ALL


    SELECT
      week_sun_sat,
      lob,
      channel_group,
      spend

    FROM PlatformSelections


    UNION ALL


    SELECT
      week_sun_sat,
      lob,
      channel_group,
      spend

    FROM AllChannels

  ),



  /* ===============================================================================================
     6. ATTACH QGP CALENDAR + QUARTER-BOUNDARY PRORATION
     =============================================================================================== */

  WithCalendar AS (

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

        /*
          Quarter-boundary stub receives its proportional share
          from the underlying natural Sunday-Saturday week.
        */
        WHEN cal.week_type = 'BOUNDARY_STUB'
         AND cal.is_complete_period
          THEN bf.spend * cal.days_in_period / 7


        /*
          First period in the new quarter receives its own
          proportional share.
        */
        WHEN cal.week_type = 'BOUNDARY_FIRST'
         AND cal.is_complete_period
          THEN b.spend * cal.days_in_period / 7


        /*
          Normal complete reporting week.
        */
        WHEN cal.is_complete_period
          THEN b.spend


        ELSE NULL

      END                                                       AS spend


    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal


    CROSS JOIN (

      SELECT DISTINCT

        lob,

        channel_group

      FROM ReportingBase

      WHERE
        lob IS NOT NULL

        AND channel_group IS NOT NULL

    ) channels



    LEFT JOIN ReportingBase b

      ON  b.week_sun_sat = cal.qgp_date

      AND b.lob = channels.lob

      AND b.channel_group = channels.channel_group



    /*
      Boundary stub points to the same underlying natural
      Sunday-Saturday week as BOUNDARY_FIRST.
    */
    LEFT JOIN ReportingBase bf

      ON cal.week_type = 'BOUNDARY_STUB'


      AND bf.week_sun_sat =
          date_add(
            cal.qgp_date,
            7 - dayofweek(cal.qgp_date)
          )


      AND bf.lob = channels.lob

      AND bf.channel_group = channels.channel_group



    WHERE

      cal.qgp_date < trunc(current_date(), 'QUARTER')


      OR


      (

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
     7. LONG METRIC BASE
     =============================================================================================== */

  Unpivoted AS (

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


      'biddableSpend'                                           AS metric_name,


      spend                                                     AS metric_value


    FROM WithCalendar


    WHERE
      lob IS NOT NULL

      AND channel_group IS NOT NULL

  ),



  /* ===============================================================================================
     8. CURRENT PERIOD LOOKUP
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



  /* ===============================================================================================
     9. PRIOR-YEAR NATURAL WEEK LOOKUP
     =============================================================================================== */

  LYWeeklyLookup AS (

    SELECT

      iso_year,

      iso_week_number,

      lob,

      channel_group,

      metric_name,


      SUM(metric_value)                                         AS ly_weekly_metric_value


    FROM Unpivoted


    WHERE
      metric_value IS NOT NULL


    GROUP BY

      iso_year,

      iso_week_number,

      lob,

      channel_group,

      metric_name

  ),



  /* ===============================================================================================
     10. WOW / YOY
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
         LY VALUE PRORATED TO CURRENT QGP PERIOD LENGTH
         ------------------------------------------------------------------------------------------- */

      ROUND(
        ly_week.ly_weekly_metric_value
          * try_divide(u.days_in_period, 7),
        2
      )                                                         AS metric_value_ly,


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

      END                                                       AS wow_numerator,


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

      END                                                       AS wow_denominator,


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

      END                                                       AS yoy_numerator,


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

      END                                                       AS yoy_denominator


    FROM Unpivoted u



    /* ---------------------------------------------------------------------------------------------
       PRIOR QGP PERIOD
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN MetricLookup wow_prior_lookup

      ON  wow_prior_lookup.qgp_date = u.wow_prior_qgp_date

      AND wow_prior_lookup.lob = u.lob

      AND wow_prior_lookup.channel_group = u.channel_group

      AND wow_prior_lookup.metric_name = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       PRIOR PERIOD CALENDAR
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal

      ON prior_cal.qgp_date = u.wow_prior_qgp_date



    /* ---------------------------------------------------------------------------------------------
       PRIOR PERIOD BOUNDARY STUB
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN MetricLookup wow_prior_stub

      ON  wow_prior_stub.qgp_date = prior_cal.boundary_stub_date

      AND wow_prior_stub.lob = u.lob

      AND wow_prior_stub.channel_group = u.channel_group

      AND wow_prior_stub.metric_name = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       CURRENT PERIOD BOUNDARY STUB
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN MetricLookup stub_lookup

      ON  stub_lookup.qgp_date = u.boundary_stub_date

      AND stub_lookup.lob = u.lob

      AND stub_lookup.channel_group = u.channel_group

      AND stub_lookup.metric_name = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       PRIOR-YEAR NATURAL ISO WEEK
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN LYWeeklyLookup ly_week

      ON  ly_week.iso_year = u.iso_year - 1

      AND ly_week.iso_week_number = u.iso_week_number

      AND ly_week.lob = u.lob

      AND ly_week.channel_group = u.channel_group

      AND ly_week.metric_name = u.metric_name

  )



  /* ===============================================================================================
     FINAL SILVER

     GRAIN:

       qgp_date
         x lob
         x reporting channel_group
         x metric_name

     EXPECTED LOB:

       POSTPAID
       BROADBAND

     There is intentionally no standalone FIBER row.
     =============================================================================================== */

  SELECT

    'BIDDABLE_SPEND_CHANNEL'                                   AS data_source,


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

      ELSE
        wow_numerator / wow_denominator - 1

    END                                                        AS wow_pct,


    yoy_numerator,

    yoy_denominator,


    CASE

      WHEN yoy_denominator IS NULL
        OR yoy_denominator = 0
        THEN NULL

      ELSE
        yoy_numerator / yoy_denominator - 1

    END                                                        AS yoy_pct,


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
    )                                                          AS max_date


  FROM WithWowYoy

  ;


END;