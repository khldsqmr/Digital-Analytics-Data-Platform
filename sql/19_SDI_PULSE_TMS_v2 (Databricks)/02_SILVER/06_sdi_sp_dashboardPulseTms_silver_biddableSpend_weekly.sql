/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly.sql
PLATFORM:     Databricks
LAYER:        Silver Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly

PURPOSE:
  Creates / refreshes:

    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  Converts approved atomic Bronze Biddable Spend into the common PulseTMS long metric format.

DATA SOURCE:
  BIDDABLE_SPEND_CHANNEL

METRIC:
  biddableSpend


===================================================================================================
UPSTREAM SOURCE UNIVERSE / APPROVED SELECTION
===================================================================================================

Bronze owns raw-source selection.

PROGRAMMATIC:

  Source universe includes multiple:
    DSPs
    accounts
    Channels
    Buy Types
    campaign types
    LOBs

  PulseTMS selection:
    Postpaid + HSI/Broadband

  No DSP / account / Channel / Buy_Type / campaign_type whitelist is applied.


PAID SOCIAL:

  Source universe underneath Channel_Group_Name = Paid Social includes multiple:
    agencies
    LOBs
    accounts
    Channel_Name/platform values

  PulseTMS selection:
    Channel_Group_Name = Paid Social
    Agency             = InHouse
    LOB                = Postpaid/Broadband

  Individual Channel_Name/platform values are NOT whitelisted.


PAID SEARCH:

  Source universe currently contains:
    Google / Bing
    multiple accounts
    SEARCH / SHOPPING / PERFORMANCE_MAX / DISCOVERY
    Brand / Generic / Shopping / PMax / DemandGen
    multiple LOBs

  PulseTMS selection:
    Google + Bing
    Postpaid + HSI/Broadband

  No account / campaign_type / advertising_channel_type /
  advertising_channel_sub_type / bidding-strategy / serving-status filter is applied.


===================================================================================================
SILVER RESPONSIBILITY
===================================================================================================

Silver performs:

  1. LOB canonicalization
  2. reporting-selection construction
  3. QGP calendar alignment
  4. quarter-boundary proration
  5. WoW
  6. YoY


---------------------------------------------------------------------------------------------------
LOB CONFORMANCE
---------------------------------------------------------------------------------------------------

Canonicalization occurs BEFORE reporting aggregation:

  POSTPAID
  CONSUMER POSTPAID
    -> POSTPAID

  HSI
  BROADBAND
    -> BROADBAND

Current Biddable business scope is explicitly:

  POSTPAID
  BROADBAND

Other LOBs are not part of the current Biddable reporting contract.


---------------------------------------------------------------------------------------------------
REPORTING CHANNEL_GROUP CONTRACT
---------------------------------------------------------------------------------------------------

Bronze grain:

  week_sun_sat
    x lob
    x channel_group
    x platform

Silver transforms this into Tableau-selectable channel_group labels.

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

OVERALL TOTAL:

  All Channels

IMPORTANT:
  These are alternative reporting selections.

  Do NOT sum different channel_group selections together.

For example:

  Paid Social - All

already represents the total of its Paid Social platform rows.

Similarly:

  All Channels

already represents Programmatic + Paid Social + Paid Search.

The Tableau parameter / filter should select one reporting channel_group at a time.


---------------------------------------------------------------------------------------------------
BOUNDARY PRORATION
---------------------------------------------------------------------------------------------------

Retains the existing PulseTMS QGP boundary logic.


---------------------------------------------------------------------------------------------------
WOW / YOY
---------------------------------------------------------------------------------------------------

Retains the existing PulseTMS calculation behavior.


---------------------------------------------------------------------------------------------------
PROGRAMMATIC YOY CAVEAT
---------------------------------------------------------------------------------------------------

prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr
is current-year-only.

Therefore:

  Programmatic-related YoY remains NULL where no prior-year source data exists.

  All Channels YoY may also have an incomplete prior-year comparison when the
  current-year total includes Programmatic but the prior-year source does not.


---------------------------------------------------------------------------------------------------
SOURCE READINESS
---------------------------------------------------------------------------------------------------

is_complete_period is a calendar / QGP completeness indicator.

It MUST NOT be interpreted as proof that every raw source is fully settled.

No weekday / source-settlement filter is applied in this procedure.

Operational source readiness is monitored separately.


---------------------------------------------------------------------------------------------------
OUTPUT GRAIN
---------------------------------------------------------------------------------------------------

  qgp_date
    x lob
    x reporting channel_group
    x metric_name

No separate platform field is required downstream because platform-level reporting
selections are represented through channel_group.

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

    Current LOB scope:
      POSTPAID
      BROADBAND

    Reporting channel_group selections include:
      All Channels
      Paid Search - All
      Paid Search - <platform>
      Paid Social - All
      Paid Social - <platform>
      Programmatic - All
      Programmatic - <platform>

    Platform remains atomic in Bronze and is represented as a
    reporting channel_group selection in Silver.

    Silver performs LOB canonicalization, QGP alignment,
    boundary proration, WoW and YoY.

    Refreshed by:
      sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     1. CANONICALIZE BRONZE LOB

     Bronze already applies the approved source-selection rules.

     This CTE remains defensive so the Silver contract cannot accidentally expand to an
     unsupported LOB if Bronze is changed later.
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
        Defensive enforcement of current approved Biddable LOB scope.
      */
      AND UPPER(TRIM(lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )


      /*
        Defensive enforcement of the three approved Biddable source groups.
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

     Derived directly from atomic Bronze rows.
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
     3. PLATFORM REPORTING SELECTIONS

     Examples:
       Paid Search - Google
       Paid Social - Facebook
       Programmatic - DV360

     No individual platform whitelist is applied here.

     Bronze has already enforced the approved source scope.
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
       Derived directly from BronzeCanonical.

       Do NOT calculate this from ChannelAll + PlatformSelections because those represent
       alternative reporting views of the same spend and would double count.
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
     5. COMPLETE REPORTING-SELECTION SET

     Each channel_group value represents an independently selectable reporting view.

     Never SUM across the resulting channel_group values without intentionally choosing the
     desired level.
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
     6. ATTACH QGP CALENDAR + QUARTER BOUNDARY PRORATION
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

        /*
          Boundary stub receives its proportional share from the
          underlying natural Sunday-Saturday week.
        */
        WHEN cal.week_type = 'BOUNDARY_STUB'
         AND cal.is_complete_period
          THEN bf.spend * cal.days_in_period / 7


        /*
          Boundary-first period receives its own proportional share.
        */
        WHEN cal.week_type = 'BOUNDARY_FIRST'
         AND cal.is_complete_period
          THEN b.spend * cal.days_in_period / 7


        /*
          Normal complete QGP week.
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
      AND b.lob           = channels.lob
      AND b.channel_group = channels.channel_group


    /*
      Boundary stub references the natural Sunday-Saturday week containing
      the stub date.
    */
    LEFT JOIN ReportingBase bf

      ON  cal.week_type = 'BOUNDARY_STUB'

      AND bf.week_sun_sat =
          date_add(
            cal.qgp_date,
            7 - dayofweek(cal.qgp_date)
          )

      AND bf.lob           = channels.lob
      AND bf.channel_group = channels.channel_group


    WHERE

      /*
        Historical quarters.
      */
      cal.qgp_date < trunc(current_date(), 'QUARTER')


      OR


      /*
        Current quarter.
      */
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


    FROM BronzeWithCalendar


    WHERE
      lob IS NOT NULL
      AND channel_group IS NOT NULL

  ),



  /* ===============================================================================================
     8. CURRENT-PERIOD LOOKUP
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
     9. PRIOR-YEAR NATURAL-WEEK LOOKUP

     Used for YoY matching by ISO year / week.
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
         LY VALUE PRORATED TO CURRENT QGP-PERIOD DURATION
         ------------------------------------------------------------------------------------------- */

      ROUND(
        ly_week.ly_weekly_metric_value
          * try_divide(u.days_in_period, 7),
        2
      )                                                         AS metric_value_ly,


      /* -------------------------------------------------------------------------------------------
         WOW NUMERATOR

         Boundary-first is recombined with its corresponding stub so that
         the natural reporting week is compared to the prior natural week.
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

      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.lob           = u.lob
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       PRIOR PERIOD CALENDAR METADATA
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal

      ON prior_cal.qgp_date = u.wow_prior_qgp_date



    /* ---------------------------------------------------------------------------------------------
       PRIOR PERIOD STUB
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN MetricLookup wow_prior_stub

      ON  wow_prior_stub.qgp_date      = prior_cal.boundary_stub_date
      AND wow_prior_stub.lob           = u.lob
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name   = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       CURRENT PERIOD STUB
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN MetricLookup stub_lookup

      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name



    /* ---------------------------------------------------------------------------------------------
       PRIOR-YEAR NATURAL ISO WEEK
       --------------------------------------------------------------------------------------------- */

    LEFT JOIN LYWeeklyLookup ly_week

      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.lob             = u.lob
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name

  )



  /* ===============================================================================================
     FINAL SILVER

     GRAIN:
       qgp_date
         x lob
         x reporting channel_group
         x metric_name

     Current LOB contract:
       POSTPAID
       BROADBAND
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