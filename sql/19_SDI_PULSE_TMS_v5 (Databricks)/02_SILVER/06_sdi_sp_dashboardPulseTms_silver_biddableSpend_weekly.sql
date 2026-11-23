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

  Selected:
    Postpaid
    HSI / Broadband
    PrePaid (includes Metro)
    TFB

  Programmatic spend is read from the archive Gold, which applies the Programmatic team's reporting
  rules (TMO / MBTM campaigns, INI and OTT excluded, exception campaign split by impression share).

  No DSP / account / Channel / Buy_Type / campaign_type whitelist in Bronze.


PAID SOCIAL:

  Selected:
    Channel_Group_Name = Paid Social
    Agency             = InHouse or In-House
    LOB                = Postpaid / HSI / Broadband / PrePaid (includes Metro) / TFB

  No individual social-platform whitelist.


PAID SEARCH:

  Selected:
    Google + Bing
    Postpaid + HSI/Broadband + Fiber + Metro + TFB

  Fiber is retained by Bronze as FIBER and mapped into BROADBAND by Silver:
    FIBER

  Paid Search Metro and TFB are retained by Bronze as (Silver maps METRO into PREPAID):
    METRO
    TFB

  No account / campaign / advertising-channel-type /
  bidding-strategy / serving-status whitelist is applied.


===================================================================================================
LOB CONFORMANCE
===================================================================================================

Silver canonicalizes atomic Bronze LOB values:

  POSTPAID
  CONSUMER POSTPAID
    -> POSTPAID

  HSI
  BROADBAND
  FIBER
    -> BROADBAND      (Fiber is part of Broadband)

  PREPAID
  METRO
    -> PREPAID        (Programmatic and Paid Social call it PREPAID, Paid Search calls it METRO)

  TFB
    -> TFB


Silver then creates the Biddable reporting LOB rollup:

  ALL
    = POSTPAID + BROADBAND + PREPAID + TFB      (all four LOBs)

  Biddable has four LOBs: Postpaid, Broadband (Fiber included), Prepaid (Metro is part of Prepaid) and TFB. ALL is their sum,
  the same definition as the MFC Total.


FINAL SILVER BIDDABLE LOB VALUES:

  ALL
  POSTPAID
  BROADBAND
  PREPAID
  TFB


IMPORTANT:

  ALL is a synthetic Biddable reporting selection.

  ALL is an alternative to the component LOB selections.

  Do NOT aggregate:

    ALL
      +
    POSTPAID
      +
    BROADBAND
      +
    PREPAID
      +
    TFB

  Correct usage is either:

    LOB = ALL

  OR individual component LOBs.

  ALL is the only selection that reports everything.


===================================================================================================
REPORTING CHANNEL_GROUP CONTRACT
===================================================================================================

Bronze contains:

  source channel_group
    x platform

Silver creates reporting selections.


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

  Channel reporting selections are alternatives.

  For example:

    Paid Search - All

  already represents the sum of its qualifying platform rows such as:

    Paid Search - Google
    Paid Search - Bing

  Channel-total rows and their platform rows MUST NOT be summed together.


===================================================================================================
LOB x CHANNEL BEHAVIOR
===================================================================================================

LOB and channel_group are independent reporting selections.


EXAMPLES:

  lob = ALL        | All Channels
    -> all approved Postpaid + Broadband + Prepaid + TFB Biddable spend across Programmatic,
       Paid Social and Paid Search.

  lob = ALL        | Paid Search - Google
    -> Google Paid Search across Postpaid, Broadband, Prepaid and TFB.

  lob = BROADBAND  | Paid Search - All
    -> Broadband Paid Search, Fiber included.

  lob = PREPAID    | All Channels
    -> Prepaid (Metro included) Biddable spend across Programmatic (PREPAID), Paid Social (PREPAID)
       and Paid Search (source METRO).

  lob = PREPAID    | Paid Search - All
    -> Prepaid (Metro) Paid Search only.

  lob = TFB        | Programmatic - All
    -> TFB Programmatic only.


===================================================================================================
BOUNDARY PRORATION
===================================================================================================

Retains the existing PulseTMS QGP quarter-boundary logic.


===================================================================================================
WOW / YOY
===================================================================================================

Retains the existing PulseTMS WoW / YoY calculation behavior.


===================================================================================================
YOY COMPARABILITY CAVEATS
===================================================================================================

PROGRAMMATIC:

  The Programmatic archive starts in 2026-01 (the raw source is current-year-only), so Programmatic
  YoY is NULL until the archive holds the prior-year week.

  All Channels YoY may also have an incomplete LY baseline when current-year All Channels contains
  Programmatic and comparable prior-year Programmatic data does not exist.


FIBER (now part of BROADBAND):

  Fiber is no longer a separate LOB. Fiber spend from Paid Search is added to BROADBAND, so Broadband
  history includes it wherever it exists.


PREPAID / TFB:

  Paid Social and Paid Search keep full history in their sources, so Prepaid and TFB YoY exists for
  those channels. The Programmatic part of Prepaid and TFB follows the Programmatic caveat above.


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

Where Silver lob is one of:

  ALL
  POSTPAID
  BROADBAND
  PREPAID
  TFB

Gold subsequently maps these Biddable Silver LOB values into true_lob while
using:

  lob = Biddable

for the common Gold business-facing LOB field.


===================================================================================================
CHANGE LOG
===================================================================================================

  - 2026-11-15: PREPAID (Metro) and TFB added as Biddable reporting LOBs.

  - 2026-11-15: FIBER folded into BROADBAND and ALL redefined. Biddable now has four LOBs: POSTPAID,
    BROADBAND (source HSI, BROADBAND or FIBER), PREPAID (source PREPAID or METRO; Metro is part of Prepaid) and TFB.
    ALL = POSTPAID + BROADBAND + PREPAID + TFB, the same definition as the MFC Total.
      BronzeCanonical : FIBER -> BROADBAND. No FIBER LOB is produced any more.
      LobReportingBase: component rows POSTPAID, BROADBAND, PREPAID, TFB; the synthetic ALL row sums
                        the same four.
    Every other CTE, the proration and the WoW / YoY logic are unchanged.

================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
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

    Canonical atomic LOBs:
      POSTPAID
      BROADBAND   (source HSI, BROADBAND or FIBER)
      PREPAID   (source PREPAID or METRO)
      TFB

    Synthetic Biddable reporting LOB:
      ALL = POSTPAID + BROADBAND + PREPAID + TFB   (all four LOBs)

    Reporting channel selections:
      All Channels

      Paid Search - All
      Paid Search - <platform>

      Paid Social - All
      Paid Social - <platform>

      Programmatic - All
      Programmatic - <platform>

    LOB and channel_group are independent reporting selections.

    Silver performs:
      LOB canonicalization
      LOB = ALL aggregation
      channel reporting-selection aggregation
      QGP alignment
      quarter-boundary proration
      WoW
      YoY

    Gold subsequently maps Silver lob to Biddable true_lob.

    Refreshed by:
      sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     1. CANONICALIZE ATOMIC BRONZE LOBS WHILE RETAINING PLATFORM

     INPUT MAY CONTAIN:

       POSTPAID
       CONSUMER POSTPAID
       HSI
       BROADBAND
       FIBER
       PREPAID
       METRO
       TFB


     OUTPUT ATOMIC LOBS:

       POSTPAID
       BROADBAND
       PREPAID
       TFB


     No ALL row is created here.
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
          'BROADBAND',
          'FIBER'
        )
          THEN 'BROADBAND'


        WHEN UPPER(TRIM(lob)) IN (
          'PREPAID',
          'METRO'
        )
          THEN 'PREPAID'


        WHEN UPPER(TRIM(lob)) = 'TFB'
          THEN 'TFB'

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
        Defensive enforcement of the approved atomic Biddable LOB universe.
      */
      AND UPPER(TRIM(lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND',
        'FIBER',
        'PREPAID',
        'METRO',
        'TFB'
      )


      /*
        Defensive enforcement of approved Biddable source channels.
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
          'BROADBAND',
          'FIBER'
        )
          THEN 'BROADBAND'


        WHEN UPPER(TRIM(lob)) IN (
          'PREPAID',
          'METRO'
        )
          THEN 'PREPAID'


        WHEN UPPER(TRIM(lob)) = 'TFB'
          THEN 'TFB'

      END,


      TRIM(channel_group),


      CASE

        WHEN NULLIF(TRIM(platform), '') IS NOT NULL
          THEN TRIM(platform)

        ELSE 'Unknown'

      END

  ),



  /* ===============================================================================================
     2. CREATE BIDDABLE LOB REPORTING SELECTIONS

     Preserve canonical atomic rows:

       POSTPAID
       BROADBAND
       PREPAID
       TFB

     Also create:

       ALL = POSTPAID + BROADBAND + PREPAID + TFB


     IMPORTANT:

       ALL is calculated directly from BronzeCanonical.

       ALL is not derived from any previously aggregated reporting row.

       ALL sums the same four LOBs.
     =============================================================================================== */

  LobReportingBase AS (

    /* ---------------------------------------------------------------------------------------------
       COMPONENT LOB ROWS
       --------------------------------------------------------------------------------------------- */

    SELECT

      week_sun_sat,

      lob,

      source_channel_group,

      platform,

      spend


    FROM BronzeCanonical


    WHERE
      lob IN (
        'POSTPAID',
        'BROADBAND',
        'PREPAID',
        'TFB'
      )



    UNION ALL



    /* ---------------------------------------------------------------------------------------------
       SYNTHETIC ALL LOB

       Retain source channel and platform at this stage so channel
       selections can be independently generated downstream.

       POSTPAID + BROADBAND + PREPAID + TFB.
       --------------------------------------------------------------------------------------------- */

    SELECT

      week_sun_sat,

      'ALL'                                                     AS lob,

      source_channel_group,

      platform,

      SUM(spend)                                                AS spend


    FROM BronzeCanonical


    WHERE
      lob IN (
        'POSTPAID',
        'BROADBAND',
        'PREPAID',
        'TFB'
      )


    GROUP BY
      week_sun_sat,
      source_channel_group,
      platform

  ),



  /* ===============================================================================================
     3. CHANNEL TOTAL SELECTIONS

     Examples:

       POSTPAID  | Paid Search - All
       BROADBAND | Paid Social - All
       BROADBAND | Paid Search - All
       PREPAID   | Programmatic - All
       ALL       | Paid Search - All
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


    FROM LobReportingBase


    WHERE
      lob IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob,
      source_channel_group

  ),



  /* ===============================================================================================
     4. PLATFORM-LEVEL SELECTIONS

     Examples:

       Paid Search - Google
       Paid Search - Bing

       Paid Social - Facebook
       Paid Social - Instagram
       Paid Social - TikTok
       ...

       Programmatic - Amazon DSP
       Programmatic - DV360
       Programmatic - The Trade Desk
       ...

     Platform values remain dynamic where Bronze allows them.
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


    FROM LobReportingBase


    WHERE
      lob IS NOT NULL

      AND platform IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob,
      source_channel_group,
      platform

  ),



  /* ===============================================================================================
     5. ALL CHANNELS

     IMPORTANT:

       All Channels is calculated directly from LobReportingBase.

       Do NOT calculate All Channels from:

         ChannelAll
         +
         PlatformSelections

       because those are alternate representations of the same spend.


     EXAMPLES:

       POSTPAID | All Channels
         = Programmatic + Paid Social + Paid Search for Postpaid

       BROADBAND | All Channels
         = Programmatic + Paid Social + Paid Search for Broadband

       PREPAID | All Channels
         = Programmatic + Paid Social + Paid Search for Prepaid

       TFB | All Channels
         = Programmatic + Paid Social + Paid Search for TFB

       ALL | All Channels
         = all approved channels across:
             POSTPAID
             BROADBAND
             PREPAID
             TFB
     =============================================================================================== */

  AllChannels AS (

    SELECT

      week_sun_sat,

      lob,

      'All Channels'                                            AS channel_group,

      SUM(spend)                                                AS spend


    FROM LobReportingBase


    WHERE
      lob IS NOT NULL


    GROUP BY
      week_sun_sat,
      lob

  ),



  /* ===============================================================================================
     6. COMBINE REPORTING SELECTIONS

     Every resulting row is one selectable reporting representation.

     DO NOT sum different LOB selections together.

     DO NOT sum a channel-total selection together with its platform selections.
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
     7. ATTACH QGP CALENDAR + QUARTER-BOUNDARY PRORATION

     Existing PulseTMS boundary behavior is retained.
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
          First period in the new quarter receives its proportional
          share from the same natural reporting week.
        */
        WHEN cal.week_type = 'BOUNDARY_FIRST'
         AND cal.is_complete_period
          THEN b.spend * cal.days_in_period / 7


        /*
          Normal complete QGP period.
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
      Boundary stub points back to the underlying natural
      Sunday-Saturday reporting week.
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
     8. LONG METRIC BASE
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
     9. CURRENT PERIOD LOOKUP
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
     10. PRIOR-YEAR NATURAL WEEK LOOKUP
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
     11. WOW / YOY
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
         x Biddable reporting lob
         x reporting channel_group
         x metric_name


     EXPECTED LOB VALUES:

       ALL
       POSTPAID
       BROADBAND
       PREPAID
       TFB


     EXAMPLES:

       ALL       | All Channels
       ALL       | Paid Search - All
       ALL       | Paid Search - Google
       ALL       | Paid Social - All
       ALL       | Paid Social - Facebook
       ALL       | Programmatic - All
       ALL       | Programmatic - DV360

       POSTPAID  | All Channels
       BROADBAND | All Channels
       PREPAID   | All Channels
       TFB       | All Channels

       BROADBAND | Paid Search - All
       PREPAID   | Paid Search - Google
       TFB       | Programmatic - All
     =============================================================================================== */

  SELECT

    'BIDDABLE_SPEND_CHANNEL'                                    AS data_source,


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

    END                                                         AS wow_pct,


    yoy_numerator,

    yoy_denominator,


    CASE

      WHEN yoy_denominator IS NULL
        OR yoy_denominator = 0
        THEN NULL

      ELSE
        yoy_numerator / yoy_denominator - 1

    END                                                         AS yoy_pct,


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
    )                                                           AS max_date


  FROM WithWowYoy

  ;


END;