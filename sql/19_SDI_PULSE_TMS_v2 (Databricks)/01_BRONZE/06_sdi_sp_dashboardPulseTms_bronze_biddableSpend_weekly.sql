/* =================================================================================================
FILE:         06_sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql
LAYER:        Bronze Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly

PURPOSE:
  Creates/refreshes:

    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly

  Combines the primary sources for the three Biddable media channel groups:

    Programmatic
      -> prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

    Paid Social
      -> prdrzranalytics.lab42.media_analytics_integrated_snapshot

    Paid Search
      -> prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily


ARCHITECTURE:
  Bronze contains ATOMIC Biddable spend only:

    week_sun_sat
      x lob
      x channel_group
      x platform

  Examples:

    Paid Search
      Google
      Bing

    Paid Social
      Facebook
      Instagram
      TikTok
      Snapchat
      Pinterest
      LinkedIn
      Twitter
      Reddit
      Indirect
      Nextdoor
      General MFC
      Creator
      Digital Sponsorship
      etc.

    Programmatic
      Amazon DSP
      The Trade Desk
      DV360
      Blis
      Google Ads
      etc.


IMPORTANT:
  Bronze DOES NOT create duplicate aggregate rows such as:

    All Biddable
    Paid Search Total
    Paid Social Total
    Programmatic Total

  Those are reporting selections and will be derived downstream.

  This prevents spend from being double-counted when platform-level rows and
  channel-level totals coexist.


PAID SOCIAL:
  Paid Social is sourced from:

    prdrzranalytics.lab42.media_analytics_integrated_snapshot

  ALL rows with:

    Channel_Group_Name = 'Paid Social'

  are retained.

  There is intentionally NO whitelist on Channel_Name.

  Known examples include:

    Paid Social - Facebook
    Paid Social - Instagram
    Paid Social - TikTok
    Paid Social - Snapchat
    Paid Social - Pinterest
    Paid Social - LinkedIn
    Paid Social - Twitter
    Paid Social - Reddit
    Paid Social - Indirect
    Paid Social - Nextdoor
    Paid Social - General MFC
    Paid Social - Creator
    Paid Social - Digital Sponsorship

  The "Paid Social - " prefix is removed so platform contains clean reporting
  labels such as:

    Facebook
    Instagram
    TikTok
    Indirect
    General MFC

  Any future Paid Social Channel_Name will automatically flow through Bronze
  instead of being silently excluded.


PAID SOCIAL LOB:
  IMPORTANT:

    Integrated Snapshot LOB is used.

    raw.LOB

  NOT:

    raw.Brand

  Brand contains business labels such as:

    T-Mobile
    T-Mobile Home Internet
    T-Mobile Fiber
    Beyond the Smartphone

  LOB contains the reporting classification required by PulseTMS:

    POSTPAID
    BROADBAND
    PREPAID
    TFB
    etc.


LOB:
  Bronze remains an unfiltered LOB landing layer.

  Programmatic:
    raw.lob

  Paid Search:
    raw.lob

  Paid Social:
    raw.LOB

  Bronze only performs UPPER/TRIM normalization.

  Cross-source semantic canonicalization remains in Silver, for example:

    HSI -> BROADBAND
    CONSUMER POSTPAID -> POSTPAID


PROGRAMMATIC PLATFORM NORMALIZATION:
  Known DSP values are standardized for Tableau/reporting consistency:

    Amazon / Amazon DSP
      -> Amazon DSP

    The Trade Desk / TTD
      -> The Trade Desk

    DV360 / DBM / Display & Video 360
      -> DV360

    Blis
      -> Blis

    Google / Google Ads
      -> Google Ads

  Any future/unmapped DSP is retained using its source value.


PAID SEARCH PLATFORM NORMALIZATION:
  Known search platforms are standardized:

    Google / Google Ads
      -> Google

    Bing / Microsoft / Microsoft Ads
      -> Bing

  Any future/unmapped platform is retained using its source value.


WEEK:
  All sources are normalized from daily grain to the PulseTMS
  Sunday-Saturday reporting week:

    week_sun_sat =
      date_add(date, 7 - dayofweek(date))


GRAIN:
  One row per:

    week_sun_sat
      x lob
      x channel_group
      x platform


NOTE ON TOTALS:
  Do NOT create:

    Paid Search Total
    Paid Social Total
    Programmatic Total
    All Biddable

  in Bronze.

  These will be calculated from the atomic platform rows downstream.


NOTE ON READINESS:
  A populated Saturday does not necessarily mean every advertising platform
  has fully settled.

  Current observed pattern:

    Paid Search
      -> generally Monday morning

    Programmatic
      -> approximately Monday 10 AM ET

    Paid Social
      -> platform-dependent and subject to later backfill

  Calendar completeness and source-settlement readiness remain separate concepts.
================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly()

LANGUAGE SQL

AS

BEGIN


  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly


  USING DELTA


  CLUSTER BY (
    week_sun_sat,
    lob,
    channel_group,
    platform
  )


  COMMENT '
    PulseTMS Bronze — Biddable Spend.

    Primary sources:
      Programmatic = Improvado Programmatic
      Paid Social  = Media Analytics Integrated Snapshot
      Paid Search  = SA360 Gold

    Grain:
      week_sun_sat x lob x channel_group x platform

    Bronze stores atomic platform-level spend only.

    No Paid Search Total, Paid Social Total, Programmatic Total,
    or All Biddable rows are generated here.

    Paid Social retains all Channel_Name values underneath
    Channel_Group_Name = Paid Social. The "Paid Social - " prefix is
    removed to create clean platform labels.

    Integrated Snapshot LOB is used for Paid Social, not Brand.

    LOB semantic canonicalization such as HSI -> BROADBAND occurs in Silver.

    Refreshed by:
      sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     PROGRAMMATIC

     Grain before final aggregation:
       day x source LOB x DSP

     Known DSP values are standardized while unknown/future DSPs are retained.
     =============================================================================================== */

  ProgrammaticMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - EXTRACT(
          DAYOFWEEK FROM CAST(raw.date AS DATE)
        )
      ) AS week_sun_sat,


      UPPER(
        TRIM(raw.lob)
      ) AS lob,


      'Programmatic' AS channel_group,


      CASE

        WHEN UPPER(TRIM(raw.DSP)) IN (
          'AMAZON',
          'AMAZON DSP'
        )
          THEN 'Amazon DSP'


        WHEN UPPER(TRIM(raw.DSP)) IN (
          'THE TRADE DESK',
          'TTD'
        )
          THEN 'The Trade Desk'


        WHEN UPPER(TRIM(raw.DSP)) IN (
          'DV360',
          'DBM',
          'DISPLAY & VIDEO 360'
        )
          THEN 'DV360'


        WHEN UPPER(TRIM(raw.DSP)) = 'BLIS'
          THEN 'Blis'


        WHEN UPPER(TRIM(raw.DSP)) IN (
          'GOOGLE',
          'GOOGLE ADS'
        )
          THEN 'Google Ads'


        /*
          Preserve future/unmapped DSPs rather than silently dropping them.
        */
        WHEN NULLIF(TRIM(raw.DSP), '') IS NOT NULL
          THEN TRIM(raw.DSP)


        ELSE 'Unknown'

      END AS platform,


      TRY_CAST(
        raw.spend AS DOUBLE
      ) AS spend


    FROM
      prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw


    WHERE
      raw.date IS NOT NULL

  ),



  /* ===============================================================================================
     PAID SOCIAL

     SOURCE:
       prdrzranalytics.lab42.media_analytics_integrated_snapshot

     IMPORTANT:
       - Use raw.LOB, NOT raw.Brand.
       - Include ALL Paid Social Channel_Name values.
       - Do NOT whitelist individual social platforms.
       - Strip "Paid Social - " from Channel_Name for clean Tableau labels.

     Examples:

       Paid Social - Facebook
         -> Facebook

       Paid Social - Instagram
         -> Instagram

       Paid Social - Indirect
         -> Indirect

       Paid Social - General MFC
         -> General MFC

       Any future Paid Social category automatically flows through.
     =============================================================================================== */

  PaidSocialMapped AS (

    SELECT

      date_add(
        CAST(raw.Date AS DATE),
        7 - EXTRACT(
          DAYOFWEEK FROM CAST(raw.Date AS DATE)
        )
      ) AS week_sun_sat,


      /*
        Integrated Snapshot LOB is the correct reporting classification.

        Examples:
          POSTPAID
          BROADBAND
          PREPAID
          TFB
      */
      UPPER(
        TRIM(raw.LOB)
      ) AS lob,


      'Paid Social' AS channel_group,


      CASE

        WHEN NULLIF(TRIM(raw.Channel_Name), '') IS NULL
          THEN 'Unknown'


        /*
          Strip the common source prefix.

          Examples:
            Paid Social - Facebook
              -> Facebook

            Paid Social - Nextdoor
              -> Nextdoor
        */
        WHEN TRIM(raw.Channel_Name) LIKE 'Paid Social - %'
          THEN TRIM(
            REGEXP_REPLACE(
              TRIM(raw.Channel_Name),
              '^Paid Social - ',
              ''
            )
          )


        /*
          Defensive fallback if Integrated introduces a Paid Social
          Channel_Name without the standard prefix.
        */
        ELSE TRIM(raw.Channel_Name)

      END AS platform,


      TRY_CAST(
        raw.Spend AS DOUBLE
      ) AS spend


    FROM
      prdrzranalytics.lab42.media_analytics_integrated_snapshot raw


    WHERE
      raw.Date IS NOT NULL

      AND TRIM(raw.Channel_Group_Name) = 'Paid Social'

  ),



  /* ===============================================================================================
     PAID SEARCH

     Known search-engine/platform names are standardized while future/unmapped
     values are retained.

     Expected current reporting labels:

       Google
       Bing
     =============================================================================================== */

  PaidSearchMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - EXTRACT(
          DAYOFWEEK FROM CAST(raw.date AS DATE)
        )
      ) AS week_sun_sat,


      UPPER(
        TRIM(raw.lob)
      ) AS lob,


      'Paid Search' AS channel_group,


      CASE

        WHEN UPPER(TRIM(raw.ad_platform)) IN (
          'GOOGLE',
          'GOOGLE ADS',
          'GOOGLE_ADS'
        )
          THEN 'Google'


        WHEN UPPER(TRIM(raw.ad_platform)) IN (
          'BING',
          'MICROSOFT',
          'MICROSOFT ADS',
          'MICROSOFT_ADS'
        )
          THEN 'Bing'


        /*
          Preserve future/unmapped search platforms.
        */
        WHEN NULLIF(TRIM(raw.ad_platform), '') IS NOT NULL
          THEN TRIM(raw.ad_platform)


        ELSE 'Unknown'

      END AS platform,


      TRY_CAST(
        raw.cost AS DOUBLE
      ) AS spend


    FROM
      prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw


    WHERE
      raw.date IS NOT NULL

  ),



  /* ===============================================================================================
     UNION ALL THREE BIDDABLE SOURCES

     IMPORTANT:
       These remain atomic platform rows.

       There are NO synthetic total rows here.
     =============================================================================================== */

  AllSources AS (

    SELECT
      week_sun_sat,
      lob,
      channel_group,
      platform,
      spend

    FROM ProgrammaticMapped


    UNION ALL


    SELECT
      week_sun_sat,
      lob,
      channel_group,
      platform,
      spend

    FROM PaidSocialMapped


    UNION ALL


    SELECT
      week_sun_sat,
      lob,
      channel_group,
      platform,
      spend

    FROM PaidSearchMapped

  )



  /* ===============================================================================================
     FINAL BRONZE

     GRAIN:
       week_sun_sat
         x lob
         x channel_group
         x platform
     =============================================================================================== */

  SELECT

    week_sun_sat,

    lob,

    channel_group,

    platform,

    SUM(
      COALESCE(spend, 0)
    ) AS spend


  FROM AllSources


  WHERE
    week_sun_sat IS NOT NULL

    AND lob IS NOT NULL

    AND channel_group IS NOT NULL

    AND platform IS NOT NULL


  GROUP BY

    week_sun_sat,

    lob,

    channel_group,

    platform

  ;


END;