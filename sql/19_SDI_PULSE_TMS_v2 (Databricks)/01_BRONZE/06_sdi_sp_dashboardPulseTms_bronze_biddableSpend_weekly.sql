/* =================================================================================================
FILE:         06_sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql
PLATFORM:     Databricks
LAYER:        Bronze Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly

PURPOSE:
  Creates / refreshes:

    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly

  Combines the approved source scope for the three Biddable media channel groups:

    Programmatic
      -> prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

    Paid Social
      -> prdrzranalytics.lab42.media_analytics_integrated_snapshot

    Paid Search
      -> prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily


===================================================================================================
SOURCE UNIVERSE VS PULSETMS SELECTION
===================================================================================================

IMPORTANT:
  "Source universe" documents values currently observed in each upstream source.

  "PulseTMS selection" defines the rows intentionally consumed by this pipeline.

  Observed categories are documentation only unless explicitly included / excluded below.


---------------------------------------------------------------------------------------------------
1. PROGRAMMATIC
---------------------------------------------------------------------------------------------------

SOURCE:
  prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr


OBSERVED SOURCE UNIVERSE:

  DSP:
    Amazon DSP
    Blis
    DV360
    The Trade Desk

  LOB:
    PostPaid
    HSI
    PrePaid
    TFB
    Archived

  Channel:
    Display
    OLV
    Streaming Radio
    NULL

  Buy_Type:
    PR-OEX
    PR-PMP
    PR-YT
    PR-DG
    NULL

  Accounts:
    Multiple accounts exist across the DSP universe.


PULSETMS SELECTION:

  INCLUDE LOB:
    Postpaid
    HSI / Broadband

  RETAIN ALL QUALIFYING:
    DSP values
    account_id values
    Channel values
    Buy_Type values
    campaign_type values
    campaign / ad-group / placement combinations

  DO NOT:
    whitelist DSPs
    whitelist accounts
    whitelist Channel
    whitelist Buy_Type
    whitelist campaign types

  EXCLUDE BY LOB:
    PrePaid
    TFB
    Archived

  Future / unmapped DSP values remain included if they satisfy the approved LOB scope.


---------------------------------------------------------------------------------------------------
2. PAID SOCIAL
---------------------------------------------------------------------------------------------------

SOURCE:
  prdrzranalytics.lab42.media_analytics_integrated_snapshot


OBSERVED SOURCE UNIVERSE:

  Channel_Group_Name:
    Paid Social

  Agency:
    InHouse
    Initiative
    Assurance
    TMO Accessibility

  LOB examples:
    POSTPAID
    BROADBAND
    PREPAID
    TFB
    ASSURANCE
    Other / Not Specified
    other Integrated classifications

  Channel_Name examples:
    Paid Social - Facebook
    Paid Social - Instagram
    Paid Social - TikTok
    Paid Social - Snapchat
    Paid Social - Pinterest
    Paid Social - Twitter
    Paid Social - LinkedIn
    Paid Social - Indirect
    Paid Social - Nextdoor
    Paid Social - General MFC
    and future values

  Accounts:
    Multiple Account_ID / Account_Name values exist across the source.


PULSETMS SELECTION:

  REQUIRED:
    Channel_Group_Name = Paid Social
    Agency             = InHouse
    LOB                = Postpaid / HSI / Broadband

  RETAIN ALL QUALIFYING:
    Channel_Name values
    Account_ID values
    Account_Name values
    Account_Type values
    campaign values
    other dimensions underneath the approved scope

  DO NOT:
    whitelist Facebook / Instagram / TikTok / etc.
    whitelist accounts

  Therefore any future Paid Social platform automatically flows through when it satisfies:

    Paid Social
      + InHouse
      + Postpaid / Broadband

  IMPORTANT:
    raw.LOB is used.

    raw.Brand MUST NOT be used as the Biddable LOB.


---------------------------------------------------------------------------------------------------
3. PAID SEARCH
---------------------------------------------------------------------------------------------------

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily


OBSERVED SOURCE UNIVERSE:

  ad_platform:
    Google
    Bing

  LOB:
    Postpaid
    HSI
    Metro
    TFB
    Fiber

  advertising_channel_type:
    SEARCH
    SHOPPING
    PERFORMANCE_MAX
    DISCOVERY

  campaign_type:
    Brand
    Generic
    Shopping
    PMax
    DemandGen

  serving_status:
    Multiple historical / current statuses may exist.

  Accounts:
    Postpaid
    Broadband
    BTS
    Metro
    TFB
    Fiber
    across Google / Bing account structures.


PULSETMS SELECTION:

  INCLUDE PLATFORM:
    Google
    Bing

  INCLUDE LOB:
    Postpaid
    HSI / Broadband
    Fiber

  RETAIN ALL QUALIFYING:
    accounts
    campaign_type values
    advertising_channel_type values
    advertising_channel_sub_type values
    bidding_strategy_type values
    serving_status values

  DO NOT:
    whitelist accounts
    filter to literal SEARCH only
    filter out Shopping
    filter out Performance Max
    filter out Discovery
    filter campaign types

  EXCLUDE BY LOB:
    Metro
    TFB
    other non-approved Paid Search LOBs

  IMPORTANT:
    Fiber is intentionally retained as FIBER.

    Fiber is NOT mapped into BROADBAND.

    Silver exposes Fiber as its own Biddable reporting LOB.

    Silver also creates:

      ALL = POSTPAID + BROADBAND + FIBER


===================================================================================================
BRONZE ARCHITECTURE
===================================================================================================

Bronze contains ATOMIC Biddable spend only.

GRAIN:

  week_sun_sat
    x lob
    x channel_group
    x platform


Bronze DOES NOT create synthetic reporting rows such as:

  LOB = ALL

  All Channels

  Paid Search - All
  Paid Social - All
  Programmatic - All

Those reporting selections are generated in Silver.


---------------------------------------------------------------------------------------------------
BRONZE LOB HANDLING
---------------------------------------------------------------------------------------------------

Bronze preserves the selected source LOB after basic normalization:

  UPPER(TRIM(source_lob))

Expected source-normalized values may include:

  POSTPAID
  CONSUMER POSTPAID
  HSI
  BROADBAND
  FIBER

Silver performs canonicalization:

  POSTPAID / CONSUMER POSTPAID
    -> POSTPAID

  HSI / BROADBAND
    -> BROADBAND

  FIBER
    -> FIBER

Silver then creates:

  ALL
    = POSTPAID + BROADBAND + FIBER


SOURCE-SPECIFIC LOB SCOPE:

  Programmatic:
    Postpaid
    HSI / Broadband

  Paid Social:
    Postpaid
    HSI / Broadband

  Paid Search:
    Postpaid
    HSI / Broadband
    Fiber


---------------------------------------------------------------------------------------------------
PROGRAMMATIC PLATFORM NORMALIZATION
---------------------------------------------------------------------------------------------------

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

Future / unmapped DSP values are retained.


---------------------------------------------------------------------------------------------------
PAID SOCIAL PLATFORM NORMALIZATION
---------------------------------------------------------------------------------------------------

No platform whitelist is used.

The standard:

  Paid Social -

prefix is removed from Channel_Name.

Examples:

  Paid Social - Facebook
    -> Facebook

  Paid Social - Instagram
    -> Instagram

  Paid Social - General MFC
    -> General MFC

Any future qualifying Paid Social Channel_Name automatically flows through.


---------------------------------------------------------------------------------------------------
PAID SEARCH PLATFORM NORMALIZATION
---------------------------------------------------------------------------------------------------

  Google / Google Ads / Google_Ads
    -> Google

  Bing / Microsoft / Microsoft Ads / Microsoft_Ads
    -> Bing


---------------------------------------------------------------------------------------------------
WEEK
---------------------------------------------------------------------------------------------------

All source dates are normalized to the PulseTMS Sunday-Saturday reporting week:

  week_sun_sat =
    date_add(date, 7 - dayofweek(date))


---------------------------------------------------------------------------------------------------
SOURCE READINESS
---------------------------------------------------------------------------------------------------

Source-selection logic and source-readiness logic are separate.

Do NOT filter spend based on weekday or expected settlement timing.

is_complete_period downstream is a QGP/calendar completeness indicator and
does NOT guarantee raw-source settlement.

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
    PulseTMS Bronze - Biddable Spend.

    Sources:
      Programmatic = Improvado Programmatic
      Paid Social  = Media Analytics Integrated Snapshot
      Paid Search  = SA360 Gold

    Grain:
      week_sun_sat x lob x channel_group x platform

    Source-specific LOB selection:

      Programmatic:
        Postpaid + HSI/Broadband

      Paid Social:
        Paid Social + InHouse + Postpaid/HSI/Broadband

      Paid Search:
        Google/Bing + Postpaid/HSI/Broadband/Fiber

    Fiber is retained as FIBER in Bronze.

    Bronze contains atomic platform-level spend only.

    LOB = ALL, All Channels, channel totals, and platform reporting
    selections are generated in Silver.

    Refreshed by:
      sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     PROGRAMMATIC
     =============================================================================================== */

  ProgrammaticMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


      /*
        Preserve source-normalized LOB.

        Canonicalization occurs in Silver.
      */
      UPPER(
        TRIM(raw.lob)
      )                                                         AS lob,


      'Programmatic'                                            AS channel_group,


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
          Preserve future / unmapped qualifying DSP values.
        */
        WHEN NULLIF(TRIM(raw.DSP), '') IS NOT NULL
          THEN TRIM(raw.DSP)


        ELSE 'Unknown'

      END                                                       AS platform,


      TRY_CAST(
        raw.spend AS DOUBLE
      )                                                         AS spend


    FROM
      prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw


    WHERE
      raw.date IS NOT NULL


      /*
        Approved Programmatic LOB scope.

        No DSP / account / Channel / Buy_Type / campaign-type
        filters are applied.
      */
      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )

  ),



  /* ===============================================================================================
     PAID SOCIAL
     =============================================================================================== */

  PaidSocialMapped AS (

    SELECT

      date_add(
        CAST(raw.Date AS DATE),
        7 - dayofweek(CAST(raw.Date AS DATE))
      )                                                         AS week_sun_sat,


      /*
        Integrated Snapshot LOB is the required Biddable LOB.

        raw.Brand is intentionally not used.
      */
      UPPER(
        TRIM(raw.LOB)
      )                                                         AS lob,


      'Paid Social'                                             AS channel_group,


      CASE

        WHEN NULLIF(TRIM(raw.Channel_Name), '') IS NULL
          THEN 'Unknown'


        /*
          Example:
            Paid Social - Facebook
              -> Facebook
        */
        WHEN UPPER(TRIM(raw.Channel_Name)) LIKE 'PAID SOCIAL - %'
          THEN TRIM(
            REGEXP_REPLACE(
              TRIM(raw.Channel_Name),
              '(?i)^Paid Social - ',
              ''
            )
          )


        /*
          Defensive fallback for a valid future Paid Social
          channel name without the standard prefix.
        */
        ELSE TRIM(raw.Channel_Name)

      END                                                       AS platform,


      TRY_CAST(
        raw.Spend AS DOUBLE
      )                                                         AS spend


    FROM
      prdrzranalytics.lab42.media_analytics_integrated_snapshot raw


    WHERE
      raw.Date IS NOT NULL


      AND UPPER(TRIM(raw.Channel_Group_Name)) = 'PAID SOCIAL'


      AND UPPER(TRIM(raw.Agency)) = 'INHOUSE'


      /*
        Approved Paid Social LOB scope.

        Fiber is not added to Paid Social.
      */
      AND UPPER(TRIM(raw.LOB)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )

  ),



  /* ===============================================================================================
     PAID SEARCH

     Approved platforms:
       Google
       Bing

     Approved source LOBs:
       Postpaid
       HSI / Broadband
       Fiber

     Fiber remains FIBER.

     No filters are applied to:
       account_id
       account_name
       campaign_type
       advertising_channel_type
       advertising_channel_sub_type
       bidding_strategy_type
       serving_status

     Therefore qualifying SEARCH / SHOPPING / PERFORMANCE_MAX / DISCOVERY
     activity remains included.

     Metro / TFB remain excluded.
     =============================================================================================== */

  PaidSearchMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


      /*
        Preserve selected source-normalized LOB.

        In particular:
          Fiber -> FIBER

        Silver performs canonicalization.
      */
      UPPER(
        TRIM(raw.lob)
      )                                                         AS lob,


      'Paid Search'                                             AS channel_group,


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
          Defensive fallback only.

          WHERE below restricts ingestion to the approved
          Google / Bing platform universe.
        */
        ELSE 'Unknown'

      END                                                       AS platform,


      TRY_CAST(
        raw.cost AS DOUBLE
      )                                                         AS spend


    FROM
      prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw


    WHERE
      raw.date IS NOT NULL


      /*
        Approved Paid Search LOB scope.

        Fiber is included and retained independently.
      */
      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND',
        'FIBER'
      )


      /*
        Approved Paid Search platform universe.

        No account / campaign / advertising-channel /
        bidding-strategy / serving-status filtering.
      */
      AND UPPER(TRIM(raw.ad_platform)) IN (
        'GOOGLE',
        'GOOGLE ADS',
        'GOOGLE_ADS',
        'BING',
        'MICROSOFT',
        'MICROSOFT ADS',
        'MICROSOFT_ADS'
      )

  ),



  /* ===============================================================================================
     UNION APPROVED ATOMIC SOURCE ROWS
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
         x source-normalized approved lob
         x channel_group
         x normalized platform

     Possible LOB values:

       POSTPAID
       CONSUMER POSTPAID
       HSI
       BROADBAND
       FIBER

     No synthetic ALL LOB exists in Bronze.
     =============================================================================================== */

  SELECT

    week_sun_sat,

    lob,

    channel_group,

    platform,

    SUM(
      COALESCE(spend, 0)
    )                                                           AS spend


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