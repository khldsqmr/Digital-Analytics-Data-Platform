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
  "Source universe" documents the values currently observed in each upstream source.

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
      + Postpaid/Broadband

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

  INCLUDE SOURCE LOB:
    Postpaid
    HSI / Broadband
    Fiber

  CURRENT REPORTING LOB MAPPING:

    Postpaid
      -> POSTPAID

    HSI / Broadband
      -> BROADBAND

    Fiber
      -> BROADBAND

  IMPORTANT:
    Fiber is included in the Paid Search pull but currently rolls into
    the existing BROADBAND reporting bucket.

    Fiber is NOT exposed as a standalone PulseTMS LOB at this time.

    This should be revisited when dedicated Fiber / TFB metrics and
    reporting are formally introduced.

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

  EXCLUDE:
    Metro
    TFB
    other non-approved Paid Search LOBs


===================================================================================================
BRONZE ARCHITECTURE
===================================================================================================

Bronze contains ATOMIC Biddable spend only.

GRAIN:

  week_sun_sat
    x lob
    x channel_group
    x platform


Bronze DOES NOT create reporting aggregate rows such as:

  All Channels
  Paid Search - All
  Paid Social - All
  Programmatic - All

Those reporting selections are generated in Silver.


---------------------------------------------------------------------------------------------------
CURRENT DOWNSTREAM LOB CONTRACT
---------------------------------------------------------------------------------------------------

The downstream Biddable reporting contract remains:

  POSTPAID
  BROADBAND

Source-specific treatment:

  Programmatic:
    Postpaid          -> POSTPAID
    HSI/Broadband     -> canonicalized to BROADBAND in Silver

  Paid Social:
    Postpaid          -> POSTPAID
    HSI/Broadband     -> canonicalized to BROADBAND in Silver

  Paid Search:
    Postpaid          -> POSTPAID
    HSI/Broadband     -> BROADBAND
    Fiber             -> BROADBAND

Fiber is intentionally mapped locally inside Paid Search because standalone
Fiber reporting is not yet part of the PulseTMS Biddable metric architecture.


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

    Current reporting LOB contract:
      POSTPAID
      BROADBAND

    Source selection:

      Programmatic:
        Postpaid + HSI/Broadband

      Paid Social:
        Paid Social + InHouse + Postpaid/HSI/Broadband

      Paid Search:
        Google/Bing + Postpaid/HSI/Broadband/Fiber

    Paid Search Fiber is intentionally mapped to BROADBAND because
    standalone Fiber reporting is not yet part of PulseTMS.

    Bronze contains atomic platform-level rows only.

    Reporting totals / selections are generated in Silver.

    Refreshed by:
      sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     PROGRAMMATIC

     SELECT:
       Postpaid
       HSI / Broadband

     DO NOT FILTER:
       DSP
       account_id
       Channel
       Buy_Type
       campaign_type

     Unknown / future DSP values remain included.
     =============================================================================================== */

  ProgrammaticMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


      /*
        Preserve source LOB here.
        Cross-source HSI/BROADBAND canonicalization occurs in Silver.
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
          Preserve future / unmapped DSPs.
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


      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )

  ),



  /* ===============================================================================================
     PAID SOCIAL

     SELECT:
       Channel_Group_Name = Paid Social
       Agency             = InHouse
       LOB                = Postpaid / HSI / Broadband

     DO NOT FILTER:
       Channel_Name / platform
       Account_ID
       Account_Name
       Account_Type
       Campaign_ID
       Campaign_Name

     IMPORTANT:
       Use raw.LOB, NOT raw.Brand.

       Platform remains dynamic.
     =============================================================================================== */

  PaidSocialMapped AS (

    SELECT

      date_add(
        CAST(raw.Date AS DATE),
        7 - dayofweek(CAST(raw.Date AS DATE))
      )                                                         AS week_sun_sat,


      UPPER(
        TRIM(raw.LOB)
      )                                                         AS lob,


      'Paid Social'                                             AS channel_group,


      CASE

        WHEN NULLIF(TRIM(raw.Channel_Name), '') IS NULL
          THEN 'Unknown'


        WHEN UPPER(TRIM(raw.Channel_Name)) LIKE 'PAID SOCIAL - %'
          THEN TRIM(
            REGEXP_REPLACE(
              TRIM(raw.Channel_Name),
              '(?i)^Paid Social - ',
              ''
            )
          )


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


      AND UPPER(TRIM(raw.LOB)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )

  ),



  /* ===============================================================================================
     PAID SEARCH

     SELECT:

       Platform:
         Google
         Bing

       Source LOB:
         Postpaid
         HSI / Broadband
         Fiber

     LOCAL REPORTING MAPPING:

       Postpaid
         -> POSTPAID

       HSI / Broadband / Fiber
         -> BROADBAND

     Fiber is therefore included in Paid Search spend without creating
     a standalone Fiber LOB downstream.

     DO NOT FILTER:
       account_id
       account_name
       campaign_type
       advertising_channel_type
       advertising_channel_sub_type
       bidding_strategy_type
       serving_status

     Metro / TFB remain excluded.
     =============================================================================================== */

  PaidSearchMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


      /*
        Paid Search-specific LOB mapping.

        Fiber rolls into the existing BROADBAND reporting bucket
        until standalone Fiber reporting is formally introduced.
      */
      CASE

        WHEN UPPER(TRIM(raw.lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'


        WHEN UPPER(TRIM(raw.lob)) IN (
          'HSI',
          'BROADBAND',
          'FIBER'
        )
          THEN 'BROADBAND'

      END                                                       AS lob,


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
          WHERE below restricts this source to Google/Bing.
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
        Fiber is explicitly included in the Paid Search pull.
      */
      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND',
        'FIBER'
      )


      /*
        Approved Paid Search platform scope.
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
     UNION APPROVED ATOMIC SOURCES
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

     Expected downstream LOB universe:
       POSTPAID
       HSI / BROADBAND

     No standalone FIBER is expected because Paid Search Fiber has already
     been mapped to BROADBAND above.
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