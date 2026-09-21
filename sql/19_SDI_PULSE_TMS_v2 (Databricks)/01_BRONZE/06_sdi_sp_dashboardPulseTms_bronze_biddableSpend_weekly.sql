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
  "Source universe" below documents what currently exists / has been observed in the upstream
  source.

  "PulseTMS selection" defines what is intentionally consumed by this Biddable pipeline.

  Observed category lists are documentation only unless the selection section explicitly says
  a category is filtered.


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

  INCLUDE:
    - Postpaid
    - HSI / Broadband

  RETAIN ALL QUALIFYING:
    - DSP values
    - account_id values
    - Channel values
    - Buy_Type values
    - campaign_type values
    - campaign / ad-group / placement combinations

  DO NOT:
    - whitelist DSPs
    - whitelist accounts
    - whitelist Channel
    - whitelist Buy_Type
    - whitelist campaign types

  EXCLUDE BY LOB SCOPE:
    - PrePaid
    - TFB
    - Archived

  Future/unmapped DSP values are intentionally retained if they satisfy the approved LOB scope.


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

  Account universe:
    Multiple account names / IDs exist underneath the Paid Social source.

PULSETMS SELECTION:

  REQUIRED:
    Channel_Group_Name = Paid Social
    Agency             = InHouse
    LOB                = Postpaid / Broadband

  RETAIN ALL QUALIFYING:
    - Channel_Name values
    - Account_ID values
    - Account_Name values
    - campaign values
    - other dimensions underneath the approved scope

  DO NOT:
    - whitelist Facebook / Instagram / TikTok / etc.
    - whitelist account names
    - whitelist account IDs

  Therefore a future Paid Social platform automatically flows through if it satisfies:

    Paid Social
      + InHouse
      + Postpaid/Broadband

  Integrated Snapshot raw.LOB is used.

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

  RETAIN ALL QUALIFYING:
    - accounts
    - campaign_type values
    - advertising_channel_type values
    - advertising_channel_sub_type values
    - bidding_strategy_type values
    - serving_status values

  DO NOT:
    - whitelist accounts
    - filter to literal SEARCH only
    - filter out Shopping
    - filter out Performance Max
    - filter out Discovery
    - filter campaign types

  EXCLUDE BY LOB SCOPE:
    Metro
    TFB
    Fiber
    other non-Postpaid / non-Broadband LOBs


===================================================================================================
BRONZE ARCHITECTURE
===================================================================================================

Bronze contains ATOMIC Biddable spend only.

GRAIN:

  week_sun_sat
    x lob
    x channel_group
    x platform

Bronze DOES NOT create reporting aggregates such as:

  All Channels
  Paid Search - All
  Paid Social - All
  Programmatic - All

Those reporting selections are generated in Silver.

This prevents aggregate rows from coexisting with the atomic rows inside Bronze.


---------------------------------------------------------------------------------------------------
LOB HANDLING
---------------------------------------------------------------------------------------------------

Bronze performs only source-level normalization:

  UPPER(TRIM(source_lob))

Approved semantic scope is:

  POSTPAID
  CONSUMER POSTPAID
  HSI
  BROADBAND

Silver canonicalizes:

  POSTPAID / CONSUMER POSTPAID
    -> POSTPAID

  HSI / BROADBAND
    -> BROADBAND


---------------------------------------------------------------------------------------------------
PROGRAMMATIC PLATFORM NORMALIZATION
---------------------------------------------------------------------------------------------------

Known DSP values are standardized:

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


---------------------------------------------------------------------------------------------------
PAID SOCIAL PLATFORM NORMALIZATION
---------------------------------------------------------------------------------------------------

No platform whitelist is used.

The common prefix:

  Paid Social -

is removed from Channel_Name.

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

Approved search platform universe:

  Google / Google Ads
    -> Google

  Bing / Microsoft / Microsoft Ads
    -> Bing


---------------------------------------------------------------------------------------------------
WEEK
---------------------------------------------------------------------------------------------------

All sources are normalized to the PulseTMS Sunday-Saturday reporting week:

  week_sun_sat =
    date_add(date, 7 - dayofweek(date))


---------------------------------------------------------------------------------------------------
SOURCE READINESS
---------------------------------------------------------------------------------------------------

Source selection and source readiness are separate concepts.

Do NOT filter spend based on current weekday or expected source-settlement timing.

Observed operational readiness is monitored separately.

is_complete_period downstream remains a QGP/calendar completeness indicator and MUST NOT be
treated as source-settlement confirmation.

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

    Approved sources:
      Programmatic = Improvado Programmatic
      Paid Social  = Media Analytics Integrated Snapshot
      Paid Search  = SA360 Gold

    Grain:
      week_sun_sat x lob x channel_group x platform

    Scope:
      Postpaid + Broadband only.

    Paid Social additionally requires Agency = InHouse.

    Bronze stores atomic platform-level spend only.

    No All Channels or channel-total rows are generated here.

    Paid Social uses Integrated Snapshot LOB, not Brand.

    LOB canonicalization occurs in Silver.

    Refreshed by:
      sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly
  '


  AS


  WITH


  /* ===============================================================================================
     PROGRAMMATIC

     APPROVED SELECTION:
       LOB = Postpaid / HSI / Broadband

     INTENTIONALLY NOT FILTERED:
       DSP
       account_id
       Channel
       Buy_Type
       campaign_type

     Future/unmapped DSP values are retained.
     =============================================================================================== */

  ProgrammaticMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


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
          Preserve a future/unmapped DSP rather than silently dropping it.
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
        Approved Biddable business LOB scope.

        CONSUMER POSTPAID is retained defensively as an equivalent
        source label if it appears in the future.
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

     APPROVED SELECTION:

       Channel_Group_Name = Paid Social
       Agency             = InHouse
       LOB                = Postpaid / Broadband

     INTENTIONALLY NOT FILTERED:
       Channel_Name / platform
       Account_ID
       Account_Name
       Account_Type
       Campaign_ID / Campaign_Name
       other qualifying Paid Social dimensions

     IMPORTANT:
       raw.LOB is used.
       raw.Brand is NOT used.

     Any future Channel_Name satisfying the approved scope automatically flows through.
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


        /*
          Standard Integrated naming:
            Paid Social - Facebook -> Facebook
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
          Defensive fallback if a future Paid Social Channel_Name
          does not contain the standard prefix.
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


      /*
        Approved Paid Social ownership scope.
      */
      AND UPPER(TRIM(raw.Agency)) = 'INHOUSE'


      /*
        Approved Biddable LOB scope.
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

     APPROVED SELECTION:

       Platform = Google / Bing
       LOB      = Postpaid / HSI / Broadband

     INTENTIONALLY NOT FILTERED:
       account_id
       campaign_type
       advertising_channel_type
       advertising_channel_sub_type
       bidding_strategy_type
       serving_status

     This means SEARCH / SHOPPING / PERFORMANCE_MAX / DISCOVERY and their corresponding
     campaign types remain in scope when they belong to the approved platform + LOB universe.
     =============================================================================================== */

  PaidSearchMapped AS (

    SELECT

      date_add(
        CAST(raw.date AS DATE),
        7 - dayofweek(CAST(raw.date AS DATE))
      )                                                         AS week_sun_sat,


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
          WHERE below restricts this source to the approved search
          platform universe, so this fallback is defensive only.
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
        Approved Biddable LOB scope.
      */
      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )


      /*
        Approved Paid Search platform universe.

        No account / campaign / advertising-channel filters are
        intentionally applied.
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

     IMPORTANT:
       No synthetic reporting totals are created here.
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
         x source-native approved lob
         x source channel group
         x normalized platform
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