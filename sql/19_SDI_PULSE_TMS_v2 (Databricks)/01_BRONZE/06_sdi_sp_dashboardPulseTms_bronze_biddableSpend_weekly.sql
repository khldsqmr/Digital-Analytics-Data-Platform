/* =================================================================================================
FILE:         06_sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql
LAYER:        Bronze Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly

PURPOSE:
  Creates/refreshes:
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly

  Combines the approved primary sources for the three Biddable media channels:

    Programmatic
      -> prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

    Paid Social
      -> prdrzranalytics.lab42.media_analytics_integrated_snapshot

    Paid Search
      -> prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily

  Bronze remains an unfiltered LOB landing layer within the APPROVED Biddable
  channel/platform scope.

  LOB semantic canonicalization is intentionally deferred to Silver.

IMPORTANT PAID SOCIAL CHANGE:
  The previous source:

    prd_dbi_analytics.improvado.mrt_paidsocial_pivot

  has been replaced with:

    prdrzranalytics.lab42.media_analytics_integrated_snapshot

  because reconciliation showed Integrated has materially broader campaign/spend
  coverage, especially for Meta and TikTok.

PAID SOCIAL SCOPE:
  media_analytics_integrated_snapshot contains more than the existing Biddable
  Paid Social universe.

  This Bronze therefore intentionally includes ONLY the current approved/core
  Biddable Paid Social platforms:

    Meta
    Pinterest
    TikTok
    Snapchat
    LinkedIn
    X
    Reddit

  The following Integrated-only categories are NOT included automatically:

    General MFC
    Indirect
    Nextdoor
    Creator
    Digital Sponsorship

  Those should only be added after their reporting scope is explicitly approved.

META:
  Facebook and Instagram rows are consolidated to platform = 'Meta'.

X:
  Integrated's "Paid Social - Twitter" is standardized to platform = 'X'.

LOB:
  Programmatic:
    raw.lob

  Paid Search:
    raw.lob

  Paid Social Integrated:
    raw.Brand

  Brand is intentionally used instead of Integrated's LOB because Brand represents
  the higher-level Postpaid / Broadband / Prepaid / TFB-style classification needed
  by PulseTMS.

  Bronze only applies UPPER/TRIM normalization.

  Examples therefore may include:
    POSTPAID
    HSI
    BROADBAND
    PREPAID
    TFB
    etc.

  HSI and BROADBAND are canonicalized together in Silver.

WEEK:
  All three sources are normalized from daily grain to Sunday-Saturday reporting weeks:

    week_sun_sat =
      date_add(date, 7 - dayofweek(date))

GRAIN:
  week_sun_sat
    x lob
    x channel_group
    x platform

PLATFORM:
  Programmatic -> DSP
  Paid Social  -> mapped Channel_Name
  Paid Search  -> ad_platform

NOTE ON READINESS:
  This table being populated for a Saturday does NOT itself mean that every source
  has fully settled.

  Source-readiness monitoring remains separate from calendar completeness.

  Current observed pattern:
    Paid Search   -> Monday morning
    Programmatic  -> approximately Monday 10 AM ET
    Paid Social   -> platform-dependent and subject to later backfill

  Therefore Silver's is_complete_period remains a QGP/calendar completeness flag,
  not a source-settlement SLA flag.
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
    channel_group
  )

  COMMENT '
    PulseTMS Bronze — Biddable Spend.

    Approved primary sources:
      Programmatic = Improvado Programmatic
      Paid Social  = Media Analytics Integrated Snapshot
      Paid Search  = SA360 Gold

    Grain:
      week_sun_sat x lob x channel_group x platform

    Bronze retains source-native high-level LOB values after casing/whitespace
    normalization. Cross-source semantic canonicalization such as HSI -> BROADBAND
    occurs in Silver.

    Paid Social is intentionally restricted to the approved/core Biddable platform
    universe. Integrated-only categories such as Indirect, Nextdoor, General MFC,
    Creator, and Digital Sponsorship are not automatically included.

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
        raw.date,
        7 - EXTRACT(DAYOFWEEK FROM raw.date)
      )                                                        AS week_sun_sat,

      UPPER(TRIM(raw.lob))                                     AS lob,

      'Programmatic'                                           AS channel_group,

      TRIM(raw.DSP)                                            AS platform,

      TRY_CAST(raw.spend AS DOUBLE)                            AS spend

    FROM
      prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw

    WHERE raw.date IS NOT NULL
  ),


  /* ===============================================================================================
     PAID SOCIAL
     SOURCE:
       media_analytics_integrated_snapshot

     IMPORTANT:
       Brand is used as the high-level LOB.
       Channel_Name is used to identify the approved Biddable platform universe.
     =============================================================================================== */

  PaidSocialMapped AS (
    SELECT
      date_add(
        CAST(raw.Date AS DATE),
        7 - EXTRACT(DAYOFWEEK FROM CAST(raw.Date AS DATE))
      ) AS week_sun_sat,

      /*
        IMPORTANT:
        Use Integrated LOB, NOT Brand.

        Brand examples:
          T-Mobile
          T-Mobile Home Internet
          T-Mobile Fiber
          Beyond the Smartphone

        LOB examples:
          POSTPAID
          BROADBAND
          PREPAID
          TFB

        PulseTMS currently consumes POSTPAID + BROADBAND.
      */
      UPPER(TRIM(raw.LOB)) AS lob,

      'Paid Social' AS channel_group,

      CASE
        WHEN raw.Channel_Name IN (
          'Paid Social - Facebook',
          'Paid Social - Instagram'
        )
          THEN 'Meta'

        WHEN raw.Channel_Name = 'Paid Social - TikTok'
          THEN 'TikTok'

        WHEN raw.Channel_Name = 'Paid Social - Snapchat'
          THEN 'Snapchat'

        WHEN raw.Channel_Name = 'Paid Social - Pinterest'
          THEN 'Pinterest'

        WHEN raw.Channel_Name = 'Paid Social - LinkedIn'
          THEN 'LinkedIn'

        WHEN raw.Channel_Name = 'Paid Social - Twitter'
          THEN 'X'

        WHEN raw.Channel_Name = 'Paid Social - Reddit'
          THEN 'Reddit'

      END AS platform,

      TRY_CAST(raw.Spend AS DOUBLE) AS spend

    FROM prdrzranalytics.lab42.media_analytics_integrated_snapshot raw

    WHERE raw.Date IS NOT NULL

      AND raw.Channel_Group_Name = 'Paid Social'

      /*
        Keep only the approved/core Biddable Paid Social platform universe.

        The following Integrated categories remain intentionally excluded:
          Indirect
          Nextdoor
          General MFC
          Creator
          Digital Sponsorship

        They should only be added after explicit business-scope approval.
      */
      AND raw.Channel_Name IN (
        'Paid Social - Facebook',
        'Paid Social - Instagram',
        'Paid Social - TikTok',
        'Paid Social - Snapchat',
        'Paid Social - Pinterest',
        'Paid Social - LinkedIn',
        'Paid Social - Twitter',
        'Paid Social - Reddit'
      )
  ),


  /* ===============================================================================================
     PAID SEARCH
     =============================================================================================== */

  PaidSearchMapped AS (

    SELECT
      date_add(
        raw.date,
        7 - EXTRACT(DAYOFWEEK FROM raw.date)
      )                                                        AS week_sun_sat,

      UPPER(TRIM(raw.lob))                                     AS lob,

      'Paid Search'                                            AS channel_group,

      TRIM(raw.ad_platform)                                    AS platform,

      TRY_CAST(raw.cost AS DOUBLE)                             AS spend

    FROM
      prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw

    WHERE raw.date IS NOT NULL
  ),


  /* ===============================================================================================
     UNION
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
     =============================================================================================== */

  SELECT
    week_sun_sat,
    lob,
    channel_group,
    platform,

    SUM(spend) AS spend

  FROM AllSources

  GROUP BY
    week_sun_sat,
    lob,
    channel_group,
    platform
  ;

END;