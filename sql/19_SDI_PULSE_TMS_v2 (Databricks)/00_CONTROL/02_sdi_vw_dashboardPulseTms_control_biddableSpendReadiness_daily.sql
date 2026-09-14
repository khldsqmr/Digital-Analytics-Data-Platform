/* =================================================================================================
FILE:         sdi_vw_dashboardPulseTms_control_biddableSpendReadiness_daily.sql
PLATFORM:     Databricks
LAYER:        Control / Monitoring View
VIEW NAME:    sdi_vw_dashboardPulseTms_control_biddableSpendReadiness_daily

PURPOSE:
  Single Biddable Spend readiness/control view for PulseTMS.

  The view monitors the prior completed Sunday-Saturday reporting week across:

    PROGRAMMATIC
      prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

    PAID SEARCH
      prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily

    PAID SOCIAL
      prdrzranalytics.lab42.media_analytics_integrated_snapshot

  It returns TWO record types from the SAME view:

    record_type = 'PLATFORM'
      One row per:
        prior_week_end x channel_group x platform

      Examples:
        Programmatic / The Trade Desk
        Programmatic / DV360
        Paid Search / Google
        Paid Search / Bing
        Paid Social / Meta
        Paid Social / TikTok
        Paid Social / X

    record_type = 'CHANNEL'
      One summarized row for:
        Programmatic
        Paid Search
        Paid Social

      These rows include:
        - active/core platform count
        - ready platform count
        - pending platform count
        - ready platform list
        - pending platform list
        - overall channel readiness
        - dashboard-ready comment

CURRENT REPORTING LOB SCOPE:
  POSTPAID + BROADBAND

  The readiness view intentionally follows the same current business scope as
  PulseTMS Biddable Gold.

  Other LOBs such as:
    TFB
    PREPAID
    METRO
    FIBER
    TMONEY

  are NOT currently used to determine Biddable readiness.

  FUTURE:
    When additional LOBs become part of the approved PulseTMS spend total,
    update the canonical_lob filters in this control view at the same time
    the Wide/Gold spend totals are expanded.

PRIOR WEEK:
  PulseTMS reporting week:
    Sunday through Saturday.

  The view checks the most recently COMPLETED Saturday.

  Example:
    If run on Monday Sep 14:
      prior_week_start = Sunday Sep 6
      prior_week_end   = Saturday Sep 12

READINESS CONCEPT:
  IMPORTANT:
    "7/7 days present" is NOT the same thing as proving that spend will never
    change again.

  Historical validation showed that some platforms, especially Paid Social,
  can receive later revisions/backfill after all seven dates exist.

  Therefore this view separates:

    days_present / date_coverage_pct
      -> calendar/source-date coverage

    expected_ready_by
      -> historically observed safe operational window

    readiness_status
      -> operational classification

    backfill_risk
      -> risk of later historical revisions

    operationally_safe_to_use
      -> practical reporting flag

CURRENT OBSERVED SAFE WINDOWS:

  PAID SEARCH:
    Google / Bing
      Monday 8 AM ET

  PROGRAMMATIC:
    Monday 10 AM ET

    NOTE:
      Programmatic readiness was validated at overall Programmatic level.
      The Monday 10 AM window is therefore applied to individual DSP rows as
      an operational monitoring rule, but the DSP-level SLA has not been
      independently benchmarked.

  PAID SOCIAL:
    TikTok
      Tuesday 10 AM ET

    Snapchat
      Tuesday 10 AM ET

    Pinterest
      Tuesday 10 AM ET

    LinkedIn
      Thursday 6 AM ET conservative window
      Usually earlier, but one historical week completed later.

    X
      Thursday 6 AM ET

    Meta
      Thursday 6 AM ET conservative operational window.
      Meta remains HIGH backfill risk because later historical changes have
      been observed.

    Reddit
      Included for visibility because it is part of the approved Bronze
      platform universe, but its readiness SLA has not yet been independently
      validated.

EXPECTED PLATFORM / ACTIVITY LOGIC:
  A known platform should not automatically block channel readiness simply
  because it had zero spend in a genuinely inactive week.

  Therefore this view looks at the previous FOUR completed weeks.

  A platform is considered expected for the current prior week when:

    - it has rows in the prior week
      OR
    - it had activity in at least one of the preceding four weeks

  If it had recent historical activity but zero prior-week rows:
    -> NO DATA - CHECK

  If it has no prior-week rows and no recent activity:
    -> NO ACTIVITY / NOT EXPECTED

  This prevents an inactive DSP/platform from unnecessarily blocking the
  channel while still catching platforms that normally run but suddenly vanish.

PAID SOCIAL CORE PLATFORM LOGIC:
  Core platforms used for channel readiness:

    Meta
    TikTok
    Snapchat
    Pinterest
    LinkedIn
    X

  Reddit:
    monitored but currently NOT used to determine core Paid Social readiness
    until its SLA is validated.

STATUS VALUES:
  READY
  READY - BACKFILL RISK
  COMPLETE - BEFORE SAFE WINDOW
  NOT READY
  NO DATA - CHECK
  NO ACTIVITY / NOT EXPECTED
  CHECK - SLA NOT VALIDATED
  IN PROGRESS

TABLEAU USAGE:

  Platform detail:
    WHERE record_type = 'PLATFORM'

  Dashboard-level comment:
    WHERE record_type = 'CHANNEL'

  Recommended Tableau field:
    dashboard_comment

IMPORTANT:
  This view is a control/monitoring object.
  It does not alter Bronze, Silver, or Gold spend.
================================================================================================= */


CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_control_biddableSpendReadiness_daily
AS

WITH


/* ===============================================================================================
   PARAMETERS
   =============================================================================================== */

Params AS (

  SELECT

    current_date() AS run_date,


    /* Most recently COMPLETED Saturday */
    date_sub(
      current_date(),
      dayofweek(current_date())
    ) AS prior_week_end,


    /* Sunday six days before the completed Saturday */
    date_sub(
      date_sub(
        current_date(),
        dayofweek(current_date())
      ),
      6
    ) AS prior_week_start,


    /*
      Four weeks immediately preceding the prior reporting week.

      Used only to determine whether a platform is normally/recently active.
    */
    date_sub(
      date_sub(
        date_sub(
          current_date(),
          dayofweek(current_date())
        ),
        6
      ),
      28
    ) AS recent_history_start,


    date_sub(
      date_sub(
        current_date(),
        dayofweek(current_date())
      ),
      7
    ) AS recent_history_end,


    /*
      ET local clock representation.

      America/New_York automatically handles EST / EDT.
    */
    from_utc_timestamp(
      current_timestamp(),
      'America/New_York'
    ) AS as_of_timestamp_et

),


/* ===============================================================================================
   EXPECTED PLATFORM CONFIGURATION

   ready_day_offset is relative to prior Saturday:

     Monday    = 2
     Tuesday   = 3
     Thursday  = 5

   is_core_platform:
     Determines whether the platform participates in channel-level readiness.

   Reddit is visible but currently not core because its SLA has not yet been
   independently validated.
   =============================================================================================== */

ExpectedPlatforms AS (

  SELECT *
  FROM VALUES


    /* -------------------------------------------------------------------------------------------
       PROGRAMMATIC
       ------------------------------------------------------------------------------------------- */

    (
      'Programmatic',
      'Amazon DSP',
      'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr',
      TRUE,
      2,
      10,
      0,
      'Monday 10 AM ET',
      'MEDIUM',
      'LOW',
      'Programmatic channel-level readiness window applied to this DSP.'
    ),

    (
      'Programmatic',
      'The Trade Desk',
      'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr',
      TRUE,
      2,
      10,
      0,
      'Monday 10 AM ET',
      'MEDIUM',
      'LOW',
      'Programmatic channel-level readiness window applied to this DSP.'
    ),

    (
      'Programmatic',
      'DV360',
      'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr',
      TRUE,
      2,
      10,
      0,
      'Monday 10 AM ET',
      'MEDIUM',
      'LOW',
      'Programmatic channel-level readiness window applied to this DSP.'
    ),

    (
      'Programmatic',
      'Blis',
      'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr',
      TRUE,
      2,
      10,
      0,
      'Monday 10 AM ET',
      'MEDIUM',
      'LOW',
      'Programmatic channel-level readiness window applied to this DSP.'
    ),

    (
      'Programmatic',
      'Google Ads',
      'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr',
      TRUE,
      2,
      10,
      0,
      'Monday 10 AM ET',
      'MEDIUM',
      'LOW',
      'Programmatic channel-level readiness window applied to this DSP.'
    ),


    /* -------------------------------------------------------------------------------------------
       PAID SEARCH
       ------------------------------------------------------------------------------------------- */

    (
      'Paid Search',
      'Google',
      'prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily',
      TRUE,
      2,
      8,
      0,
      'Monday 8 AM ET',
      'HIGH',
      'LOW',
      'Historical checks show Paid Search effectively complete by Monday morning.'
    ),

    (
      'Paid Search',
      'Bing',
      'prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily',
      TRUE,
      2,
      8,
      0,
      'Monday 8 AM ET',
      'HIGH',
      'LOW',
      'Historical checks show Paid Search effectively complete by Monday morning.'
    ),


    /* -------------------------------------------------------------------------------------------
       PAID SOCIAL
       ------------------------------------------------------------------------------------------- */

    (
      'Paid Social',
      'TikTok',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      3,
      10,
      0,
      'Tuesday 10 AM ET',
      'HIGH',
      'LOW',
      'Consistently Tuesday-ready in the normal historical weeks tested.'
    ),

    (
      'Paid Social',
      'Snapchat',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      3,
      10,
      0,
      'Tuesday 10 AM ET',
      'HIGH',
      'LOW',
      'Consistently Tuesday-ready in the normal historical weeks tested.'
    ),

    (
      'Paid Social',
      'Pinterest',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      3,
      10,
      0,
      'Tuesday 10 AM ET',
      'HIGH',
      'LOW',
      'Consistently Tuesday-ready in the normal historical weeks tested.'
    ),

    (
      'Paid Social',
      'LinkedIn',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      5,
      6,
      0,
      'Thursday 6 AM ET',
      'MEDIUM',
      'MEDIUM',
      'Usually Tuesday-ready; Thursday is used as the conservative safe window.'
    ),

    (
      'Paid Social',
      'X',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      5,
      6,
      0,
      'Thursday 6 AM ET',
      'MEDIUM',
      'MEDIUM',
      'Historical testing showed X can continue filling after Tuesday.'
    ),

    (
      'Paid Social',
      'Meta',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      TRUE,
      5,
      6,
      0,
      'Thursday 6 AM ET',
      'MEDIUM',
      'HIGH',
      'Conservative operational window; later historical spend revisions have been observed.'
    ),

    (
      'Paid Social',
      'Reddit',
      'prdrzranalytics.lab42.media_analytics_integrated_snapshot',
      FALSE,
      5,
      6,
      0,
      'Not yet validated',
      'UNVALIDATED',
      'UNKNOWN',
      'Monitored for visibility but currently excluded from core Paid Social readiness.'
    )


  AS t(
    channel_group,
    platform,
    source_table,
    is_core_platform,
    ready_day_offset,
    ready_hour_et,
    ready_minute_et,
    expected_ready_by,
    sla_confidence,
    backfill_risk,
    sla_note
  )

),


/* ===============================================================================================
   PROGRAMMATIC SOURCE

   Scope:
     POSTPAID + BROADBAND only.

   Source LOB normalization:
     POSTPAID / CONSUMER POSTPAID -> POSTPAID
     HSI / BROADBAND              -> BROADBAND
   =============================================================================================== */

ProgrammaticBase AS (

  SELECT

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


      ELSE TRIM(raw.DSP)

    END AS platform,


    CASE

      WHEN UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID'
      )
        THEN 'POSTPAID'


      WHEN UPPER(TRIM(raw.lob)) IN (
        'HSI',
        'BROADBAND'
      )
        THEN 'BROADBAND'


      ELSE UPPER(TRIM(raw.lob))

    END AS canonical_lob,


    CAST(raw.date AS DATE) AS activity_date,

    TRY_CAST(raw.spend AS DOUBLE) AS spend


  FROM
    prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw

  CROSS JOIN Params p

  WHERE
    raw.date BETWEEN p.recent_history_start AND p.prior_week_end

),


ProgrammaticDaily AS (

  SELECT
    channel_group,
    platform,
    canonical_lob,
    activity_date,
    spend

  FROM ProgrammaticBase

  WHERE
    canonical_lob IN (
      'POSTPAID',
      'BROADBAND'
    )

    AND platform IS NOT NULL

),


/* ===============================================================================================
   PAID SEARCH SOURCE

   Scope:
     POSTPAID + BROADBAND only.
   =============================================================================================== */

PaidSearchBase AS (

  SELECT

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


      ELSE TRIM(raw.ad_platform)

    END AS platform,


    CASE

      WHEN UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID'
      )
        THEN 'POSTPAID'


      WHEN UPPER(TRIM(raw.lob)) IN (
        'HSI',
        'BROADBAND'
      )
        THEN 'BROADBAND'


      ELSE UPPER(TRIM(raw.lob))

    END AS canonical_lob,


    CAST(raw.date AS DATE) AS activity_date,

    TRY_CAST(raw.cost AS DOUBLE) AS spend


  FROM
    prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw

  CROSS JOIN Params p

  WHERE
    raw.date BETWEEN p.recent_history_start AND p.prior_week_end

),


PaidSearchDaily AS (

  SELECT
    channel_group,
    platform,
    canonical_lob,
    activity_date,
    spend

  FROM PaidSearchBase

  WHERE
    canonical_lob IN (
      'POSTPAID',
      'BROADBAND'
    )

    AND platform IS NOT NULL

),


/* ===============================================================================================
   PAID SOCIAL SOURCE

   Source:
     media_analytics_integrated_snapshot

   Scope:
     POSTPAID + BROADBAND only.

   Brand is used as the high-level business LOB.

   Platform normalization:
     Facebook + Instagram -> Meta
     Twitter              -> X
   =============================================================================================== */

PaidSocialBase AS (

  SELECT

    'Paid Social' AS channel_group,


    CASE

      WHEN UPPER(raw.Channel_Name) LIKE '%FACEBOOK%'
        OR UPPER(raw.Channel_Name) LIKE '%INSTAGRAM%'
        THEN 'Meta'


      WHEN UPPER(raw.Channel_Name) LIKE '%TIKTOK%'
        THEN 'TikTok'


      WHEN UPPER(raw.Channel_Name) LIKE '%SNAPCHAT%'
        THEN 'Snapchat'


      WHEN UPPER(raw.Channel_Name) LIKE '%PINTEREST%'
        THEN 'Pinterest'


      WHEN UPPER(raw.Channel_Name) LIKE '%LINKEDIN%'
        THEN 'LinkedIn'


      WHEN UPPER(raw.Channel_Name) LIKE '%TWITTER%'
        OR UPPER(raw.Channel_Name) LIKE '%X / TWITTER%'
        THEN 'X'


      WHEN UPPER(raw.Channel_Name) LIKE '%REDDIT%'
        THEN 'Reddit'


      ELSE NULL

    END AS platform,


    CASE

      WHEN UPPER(TRIM(raw.Brand)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID'
      )
        THEN 'POSTPAID'


      WHEN UPPER(TRIM(raw.Brand)) IN (
        'HSI',
        'BROADBAND'
      )
        THEN 'BROADBAND'


      ELSE UPPER(TRIM(raw.Brand))

    END AS canonical_lob,


    CAST(raw.Date AS DATE) AS activity_date,

    TRY_CAST(raw.Spend AS DOUBLE) AS spend


  FROM
    prdrzranalytics.lab42.media_analytics_integrated_snapshot raw

  CROSS JOIN Params p

  WHERE
    raw.Date BETWEEN p.recent_history_start AND p.prior_week_end

    AND raw.Channel_Group_Name = 'Paid Social'

),


PaidSocialDaily AS (

  SELECT
    channel_group,
    platform,
    canonical_lob,
    activity_date,
    spend

  FROM PaidSocialBase

  WHERE
    canonical_lob IN (
      'POSTPAID',
      'BROADBAND'
    )

    AND platform IS NOT NULL

),


/* ===============================================================================================
   UNIFIED DAILY SOURCE
   =============================================================================================== */

AllDaily AS (

  SELECT * FROM ProgrammaticDaily

  UNION ALL

  SELECT * FROM PaidSearchDaily

  UNION ALL

  SELECT * FROM PaidSocialDaily

),


/* ===============================================================================================
   COLLAPSE SOURCE ROWS TO PLATFORM x LOB x DATE
   =============================================================================================== */

DailyPlatformLob AS (

  SELECT

    channel_group,
    platform,
    canonical_lob,
    activity_date,

    SUM(
      COALESCE(spend, 0)
    ) AS day_spend

  FROM AllDaily

  GROUP BY
    channel_group,
    platform,
    canonical_lob,
    activity_date

),


/* ===============================================================================================
   PRIOR-WEEK PLATFORM ACTUALS
   =============================================================================================== */

PriorWeekActual AS (

  SELECT

    d.channel_group,
    d.platform,


    COUNT(
      DISTINCT d.activity_date
    ) AS days_present,


    COUNT(
      DISTINCT CASE
        WHEN d.canonical_lob = 'POSTPAID'
          THEN d.activity_date
      END
    ) AS postpaid_days_present,


    COUNT(
      DISTINCT CASE
        WHEN d.canonical_lob = 'BROADBAND'
          THEN d.activity_date
      END
    ) AS broadband_days_present,


    MIN(
      d.activity_date
    ) AS first_data_date,


    MAX(
      d.activity_date
    ) AS latest_data_date,


    SUM(
      d.day_spend
    ) AS week_spend,


    SUM(
      CASE
        WHEN d.canonical_lob = 'POSTPAID'
          THEN d.day_spend
        ELSE 0
      END
    ) AS postpaid_week_spend,


    SUM(
      CASE
        WHEN d.canonical_lob = 'BROADBAND'
          THEN d.day_spend
        ELSE 0
      END
    ) AS broadband_week_spend


  FROM DailyPlatformLob d

  CROSS JOIN Params p

  WHERE
    d.activity_date
      BETWEEN p.prior_week_start
          AND p.prior_week_end


  GROUP BY
    d.channel_group,
    d.platform

),


/* ===============================================================================================
   RECENT PLATFORM ACTIVITY

   Counts how many of the preceding four weeks contained at least one source row.

   This helps distinguish:

     temporarily inactive platform

   from:

     platform that normally runs but suddenly has no prior-week data
   =============================================================================================== */

RecentActivity AS (

  SELECT

    d.channel_group,
    d.platform,


    COUNT(
      DISTINCT date_add(
        d.activity_date,
        7 - dayofweek(d.activity_date)
      )
    ) AS recent_active_weeks


  FROM DailyPlatformLob d

  CROSS JOIN Params p

  WHERE
    d.activity_date
      BETWEEN p.recent_history_start
          AND p.recent_history_end


  GROUP BY
    d.channel_group,
    d.platform

),


/* ===============================================================================================
   EXPECTED PLATFORM SCAFFOLD + SLA TIME
   =============================================================================================== */

Scaffold AS (

  SELECT

    p.run_date,
    p.prior_week_start,
    p.prior_week_end,
    p.as_of_timestamp_et,

    e.channel_group,
    e.platform,
    e.source_table,

    e.is_core_platform,

    e.expected_ready_by,
    e.sla_confidence,
    e.backfill_risk,
    e.sla_note,


    /*
      Local ET operational cutoff.
    */
    TO_TIMESTAMP(
      CONCAT(

        CAST(
          date_add(
            p.prior_week_end,
            e.ready_day_offset
          )
          AS STRING
        ),

        ' ',

        LPAD(
          CAST(e.ready_hour_et AS STRING),
          2,
          '0'
        ),

        ':',

        LPAD(
          CAST(e.ready_minute_et AS STRING),
          2,
          '0'
        ),

        ':00'

      )
    ) AS expected_ready_at_et


  FROM Params p

  CROSS JOIN ExpectedPlatforms e

),


/* ===============================================================================================
   PLATFORM COVERAGE + EXPECTED-ACTIVITY LOGIC
   =============================================================================================== */

PlatformCoverage AS (

  SELECT

    s.run_date,
    s.as_of_timestamp_et,

    s.prior_week_start,
    s.prior_week_end,

    s.channel_group,
    s.platform,
    s.source_table,

    s.is_core_platform,

    COALESCE(r.recent_active_weeks, 0)
      AS recent_active_weeks,


    COALESCE(a.days_present, 0)
      AS days_present,


    7
      AS expected_days,


    COALESCE(a.postpaid_days_present, 0)
      AS postpaid_days_present,


    COALESCE(a.broadband_days_present, 0)
      AS broadband_days_present,


    a.first_data_date,
    a.latest_data_date,


    COALESCE(a.week_spend, 0)
      AS week_spend,


    COALESCE(a.postpaid_week_spend, 0)
      AS postpaid_week_spend,


    COALESCE(a.broadband_week_spend, 0)
      AS broadband_week_spend,


    ROUND(
      COALESCE(a.days_present, 0) / 7.0 * 100,
      2
    ) AS date_coverage_pct,


    CASE

      WHEN COALESCE(a.days_present, 0) = 7
        THEN TRUE

      ELSE FALSE

    END AS calendar_complete_flag,


    /*
      Expected this week when:
        - current prior week has rows
          OR
        - platform was active during preceding four weeks
    */
    CASE

      WHEN COALESCE(a.days_present, 0) > 0
        THEN TRUE

      WHEN COALESCE(r.recent_active_weeks, 0) > 0
        THEN TRUE

      ELSE FALSE

    END AS expected_this_week_flag,


    s.expected_ready_by,
    s.expected_ready_at_et,


    CASE

      WHEN s.as_of_timestamp_et >= s.expected_ready_at_et
        THEN TRUE

      ELSE FALSE

    END AS safe_window_reached_flag,


    s.sla_confidence,
    s.backfill_risk,
    s.sla_note


  FROM Scaffold s


  LEFT JOIN PriorWeekActual a

    ON  a.channel_group = s.channel_group
    AND a.platform      = s.platform


  LEFT JOIN RecentActivity r

    ON  r.channel_group = s.channel_group
    AND r.platform      = s.platform

),


/* ===============================================================================================
   PLATFORM READINESS CLASSIFICATION
   =============================================================================================== */

PlatformStatus AS (

  SELECT

    *,

    CASE


      /* -----------------------------------------------------------------------------------------
         No prior-week data and no recent platform activity.

         Treat as inactive/not expected rather than automatically failing the channel.
         ----------------------------------------------------------------------------------------- */

      WHEN expected_this_week_flag = FALSE

        THEN 'NO ACTIVITY / NOT EXPECTED'


      /* -----------------------------------------------------------------------------------------
         Platform was recently active but prior-week data completely disappeared.
         ----------------------------------------------------------------------------------------- */

      WHEN expected_this_week_flag = TRUE
       AND days_present = 0

        THEN 'NO DATA - CHECK'


      /* -----------------------------------------------------------------------------------------
         Prior week exists but does not contain all seven calendar dates.
         ----------------------------------------------------------------------------------------- */

      WHEN days_present < 7

        THEN 'NOT READY'


      /* -----------------------------------------------------------------------------------------
         Reddit SLA is not yet independently validated.
         ----------------------------------------------------------------------------------------- */

      WHEN sla_confidence = 'UNVALIDATED'

        THEN 'CHECK - SLA NOT VALIDATED'


      /* -----------------------------------------------------------------------------------------
         All seven dates are present but the conservative operational window has not yet passed.
         ----------------------------------------------------------------------------------------- */

      WHEN calendar_complete_flag = TRUE
       AND safe_window_reached_flag = FALSE

        THEN 'COMPLETE - BEFORE SAFE WINDOW'


      /* -----------------------------------------------------------------------------------------
         Ready, but historical changes/backfills remain a material consideration.
         ----------------------------------------------------------------------------------------- */

      WHEN calendar_complete_flag = TRUE
       AND safe_window_reached_flag = TRUE
       AND backfill_risk = 'HIGH'

        THEN 'READY - BACKFILL RISK'


      /* -----------------------------------------------------------------------------------------
         Standard ready.
         ----------------------------------------------------------------------------------------- */

      WHEN calendar_complete_flag = TRUE
       AND safe_window_reached_flag = TRUE

        THEN 'READY'


      ELSE 'NOT READY'

    END AS readiness_status,


    CASE


      /* Inactive/non-expected platform should not block the channel */
      WHEN expected_this_week_flag = FALSE
        THEN CAST(NULL AS BOOLEAN)


      /* Unvalidated SLA cannot be automatically certified */
      WHEN sla_confidence = 'UNVALIDATED'
        THEN FALSE


      /* Need all 7 dates */
      WHEN days_present <> 7
        THEN FALSE


      /* Need conservative safe reporting window */
      WHEN safe_window_reached_flag = FALSE
        THEN FALSE


      ELSE TRUE

    END AS operationally_safe_to_use


  FROM PlatformCoverage

),


/* ===============================================================================================
   CHANNEL-LEVEL AGGREGATION

   Only active/expected CORE platforms determine core channel readiness.

   Example:
     Paid Social Reddit does not currently block core readiness because its SLA
     has not yet been validated.
   =============================================================================================== */

ChannelAgg AS (

  SELECT

    run_date,
    as_of_timestamp_et,
    prior_week_start,
    prior_week_end,

    channel_group,


    SUM(
      CASE
        WHEN is_core_platform = TRUE
         AND expected_this_week_flag = TRUE
          THEN 1
        ELSE 0
      END
    ) AS required_platform_count,


    SUM(
      CASE
        WHEN is_core_platform = TRUE
         AND expected_this_week_flag = TRUE
         AND operationally_safe_to_use = TRUE
          THEN 1
        ELSE 0
      END
    ) AS ready_platform_count,


    SUM(
      CASE
        WHEN is_core_platform = TRUE
         AND expected_this_week_flag = TRUE
         AND COALESCE(operationally_safe_to_use, FALSE) = FALSE
          THEN 1
        ELSE 0
      END
    ) AS pending_platform_count,


    MAX(
      CASE
        WHEN is_core_platform = TRUE
         AND expected_this_week_flag = TRUE
          THEN expected_ready_at_et
      END
    ) AS latest_core_expected_ready_at_et,


    MAX(
      CASE
        WHEN is_core_platform = TRUE
         AND expected_this_week_flag = TRUE
         AND backfill_risk = 'HIGH'
         AND operationally_safe_to_use = TRUE
          THEN 1
        ELSE 0
      END
    ) AS ready_high_backfill_risk_flag,


    CONCAT_WS(
      ', ',
      SORT_ARRAY(
        COLLECT_SET(
          CASE
            WHEN is_core_platform = TRUE
             AND expected_this_week_flag = TRUE
             AND operationally_safe_to_use = TRUE
              THEN platform
          END
        )
      )
    ) AS ready_platforms,


    CONCAT_WS(
      ', ',
      SORT_ARRAY(
        COLLECT_SET(
          CASE
            WHEN is_core_platform = TRUE
             AND expected_this_week_flag = TRUE
             AND COALESCE(operationally_safe_to_use, FALSE) = FALSE
              THEN platform
          END
        )
      )
    ) AS pending_platforms,


    CONCAT_WS(
      ', ',
      SORT_ARRAY(
        COLLECT_SET(
          CASE
            WHEN expected_this_week_flag = FALSE
              THEN platform
          END
        )
      )
    ) AS inactive_or_not_expected_platforms,


    CONCAT_WS(
      ', ',
      SORT_ARRAY(
        COLLECT_SET(
          CASE
            WHEN is_core_platform = FALSE
             AND expected_this_week_flag = TRUE
             AND sla_confidence = 'UNVALIDATED'
              THEN platform
          END
        )
      )
    ) AS active_unvalidated_platforms


  FROM PlatformStatus


  GROUP BY

    run_date,
    as_of_timestamp_et,
    prior_week_start,
    prior_week_end,
    channel_group

),


/* ===============================================================================================
   CHANNEL SUMMARY
   =============================================================================================== */

ChannelSummary AS (

  SELECT

    *,

    CASE


      WHEN required_platform_count = 0

        THEN 'NO ACTIVE CORE PLATFORMS'


      WHEN ready_platform_count = required_platform_count
       AND ready_high_backfill_risk_flag = 1

        THEN 'READY - BACKFILL RISK'


      WHEN ready_platform_count = required_platform_count

        THEN 'READY'


      WHEN as_of_timestamp_et < latest_core_expected_ready_at_et

        THEN 'IN PROGRESS'


      ELSE 'NOT READY'

    END AS channel_readiness_status,


    CASE

      WHEN required_platform_count > 0
       AND ready_platform_count = required_platform_count

        THEN TRUE

      ELSE FALSE

    END AS channel_operationally_safe_to_use,


    CASE channel_group

      WHEN 'Paid Search'
        THEN 'Monday 8 AM ET'

      WHEN 'Programmatic'
        THEN 'Monday 10 AM ET'

      WHEN 'Paid Social'
        THEN 'Tuesday 10 AM ET through Thursday 6 AM ET, platform-dependent'

    END AS channel_expected_ready_by

  FROM ChannelAgg

),


/* ===============================================================================================
   PLATFORM OUTPUT ROWS
   =============================================================================================== */

PlatformOutput AS (

  SELECT

    'PLATFORM'                                    AS record_type,

    run_date,
    as_of_timestamp_et,

    prior_week_start,
    prior_week_end,

    'POSTPAID + BROADBAND'                        AS lob_scope,

    channel_group,
    platform,

    source_table,

    is_core_platform,
    expected_this_week_flag,

    recent_active_weeks,

    days_present,
    expected_days,

    postpaid_days_present,
    broadband_days_present,

    date_coverage_pct,

    first_data_date,
    latest_data_date,

    week_spend,
    postpaid_week_spend,
    broadband_week_spend,

    expected_ready_by,
    expected_ready_at_et,

    safe_window_reached_flag,

    sla_confidence,
    backfill_risk,

    readiness_status,

    operationally_safe_to_use,

    CAST(NULL AS BIGINT)                          AS required_platform_count,
    CAST(NULL AS BIGINT)                          AS ready_platform_count,
    CAST(NULL AS BIGINT)                          AS pending_platform_count,

    CAST(NULL AS STRING)                          AS ready_platforms,
    CAST(NULL AS STRING)                          AS pending_platforms,
    CAST(NULL AS STRING)                          AS inactive_or_not_expected_platforms,


    CASE

      WHEN readiness_status = 'READY'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': ready. ',
          CAST(days_present AS STRING),
          '/7 prior-week days are present; expected safe window is ',
          expected_ready_by,
          '.'
        )


      WHEN readiness_status = 'READY - BACKFILL RISK'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': operationally ready, but later backfill/revisions remain possible. ',
          CAST(days_present AS STRING),
          '/7 prior-week days are present.'
        )


      WHEN readiness_status = 'COMPLETE - BEFORE SAFE WINDOW'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': 7/7 days are present, but the conservative safe reporting window (',
          expected_ready_by,
          ') has not yet been reached.'
        )


      WHEN readiness_status = 'NOT READY'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': not ready. Only ',
          CAST(days_present AS STRING),
          '/7 prior-week days are currently present.'
        )


      WHEN readiness_status = 'NO DATA - CHECK'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': no prior-week data found although the platform was active recently. Check source refresh.'
        )


      WHEN readiness_status = 'CHECK - SLA NOT VALIDATED'

        THEN CONCAT(
          channel_group,
          ' / ',
          platform,
          ': data is visible, but the platform readiness SLA has not yet been validated.'
        )


      ELSE CONCAT(
        channel_group,
        ' / ',
        platform,
        ': no recent activity detected; currently not expected to block channel readiness.'
      )

    END                                           AS dashboard_comment,


    sla_note


  FROM PlatformStatus

),


/* ===============================================================================================
   CHANNEL OUTPUT ROWS

   Same view, record_type = CHANNEL.
   =============================================================================================== */

ChannelOutput AS (

  SELECT

    'CHANNEL'                                     AS record_type,

    run_date,
    as_of_timestamp_et,

    prior_week_start,
    prior_week_end,

    'POSTPAID + BROADBAND'                        AS lob_scope,

    channel_group,

    'ALL PLATFORMS'                               AS platform,

    CASE channel_group

      WHEN 'Programmatic'
        THEN 'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr'

      WHEN 'Paid Search'
        THEN 'prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily'

      WHEN 'Paid Social'
        THEN 'prdrzranalytics.lab42.media_analytics_integrated_snapshot'

    END                                           AS source_table,


    CAST(NULL AS BOOLEAN)                         AS is_core_platform,

    CAST(NULL AS BOOLEAN)                         AS expected_this_week_flag,

    CAST(NULL AS BIGINT)                          AS recent_active_weeks,

    CAST(NULL AS BIGINT)                          AS days_present,

    CAST(7 AS INT)                                AS expected_days,

    CAST(NULL AS BIGINT)                          AS postpaid_days_present,
    CAST(NULL AS BIGINT)                          AS broadband_days_present,

    CAST(NULL AS DOUBLE)                          AS date_coverage_pct,

    CAST(NULL AS DATE)                            AS first_data_date,
    CAST(NULL AS DATE)                            AS latest_data_date,

    CAST(NULL AS DOUBLE)                          AS week_spend,
    CAST(NULL AS DOUBLE)                          AS postpaid_week_spend,
    CAST(NULL AS DOUBLE)                          AS broadband_week_spend,

    channel_expected_ready_by                     AS expected_ready_by,

    latest_core_expected_ready_at_et              AS expected_ready_at_et,


    CASE

      WHEN as_of_timestamp_et >= latest_core_expected_ready_at_et
        THEN TRUE

      ELSE FALSE

    END                                           AS safe_window_reached_flag,


    CAST(NULL AS STRING)                          AS sla_confidence,


    CASE

      WHEN channel_readiness_status = 'READY - BACKFILL RISK'
        THEN 'HIGH'

      ELSE 'CHANNEL DEPENDENT'

    END                                           AS backfill_risk,


    channel_readiness_status                      AS readiness_status,

    channel_operationally_safe_to_use             AS operationally_safe_to_use,

    required_platform_count,
    ready_platform_count,
    pending_platform_count,

    ready_platforms,
    pending_platforms,
    inactive_or_not_expected_platforms,


    CASE


      /* -----------------------------------------------------------------------------------------
         ALL CORE PLATFORMS READY
         ----------------------------------------------------------------------------------------- */

      WHEN required_platform_count > 0
       AND ready_platform_count = required_platform_count
       AND channel_group = 'Paid Social'

        THEN CONCAT(
          'Paid Social: ',
          CAST(ready_platform_count AS STRING),
          '/',
          CAST(required_platform_count AS STRING),
          ' active core platforms are operationally ready. ',
          CASE
            WHEN ready_platforms <> ''
              THEN CONCAT(
                'Ready: ',
                ready_platforms,
                '. '
              )
            ELSE ''
          END,
          'Meta may still receive later historical backfill/revisions.'
        )


      WHEN required_platform_count > 0
       AND ready_platform_count = required_platform_count

        THEN CONCAT(
          channel_group,
          ': ',
          CAST(ready_platform_count AS STRING),
          '/',
          CAST(required_platform_count AS STRING),
          ' active core platforms are ready. ',
          'Expected safe reporting window: ',
          channel_expected_ready_by,
          '.'
        )


      /* -----------------------------------------------------------------------------------------
         SOME CORE PLATFORMS PENDING
         ----------------------------------------------------------------------------------------- */

      WHEN required_platform_count > 0

        THEN CONCAT(
          channel_group,
          ': ',
          CAST(ready_platform_count AS STRING),
          '/',
          CAST(required_platform_count AS STRING),
          ' active core platforms are ready. ',
          CASE
            WHEN ready_platforms <> ''
              THEN CONCAT(
                'Ready: ',
                ready_platforms,
                '. '
              )
            ELSE ''
          END,
          CASE
            WHEN pending_platforms <> ''
              THEN CONCAT(
                'Pending/check: ',
                pending_platforms,
                '. '
              )
            ELSE ''
          END,
          'Expected safe reporting window: ',
          channel_expected_ready_by,
          '.',
          CASE
            WHEN channel_group = 'Paid Social'
              THEN ' Meta can receive later historical backfill.'
            ELSE ''
          END
        )


      ELSE CONCAT(
        channel_group,
        ': no active core platforms were detected for the prior week or recent activity window.'
      )

    END                                           AS dashboard_comment,


    CASE

      WHEN channel_group = 'Programmatic'
        THEN 'Programmatic safe window was validated at overall channel level; DSP rows use the same operational cutoff.'

      WHEN channel_group = 'Paid Search'
        THEN 'Paid Search historically shows very little movement after Monday morning.'

      WHEN channel_group = 'Paid Social'
        THEN 'Paid Social readiness varies by platform; Meta and X require the most conservative treatment.'

    END                                           AS sla_note


  FROM ChannelSummary

)


/* ===============================================================================================
   SINGLE VIEW OUTPUT
   =============================================================================================== */

SELECT *
FROM PlatformOutput


UNION ALL


SELECT *
FROM ChannelOutput
;