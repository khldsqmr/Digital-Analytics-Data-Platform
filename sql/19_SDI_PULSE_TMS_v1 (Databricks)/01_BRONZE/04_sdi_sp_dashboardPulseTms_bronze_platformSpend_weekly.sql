/* =================================================================================================
FILE:         04_sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly.
  Aggregates daily platform (paid media) spend to weekly grain, scoped to Postpaid and
  Broadband/HSI LOBs only, with spend rolled up into PulseTMS's standard channel_group
  vocabulary. Actuals only -- this source has no forecast column.

  REPLACES an earlier placeholder version of this file that referenced an unconfirmed table
  (agg_day_media_and_outcomes). This version is built off the source, schema, and live data
  actually confirmed this session -- see below.

SOURCE (confirmed via DESCRIBE TABLE + live query, this session):
  prdrzranalytics.lab42.media_analytics_integrated_snapshot
  Columns used: Date, Week_Start_Mon, Week_End_Sun, LOB, Channel_Group_Name, Spend.
  Week_Start_Mon/Week_End_Sun are Monday-Sunday (media buying week) -- NOT the Sunday-Saturday
  convention this pipeline uses everywhere else, so week_sun_sat below is derived from the
  daily Date column instead of trusting either of those two columns directly.

WEEK CONVERSION (Monday-Sunday raw -> Sunday-Saturday PulseTMS convention):
  date_add(Date, 7 - EXTRACT(DAYOFWEEK FROM Date))
  Same roll-forward-to-Saturday logic used elsewhere in this pipeline. Databricks DAYOFWEEK is
  Sunday=1..Saturday=7 (identical to BigQuery), so for a Tuesday (3): 7-3=4 days forward lands
  on Saturday.

LOB SCOPE -- filtered, not just canonicalized (confirmed via live GROUP BY this session):
  Raw LOB values in this table are already spelled exactly 'POSTPAID' and 'BROADBAND' --
  unlike MFC Spend's source, which needed 'HSI' -> 'BROADBAND' recoding, this source needs no
  recoding, just a straight filter. Explicitly scoped to these two only -- PREPAID (~$1.19B),
  TFB (~$850M), and the small tail (Not Specified, Other, ASSURANCE, FINANCIAL SERVICES,
  T-VISION) are deliberately excluded, not folded into 'Other' -- this file targets Postpaid
  and Broadband/HSI specifically, not full LOB coverage.

CHANNEL GROUP MAPPING (confirmed via live GROUP BY on Channel_Group_Name this session --
all 11 raw values observed and classified, none left to an unverified default):
  Paid Search                          -> 'Paid Search'
  Paid Social                          -> 'Paid Social'
  Display, Online Video, Digital Audio -> 'Programmatic'
  National TV                          -> 'iSpot National TV'
  OTT                                  -> 'iSpot OTT'
  Affiliate                            -> 'Affiliate'
  UNKNOWN, Out of Home, Other          -> 'Other'
  National TV and OTT were kept as two separate groups rather than merged into one 'iSpot'
  bucket -- both are iSpot-sourced (per this table's own view_source column description: "OTT
  is from iSpot"), but National TV ($2.16B) and OTT ($1.22B) are different media types with
  very different spend scale, so collapsing them would hide that distinction.

DELIBERATE MISMATCH WITH ADOBE'S CHANNEL VOCABULARY -- not a bug:
  Adobe's own channel_group values (confirmed from its Bronze file) are 'Paid Search' |
  'Paid Social' | 'Organic Search' | 'Direct' | 'Programmatic' | 'Other' -- no Affiliate or
  iSpot equivalent, since Adobe only tracks attributed on-site actions, and Direct/Organic
  traffic has no associated paid spend to report. This file's vocabulary shares 4 of Adobe's 6
  groups and adds 'Affiliate'/'iSpot National TV'/'iSpot OTT' (spend channels Adobe doesn't
  track actions for), while omitting 'Direct'/'Organic Search' (action channels with no spend
  to report). A Gold-level spend-vs-actions view built on channel_group will show gaps on both
  sides of that overlap -- expected, not a join bug.

CARRIED FROM AN EARLIER INVESTIGATION, NOT RE-VERIFIED THIS SESSION:
  27.4M total rows vs. 25.7M distinct on (Date, Account_ID, Campaign_ID, Placement_ID,
  Creative_ID) -- a ~6.4% gap, assessed at the time as not a blocker for GROUP BY aggregation
  (the aggregation below sums Spend directly rather than depending on row-level uniqueness on
  that narrower column subset, so genuinely distinct spend records sharing those five columns
  -- e.g. differing only by Creative_Name or Publisher -- are not lost or double-counted by
  this file's own logic). Flagging as inherited context rather than something confirmed fresh
  today, consistent with how every other reused note in this pipeline has been flagged.

CALENDAR ALIGNMENT:
  This file only derives week_sun_sat. The join to sdi_vw_dashboardPulseTms_dim_qgp_calendar
  (quarter, week_type, days_in_period, BOUNDARY_STUB/BOUNDARY_FIRST, ISO week for YoY) happens
  in Silver, same as every other source in this pipeline -- that calendar view is PulseTMS's
  shared date backbone, not something QGP-specific despite its name.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly
  USING DELTA
  CLUSTER BY (week_sun_sat, lob, channel_group)
  COMMENT 'PulseTMS Bronze — Platform (paid media) spend aggregated to weekly grain, scoped to Postpaid and Broadband/HSI LOBs only. One row per week_sun_sat x lob x channel_group. channel_group values: Paid Search, Paid Social, Programmatic, iSpot National TV, iSpot OTT, Affiliate, Other. Actuals only — no forecast available from this source. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly.'
  AS
  WITH Mapped AS (
    SELECT
      -- Roll each calendar day forward to its week-ending Saturday (Sun=1..Sat=7)
      date_add(raw.Date, 7 - EXTRACT(DAYOFWEEK FROM raw.Date))              AS week_sun_sat,

      UPPER(TRIM(raw.LOB))                                                  AS lob,

      CASE TRIM(raw.Channel_Group_Name)
        WHEN 'Paid Search'   THEN 'Paid Search'
        WHEN 'Paid Social'   THEN 'Paid Social'
        WHEN 'Display'       THEN 'Programmatic'
        WHEN 'Online Video'  THEN 'Programmatic'
        WHEN 'Digital Audio' THEN 'Programmatic'
        WHEN 'National TV'   THEN 'iSpot National TV'
        WHEN 'OTT'           THEN 'iSpot OTT'
        WHEN 'Affiliate'     THEN 'Affiliate'
        WHEN 'UNKNOWN'       THEN 'Other'
        WHEN 'Out of Home'   THEN 'Other'
        WHEN 'Other'         THEN 'Other'
        ELSE 'Other'  -- defensive: any future/unseen raw value falls to Other rather than being silently dropped
      END                                                                   AS channel_group,

      TRY_CAST(raw.Spend AS DOUBLE)                                         AS spend

    FROM prdrzranalytics.lab42.media_analytics_integrated_snapshot raw
    WHERE UPPER(TRIM(raw.LOB)) IN ('POSTPAID', 'BROADBAND')
      AND raw.Channel_Group_Name IS NOT NULL
  )
  SELECT
    week_sun_sat,
    lob,
    channel_group,
    SUM(spend) AS spend
  FROM Mapped
  GROUP BY week_sun_sat, lob, channel_group;

END;