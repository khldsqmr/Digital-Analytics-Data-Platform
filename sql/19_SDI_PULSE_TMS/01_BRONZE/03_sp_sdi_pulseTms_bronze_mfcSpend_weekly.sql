/* =================================================================================================
FILE:         03_sp_sdi_pulseTms_bronze_mfcSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_bronze_mfcSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_bronze_mfcSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_mfcSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_mfcSpend_weekly`
  PARTITION BY qgp_week
  CLUSTER BY lob, channel_group
  OPTIONS (
    description = 'PulseTMS Bronze — MFC spend granular. One row per qgp_week x lob x channel_group x tactic x message_type x agency. Partitioned by qgp_week, clustered by lob and channel_group. Refreshed weekly via sp_sdi_pulseTms_bronze_mfcSpend_weekly.'
  )
  AS
  SELECT
    SAFE_CAST(raw.Quarter          AS STRING)                             AS quarter_raw,
    SAFE_CAST(raw.Quarter_End_Date AS DATE)                               AS quarter_end_date,
    SAFE_CAST(raw.Period_Start     AS DATE)                               AS period_start,
    SAFE_CAST(raw.Period_End       AS DATE)                               AS period_end,
    SAFE_CAST(raw.QGP_Week         AS DATE)                               AS qgp_week,
    SAFE_CAST(UPPER(TRIM(raw.week_type)) AS STRING)                       AS week_type,
    CASE UPPER(TRIM(raw.LOB_Supported))
      WHEN 'TBG' THEN 'TFB'
      ELSE UPPER(TRIM(raw.LOB_Supported))
    END                                                                   AS lob,
    CASE
      WHEN UPPER(TRIM(raw.Channel)) = 'PAID SEARCH'                       THEN 'Paid Search'
      WHEN UPPER(TRIM(raw.Channel)) = 'PAID SOCIAL'                       THEN 'Paid Social'
      WHEN UPPER(TRIM(raw.Channel)) IN ('DISPLAY', 'OLV', 'AUDIO')        THEN 'Programmatic'
      WHEN UPPER(TRIM(raw.Channel)) = 'OTT'
        AND UPPER(TRIM(raw.Tactic)) LIKE '%PROGRAMMATIC%'                 THEN 'Programmatic'
      WHEN UPPER(TRIM(raw.Channel)) = 'OOH'
        AND UPPER(TRIM(raw.Tactic)) LIKE '%PROGRAMMATIC%'                 THEN 'Programmatic'
      ELSE 'Other'
    END                                                                   AS channel_group,
    SAFE_CAST(UPPER(TRIM(raw.Channel))      AS STRING)                    AS channel,
    SAFE_CAST(UPPER(TRIM(raw.Tactic))       AS STRING)                    AS tactic,
    SAFE_CAST(UPPER(TRIM(raw.Message_Type)) AS STRING)                    AS message_type,
    SAFE_CAST(UPPER(TRIM(raw.Agency))       AS STRING)                    AS agency,
    SAFE_CAST(raw.spend_actual   AS FLOAT64)                              AS spend_actual,
    SAFE_CAST(raw.spend_forecast AS FLOAT64)                              AS spend_forecast,
    SAFE_CAST(raw.FileLoad_Date  AS DATE)                                 AS file_load_date
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` raw
  WHERE raw.Channel IS NOT NULL
    AND UPPER(TRIM(raw.Channel)) NOT IN (
      'OTHER (DO NOT USE)', 'NON-WORKING', 'UNALLOCATED', 'BUDGET HELD'
    )
    AND UPPER(TRIM(raw.Message_Type)) != 'MICRO'
    AND raw.spend_display IS NOT NULL
    AND raw.spend_display != 0;

END;