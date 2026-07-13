-- ============================================================
-- BRONZE 2: ACTUALS - GRANULAR — BigQuery
-- Converted from Databricks sdi_sp_mfc_bronze_spendActualsGranular_weekly
--
-- Logic:
--   Same as Bronze 1 but partition key expands to
--   (Quarter, QGP_Week, LOB, Channel, Tactic, Message_Type, Agency)
--   Agency normalization applied:
--     ini                                    → Initiative
--     in house/inhouse/internal/progact/search → In-House (TMO)
-- ============================================================
CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActualsGranular_weekly`()
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendActualsGranular_weekly`
  OPTIONS (
    description = 'MFC Bronze — refreshed via sdi_sp_mfc_bronze_spendActualsGranular_weekly.'
  )
  AS

  WITH raw AS (
    SELECT
      Quarter,
      CAST(Week_Beginning_Monday AS DATE) AS Week_Beginning_Monday,
      CAST(Week_Ending_Sunday    AS DATE) AS Week_Ending_Sunday,
      CAST(QGP_Week              AS DATE) AS QGP_Week,
      File_Date                           AS FileLoad_Date,
      UPPER(TRIM(LOB_Supported))          AS LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      CASE
        WHEN LOWER(TRIM(Agency)) = 'ini'
          THEN 'Initiative'
        WHEN LOWER(TRIM(Agency)) IN ('in house', 'inhouse', 'internal', 'progact', 'search')
          THEN 'In-House (TMO)'
        WHEN Agency IS NULL
          THEN NULL
        ELSE TRIM(Agency)
      END                                 AS Agency,
      CASE WHEN QGP = 'Actual' THEN Spend ELSE NULL END AS Spend_Actual
    FROM `prj-dbi-prd-1.ds_dbi_marketing.ma_mfc_raw`
    WHERE UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND')
      AND WM_NWM = 'Working'
      AND Channel IS NOT NULL
      AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Unallocated', 'Budget Held')
      AND Week_Beginning_Monday IS NOT NULL AND Week_Beginning_Monday != 'None'
      AND Week_Ending_Sunday    IS NOT NULL AND Week_Ending_Sunday    != 'None'
      AND QGP_Week              IS NOT NULL AND QGP_Week              != 'None'
      AND REGEXP_CONTAINS(Week_Beginning_Monday,    r'^\d{4}-\d{2}-\d{2}$')
      AND REGEXP_CONTAINS(Week_Ending_Sunday,       r'^\d{4}-\d{2}-\d{2}$')
      AND REGEXP_CONTAINS(CAST(QGP_Week AS STRING), r'^\d{4}-\d{2}-\d{2}$')
      AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      AND UPPER(TRIM(Message))      NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
      AND Quarter IS NOT NULL
      AND REGEXP_CONTAINS(Quarter, r"^Q[1-4]'[0-9]{2}$")
      AND QGP = 'Actual'
      AND Spend IS NOT NULL
  ),

  weekly_snapshots AS (
    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
      SUM(Spend_Actual) AS weekly_actual
    FROM raw
    WHERE CAST(Week_Beginning_Monday AS DATE) <= CAST(Week_Ending_Sunday AS DATE)
    GROUP BY
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week,
      FileLoad_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),

  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY Quarter, QGP_Week, LOB_Supported,
                     Channel, Tactic, Message_Type, Agency
        ORDER BY FileLoad_Date DESC
      ) AS rn
    FROM weekly_snapshots
  ),

  best AS (
    SELECT * FROM ranked WHERE rn = 1
  ),

  week_type AS (
    SELECT
      QGP_Week,
      CASE
        WHEN COUNT(DISTINCT Quarter) > 1 THEN 'boundary_week'
        ELSE 'normal'
      END AS week_type
    FROM best
    GROUP BY QGP_Week
  )

  SELECT
    b.Quarter,
    b.Week_Beginning_Monday,
    b.Week_Ending_Sunday,
    b.QGP_Week,
    b.FileLoad_Date,
    b.LOB_Supported,
    b.Channel,
    b.Tactic,
    b.Message_Type,
    b.Agency,
    b.weekly_actual,
    w.week_type
  FROM best b
  JOIN week_type w ON b.QGP_Week = w.QGP_Week
  WHERE b.weekly_actual IS NOT NULL
    AND b.weekly_actual != 0
  ;

END;