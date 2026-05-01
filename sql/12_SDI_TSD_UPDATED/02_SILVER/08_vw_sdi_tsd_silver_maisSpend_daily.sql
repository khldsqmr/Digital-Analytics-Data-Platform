/* =================================================================================================
FILE: 08_vw_sdi_tsd_silver_maisSpend_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_maisSpend_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_maisSpend_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_maisSpend_daily

PURPOSE:
  Canonical Silver spend daily source mart for the Total Search Dashboard sourced
  from media_analytics_integrated_summary. Paid Search sub-channels are mapped up
  to PAID SEARCH channel. Sub-channel breakdown preserved as separate columns.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

LOB STATUS:
  POSTPAID  -- active
  BROADBAND -- uncomment in filtered CTE when ready
  PREPAID   -- uncomment in filtered CTE when ready
  TFB       -- uncomment in filtered CTE when ready

OUTPUT METRICS:
  - mais_platform_spend
      NULL for PAID SEARCH — developer aggregates sub-channels to get total
      Populated for all other channels as total spend

  - mais_platform_spend_branded
      PAID SEARCH only — NULL for all other channels

  - mais_platform_spend_nonbranded
      PAID SEARCH only — NULL for all other channels

  - mais_platform_spend_pla
      PAID SEARCH only — NULL for all other channels

  - mais_platform_spend_pmax
      PAID SEARCH only — NULL for all other channels

KEY MODELING NOTES:
  - Paid Search sub-channels (PAID SEARCH BRANDED, NON-BRANDED, PLAS, PERFORMANCE MAX)
    are all mapped to channel = PAID SEARCH so they align with SA360/Adobe/GSC
  - mais_platform_spend is NULL for PAID SEARCH — use sub-channel columns instead
  - Sub-channels sum to Paid Search total: branded + nonbranded + pla + pmax = total
  - Does not affect existing platform_spend from vw_sdi_tsd_silver_platformSpend_daily
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_maisSpend_daily`
AS
WITH filtered AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_maisSpend_daily`
    WHERE lob IN (
        'POSTPAID'
        -- ,'BROADBAND'  -- uncomment when ready
        -- ,'PREPAID'    -- uncomment when ready
        -- ,'TFB'        -- uncomment when ready
    )
),

mapped AS (
    SELECT
        event_date,
        lob,
        CASE
            WHEN channel IN (
                'PAID SEARCH BRANDED',
                'PAID SEARCH NON-BRANDED',
                'PAID SEARCH PLAS',
                'PAID SEARCH PERFORMANCE MAX'
            ) THEN 'PAID SEARCH'
            ELSE channel
        END                     AS channel,
        channel                 AS channel_raw,
        mais_platform_spend
    FROM filtered
)

SELECT
    event_date,
    lob,
    channel,

    -- Non-Paid Search channels: total spend populated
    -- PAID SEARCH: NULL — developer aggregates sub-channel columns to get total
    SUM(CASE WHEN channel != 'PAID SEARCH'
             THEN mais_platform_spend END)                  AS mais_platform_spend,

    -- Paid Search sub-channel breakdown
    -- NULL for all non-Paid Search channels
    -- Developer sums these 4 to get total Paid Search spend
    SUM(CASE WHEN channel_raw = 'PAID SEARCH BRANDED'
             THEN mais_platform_spend END)                  AS mais_platform_spend_branded,
    SUM(CASE WHEN channel_raw = 'PAID SEARCH NON-BRANDED'
             THEN mais_platform_spend END)                  AS mais_platform_spend_nonbranded,
    SUM(CASE WHEN channel_raw = 'PAID SEARCH PLAS'
             THEN mais_platform_spend END)                  AS mais_platform_spend_pla,
    SUM(CASE WHEN channel_raw = 'PAID SEARCH PERFORMANCE MAX'
             THEN mais_platform_spend END)                  AS mais_platform_spend_pmax

FROM mapped
GROUP BY 1, 2, 3;