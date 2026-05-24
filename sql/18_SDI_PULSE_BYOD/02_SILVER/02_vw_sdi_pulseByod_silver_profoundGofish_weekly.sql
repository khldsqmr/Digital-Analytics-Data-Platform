/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_silver_profoundGofish_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_profoundGofish_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly

PURPOSE:
  Silver view for Profound GoFish BRANDED AI visibility data.
  Applies BYOD tag filter, asset name filter, brand type label,
  and week-end Saturday conversion on top of the Bronze view.
  Output is one clean row per week + asset_name ready for Gold unpivoting.
  Structurally identical to the Profound Silver view — brand_type differs only.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat + asset_name

FILTERS APPLIED:
  - tag = 'BYOD'
  - asset_name IN ('T-Mobile', 'Verizon', 'AT&T')

BUSINESS LOGIC APPLIED:
  - brand_type = 'BRAND' for all rows (entire source is branded by definition)
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)

KEY MODELING NOTES:
  - No aggregation needed — source is already weekly at asset + tag grain
  - asset_name preserved as-is for Gold metric name construction:
      T-Mobile → tmo_brand_visibility_score
      Verizon  → verizon_brand_visibility_score
      AT&T     → att_brand_visibility_score
  - visibility_score is the primary KPI for this source
  - executions, mentions_count, share_of_voice retained for completeness

DOWNSTREAM:
  Gold : vw_sdi_pulseByod_gold_unified_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
AS

WITH filtered AS (
    SELECT
        -- Week-end Saturday conversion
        -- Source provides Sunday week start; add 6 days to get Saturday week end
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,

        -- Asset dimension
        -- Filtered to T-Mobile, Verizon, AT&T only
        asset_name,

        -- Brand type
        -- Entire Profound GoFish source is BRAND by definition (branded AI queries)
        'BRAND'                                     AS brand_type,

        -- Metrics
        visibility_score,
        executions,
        mentions_count,
        share_of_voice

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly`

    -- BYOD topic filter
    WHERE tag = 'BYOD'
      -- Asset filter: T-Mobile, Verizon, AT&T only
      AND asset_name IN ('T-Mobile', 'Verizon', 'AT&T')
)

SELECT
    week_sun_to_sat,
    asset_name,
    brand_type,
    visibility_score,
    executions,
    mentions_count,
    share_of_voice
FROM filtered
ORDER BY week_sun_to_sat ASC, asset_name ASC
;