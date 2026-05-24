/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_silver_profound_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_profound_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly

PURPOSE:
  Silver view for Profound NON-BRANDED AI visibility data.
  Applies BYOD tag filter, asset name filter, brand type label,
  and week-end Saturday conversion on top of the Bronze view.
  Output is one clean row per week + asset_name ready for Gold unpivoting.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat + asset_name

FILTERS APPLIED:
  - tag = 'BYOD'
  - asset_name IN ('T-Mobile', 'Verizon', 'AT&T')

BUSINESS LOGIC APPLIED:
  - brand_type = 'NONBRAND' for all rows (entire source is non-branded by definition)
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)

KEY MODELING NOTES:
  - No aggregation needed — source is already weekly at asset + tag grain
  - asset_name preserved as-is for Gold metric name construction:
      T-Mobile → tmo_nonbrand_visibility_score
      Verizon  → verizon_nonbrand_visibility_score
      AT&T     → att_nonbrand_visibility_score
  - visibility_score is the primary KPI for this source
  - executions, mentions_count, share_of_voice retained for completeness

DOWNSTREAM:
  Gold : vw_sdi_pulseByod_gold_unified_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
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
        -- Entire Profound source is NON-BRAND by definition (non-branded AI queries)
        'NONBRAND'                                  AS brand_type,

        -- Metrics
        visibility_score,
        executions,
        mentions_count,
        share_of_voice

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly`

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