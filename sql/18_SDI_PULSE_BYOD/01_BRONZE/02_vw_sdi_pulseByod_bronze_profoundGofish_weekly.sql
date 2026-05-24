/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_bronze_profoundGofish_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_bronze_profoundGofish_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_weekly_sunday_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly

PURPOSE:
  Source-close Bronze view for Profound GoFish BRANDED AI visibility data.
  Profound GoFish tracks how often brands/competitors are mentioned in AI-generated
  answers when users ask BRANDED queries that explicitly include a brand name,
  such as "does T-Mobile have a BYOD program" or "T-Mobile bring your own phone deal".
  This source covers 60+ asset names including T-Mobile, Verizon, AT&T and many others.
  Asset filtering to T-Mobile, Verizon, AT&T is applied in Silver.
  Structurally identical to the Profound Bronze view — different source table only.
  Deduplicates the weekly snapshot and preserves all raw fields as-is.

BUSINESS GRAIN:
  One row per:
    account_id + asset_id + date_yyyymmdd + tag

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

KEY MODELING NOTES:
  - All asset_name values preserved — no asset filtering applied here (pushed to Silver)
  - All tag values preserved — no tag filtering applied here (pushed to Silver)
  - date_yyyymmdd reflects the Sunday start of the ISO week as supplied by source
  - event_date_sun is the parsed DATE version of date_yyyymmdd for downstream arithmetic
  - Week-end Saturday conversion (week_sun_to_sat) is applied in Silver:
      DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - No brand/nonbrand classification applied here — entire source is BRAND
    by definition; classification label is applied in Silver for consistency
  - No BYOD tag filtering applied here — pushed to Silver
  - visibility_score : proportion of AI executions that mention this asset for the tag
  - share_of_voice   : asset mentions / total mentions across all assets for this tag
  - executions       : total AI queries executed for this tag this week
  - mentions_count   : number of times this asset was mentioned across all executions

DOWNSTREAM:
  Silver : vw_sdi_pulseByod_silver_profoundGofish_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly`
AS

WITH ranked AS (
    SELECT
        -- Primary keys
        SAFE_CAST(raw.account_id   AS STRING)    AS account_id,
        SAFE_CAST(raw.account_name AS STRING)    AS account_name,
        SAFE_CAST(raw.asset_id     AS STRING)    AS asset_id,
        SAFE_CAST(raw.asset_name   AS STRING)    AS asset_name,

        -- Date fields
        -- date_yyyymmdd : raw string date key in YYYYMMDD format (Sunday week start)
        -- event_date_sun: parsed DATE type for downstream arithmetic and Silver week-end conversion
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date_sun,

        -- Segmentation
        -- tag: Profound topic/category tag (e.g. 'BYOD', 'LOB - Postpaid')
        -- filtered to tag = 'BYOD' in Silver
        SAFE_CAST(raw.tag AS STRING)                                    AS tag,

        -- Metrics
        -- executions    : total AI queries run for this tag this week
        -- mentions_count: times this asset was mentioned across all executions
        -- share_of_voice: asset mentions / total mentions across all assets for this tag
        -- visibility_score: primary KPI — proportion of executions mentioning this asset
        SAFE_CAST(raw.executions       AS FLOAT64)                      AS executions,
        SAFE_CAST(raw.mentions_count   AS FLOAT64)                      AS mentions_count,
        SAFE_CAST(raw.share_of_voice   AS FLOAT64)                      AS share_of_voice,
        SAFE_CAST(raw.visibility_score AS FLOAT64)                      AS visibility_score,

        -- Audit fields (preserved for data lineage and dedup ordering)
        SAFE_CAST(raw.__insert_date AS INT64)                           AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per account_id + asset_id + date_yyyymmdd + tag
        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id AS STRING),
                SAFE_CAST(raw.asset_id   AS STRING),
                CAST(raw.date_yyyymmdd   AS STRING),
                SAFE_CAST(raw.tag        AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)     DESC,
                raw.Filename                          DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_weekly_sunday_tmo` raw

    -- Exclude rows missing primary key fields to prevent dedup grain pollution
    WHERE raw.account_id    IS NOT NULL
      AND raw.asset_id      IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
      AND raw.tag           IS NOT NULL
)

SELECT
    account_id,
    account_name,
    asset_id,
    asset_name,
    date_yyyymmdd,
    event_date_sun,
    tag,
    executions,
    mentions_count,
    share_of_voice,
    visibility_score,
    insert_date,
    file_load_datetime,
    filename
FROM ranked
WHERE rn = 1
;