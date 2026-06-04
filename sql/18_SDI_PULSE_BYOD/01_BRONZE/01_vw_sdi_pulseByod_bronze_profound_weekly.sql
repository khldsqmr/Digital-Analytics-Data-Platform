/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_bronze_profound_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_bronze_profound_weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profoundVis_tag_weekly_sunday_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profoundCit_tag_weekly_sunday_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly

PURPOSE:
  Source-close Bronze view for Profound NON-BRANDED AI visibility data.
  Combines two Profound non-brand sources via UNION ALL:

    1. VIS (sdi_seo_profoundVis_tag_weekly_sunday_tmo)
       Tracks how often brand/competitor NAMES are mentioned in AI-generated
       answers (ChatGPT, Perplexity, Gemini etc.) for NON-BRANDED queries
       such as "best BYOD plan" or "bring your own phone carrier".
       Identified by: source_type = 'VIS', asset_name populated, root_domain NULL.

    2. CIT (sdi_seo_profoundCit_tag_weekly_sunday_tmo)
       Tracks how often brand/competitor DOMAINS are cited as sources in
       AI-generated answers for NON-BRANDED queries.
       Identified by: source_type = 'CIT', root_domain populated, asset_name NULL.

  This source covers T-Mobile, Verizon, AT&T and many other asset/domain names.
  Asset filtering is applied in Silver.
  Deduplicates each weekly snapshot and preserves all raw fields as-is.

BUSINESS GRAIN:
  One row per:
    account_id + asset_id + date_yyyymmdd + tag + source_type

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

KEY MODELING NOTES:
  - asset_name  : populated for VIS rows; NULL for CIT rows
  - root_domain : populated for CIT rows; NULL for VIS rows
  - source_type : 'VIS' or 'CIT' — used to distinguish the two sources downstream
  - asset_id    : present in VIS; set to CAST(NULL AS STRING) for CIT rows
                  (CIT source does not carry asset_id)
  - All tag values preserved — no tag filtering applied here (pushed to Silver)
  - date_yyyymmdd reflects the Sunday start of the ISO week as supplied by source
  - event_date_sun is the parsed DATE version of date_yyyymmdd for downstream arithmetic
  - Week-end Saturday conversion (week_sun_to_sat) is applied in Silver:
      DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - No brand/nonbrand classification applied here — entire source is NON-BRAND
    by definition; classification label is applied in Silver for consistency
  - No BYOD tag filtering applied here — pushed to Silver
  - visibility_score : proportion of AI executions that mention this asset (VIS only)
  - share_of_voice   : asset/domain mentions / total mentions across all assets for this tag
  - executions       : total AI queries executed for this tag this week (VIS only; NULL for CIT)
  - mentions_count   : number of times this asset was mentioned across all executions (VIS only; NULL for CIT)

DOWNSTREAM:
  Silver : vw_sdi_pulseByod_silver_profound_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly`
AS

WITH ranked AS (

    -- -----------------------------------------------------------------------
    -- LEG 1: VIS — brand name visibility in AI-generated answers
    -- asset_name populated; root_domain NULL
    -- -----------------------------------------------------------------------
    SELECT
        -- Primary keys
        SAFE_CAST(raw.account_id   AS STRING)                           AS account_id,
        SAFE_CAST(raw.account_name AS STRING)                           AS account_name,
        SAFE_CAST(raw.asset_id     AS STRING)                           AS asset_id,
        SAFE_CAST(raw.asset_name   AS STRING)                           AS asset_name,
        CAST(NULL AS STRING)                                            AS root_domain,

        -- Source type identifier
        'VIS'                                                           AS source_type,

        -- Date fields
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date_sun,

        -- Segmentation
        SAFE_CAST(raw.tag AS STRING)                                    AS tag,

        -- Metrics
        SAFE_CAST(raw.executions       AS FLOAT64)                      AS executions,
        SAFE_CAST(raw.mentions_count   AS FLOAT64)                      AS mentions_count,
        SAFE_CAST(raw.share_of_voice   AS FLOAT64)                      AS share_of_voice,
        SAFE_CAST(raw.visibility_score AS FLOAT64)                      AS visibility_score,

        -- Audit fields
        SAFE_CAST(raw.__insert_date AS INT64)                           AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per account_id + asset_id + asset_name + date_yyyymmdd + tag + source_type
        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id  AS STRING),
                SAFE_CAST(raw.asset_id    AS STRING),
                SAFE_CAST(raw.asset_name  AS STRING),
                CAST(raw.date_yyyymmdd    AS STRING),
                SAFE_CAST(raw.tag         AS STRING),
                'VIS'
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)     DESC,
                raw.Filename                          DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profoundVis_tag_weekly_sunday_tmo` raw

    WHERE raw.account_id    IS NOT NULL
      AND raw.asset_id      IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
      AND raw.tag           IS NOT NULL

    UNION ALL

    -- -----------------------------------------------------------------------
    -- LEG 2: CIT — domain citation share of voice in AI-generated answers
    -- root_domain populated; asset_name and asset_id NULL
    -- -----------------------------------------------------------------------
    SELECT
        -- Primary keys
        SAFE_CAST(raw.account_id   AS STRING)                           AS account_id,
        SAFE_CAST(raw.account_name AS STRING)                           AS account_name,
        CAST(NULL AS STRING)                                            AS asset_id,
        CAST(NULL AS STRING)                                            AS asset_name,
        SAFE_CAST(raw.root_domain  AS STRING)                           AS root_domain,

        -- Source type identifier
        'CIT'                                                           AS source_type,

        -- Date fields
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date_sun,

        -- Segmentation
        SAFE_CAST(raw.tag AS STRING)                                    AS tag,

        -- Metrics
        -- executions, mentions_count, visibility_score not available in CIT source
        CAST(NULL AS FLOAT64)                                           AS executions,
        CAST(NULL AS FLOAT64)                                           AS mentions_count,
        SAFE_CAST(raw.share_of_voice AS FLOAT64)                        AS share_of_voice,
        CAST(NULL AS FLOAT64)                                           AS visibility_score,

        -- Audit fields
        SAFE_CAST(raw.__insert_date AS INT64)                           AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per account_id + root_domain + date_yyyymmdd + tag + source_type
        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id  AS STRING),
                SAFE_CAST(raw.root_domain AS STRING),
                CAST(raw.date_yyyymmdd    AS STRING),
                SAFE_CAST(raw.tag         AS STRING),
                'CIT'
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)     DESC,
                raw.Filename                          DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profoundCit_tag_weekly_sunday_tmo` raw

    WHERE raw.account_id    IS NOT NULL
      AND raw.root_domain   IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
      AND raw.tag           IS NOT NULL
)

SELECT
    account_id,
    account_name,
    asset_id,
    asset_name,
    root_domain,
    source_type,
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