/* =================================================================================================
FILE: 11_vw_sdi_tsd_bronze_profoundVisCitTag_monthly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_profoundVisCitTag_monthly

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_monthly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_monthly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_cit_tag_monthly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_monthly

PURPOSE:
  Canonical Bronze monthly ProFound / GoFish source mart for the Total Search Dashboard.
  This view standardizes monthly visibility and citation tag-level data into one source-close object.

BUSINESS GRAIN:
  One row per:
      period_date
      lob
      channel
      company
      brand_type
      metric_source

BUSINESS RULES:
  - period_date comes from date_yyyymmdd
  - lob is derived from tag
  - channel is fixed as AI SEARCH
  - ProFound source = NONBRAND
  - GoFish source = BRAND
  - only Tag = 'LOB - Postpaid' is included
  - scores / percentages are preserved by canonical row selection, not SUM() / AVG()
  - company standardization:
      T-Mobile / t-mobile.com -> TMO
      AT&T / att.com          -> ATT
      Verizon / verizon.com   -> VERIZON

KEY DEDUPE FIX:
  - When multiple raw rows collapse into the same standardized business key
    (for example, 'AT&T' and 'At&t' both mapping to ATT),
    metadata alone may tie.
  - Visibility rows now use mentions_count / executions / visibility_score as deterministic tie-breakers.
  - Citation rows now use citation_count / citation_share as deterministic tie-breakers.

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_monthly`
AS

WITH vis_nonbrand_base AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS period_date,
        CASE
            WHEN UPPER(TRIM(tag)) = 'LOB - POSTPAID' THEN 'POSTPAID'
            ELSE NULL
        END AS lob,
        UPPER(TRIM('AI SEARCH')) AS channel,
        CASE
            WHEN UPPER(TRIM(asset_name)) = 'T-MOBILE' THEN 'TMO'
            WHEN UPPER(TRIM(asset_name)) = 'AT&T' THEN 'ATT'
            WHEN UPPER(TRIM(asset_name)) = 'VERIZON' THEN 'VERIZON'
            ELSE NULL
        END AS company,
        'NONBRAND' AS brand_type,
        'VISIBILITY' AS metric_source,

        SAFE_CAST(executions AS FLOAT64) AS executions,
        SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS visibility_share_of_voice,
        SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,
        CAST(NULL AS FLOAT64) AS citation_count,
        CAST(NULL AS FLOAT64) AS citation_share,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        TIMESTAMP(File_Load_datetime) AS file_load_datetime,
        Filename AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_monthly_tmo`
    WHERE UPPER(TRIM(tag)) = 'LOB - POSTPAID'
      AND UPPER(TRIM(asset_name)) IN ('T-MOBILE', 'AT&T', 'VERIZON')
),

vis_nonbrand AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY period_date, lob, channel, company, brand_type, metric_source
                ORDER BY
                    file_load_datetime DESC,
                    filename DESC,
                    insert_date DESC,
                    mentions_count DESC,
                    executions DESC,
                    visibility_score DESC
            ) AS rn
        FROM vis_nonbrand_base
        WHERE lob IS NOT NULL
          AND company IS NOT NULL
    )
    WHERE rn = 1
),

cit_nonbrand_base AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS period_date,
        CASE
            WHEN UPPER(TRIM(tag)) = 'LOB - POSTPAID' THEN 'POSTPAID'
            ELSE NULL
        END AS lob,
        UPPER(TRIM('AI SEARCH')) AS channel,
        CASE
            WHEN LOWER(TRIM(root_domain)) = 't-mobile.com' THEN 'TMO'
            WHEN LOWER(TRIM(root_domain)) = 'att.com' THEN 'ATT'
            WHEN LOWER(TRIM(root_domain)) = 'verizon.com' THEN 'VERIZON'
            ELSE NULL
        END AS company,
        'NONBRAND' AS brand_type,
        'CITATION' AS metric_source,

        CAST(NULL AS FLOAT64) AS executions,
        CAST(NULL AS FLOAT64) AS mentions_count,
        CAST(NULL AS FLOAT64) AS visibility_share_of_voice,
        CAST(NULL AS FLOAT64) AS visibility_score,
        SAFE_CAST(count AS FLOAT64) AS citation_count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS citation_share,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        TIMESTAMP(File_Load_datetime) AS file_load_datetime,
        Filename AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo`
    WHERE UPPER(TRIM(tag)) = 'LOB - POSTPAID'
      AND LOWER(TRIM(root_domain)) IN ('t-mobile.com', 'att.com', 'verizon.com')
),

cit_nonbrand AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY period_date, lob, channel, company, brand_type, metric_source
                ORDER BY
                    file_load_datetime DESC,
                    filename DESC,
                    insert_date DESC,
                    citation_count DESC,
                    citation_share DESC
            ) AS rn
        FROM cit_nonbrand_base
        WHERE lob IS NOT NULL
          AND company IS NOT NULL
    )
    WHERE rn = 1
),

vis_brand_base AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS period_date,
        CASE
            WHEN UPPER(TRIM(tag)) = 'LOB - POSTPAID' THEN 'POSTPAID'
            ELSE NULL
        END AS lob,
        UPPER(TRIM('AI SEARCH')) AS channel,
        CASE
            WHEN UPPER(TRIM(asset_name)) = 'T-MOBILE' THEN 'TMO'
            WHEN UPPER(TRIM(asset_name)) = 'AT&T' THEN 'ATT'
            WHEN UPPER(TRIM(asset_name)) = 'VERIZON' THEN 'VERIZON'
            ELSE NULL
        END AS company,
        'BRAND' AS brand_type,
        'VISIBILITY' AS metric_source,

        SAFE_CAST(executions AS FLOAT64) AS executions,
        SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS visibility_share_of_voice,
        SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,
        CAST(NULL AS FLOAT64) AS citation_count,
        CAST(NULL AS FLOAT64) AS citation_share,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        TIMESTAMP(File_Load_datetime) AS file_load_datetime,
        Filename AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_monthly_tmo`
    WHERE UPPER(TRIM(tag)) = 'LOB - POSTPAID'
      AND UPPER(TRIM(asset_name)) IN ('T-MOBILE', 'AT&T', 'VERIZON')
),

vis_brand AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY period_date, lob, channel, company, brand_type, metric_source
                ORDER BY
                    file_load_datetime DESC,
                    filename DESC,
                    insert_date DESC,
                    mentions_count DESC,
                    executions DESC,
                    visibility_score DESC
            ) AS rn
        FROM vis_brand_base
        WHERE lob IS NOT NULL
          AND company IS NOT NULL
    )
    WHERE rn = 1
),

cit_brand_base AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS period_date,
        CASE
            WHEN UPPER(TRIM(tag)) = 'LOB - POSTPAID' THEN 'POSTPAID'
            ELSE NULL
        END AS lob,
        UPPER(TRIM('AI SEARCH')) AS channel,
        CASE
            WHEN LOWER(TRIM(root_domain)) = 't-mobile.com' THEN 'TMO'
            WHEN LOWER(TRIM(root_domain)) = 'att.com' THEN 'ATT'
            WHEN LOWER(TRIM(root_domain)) = 'verizon.com' THEN 'VERIZON'
            ELSE NULL
        END AS company,
        'BRAND' AS brand_type,
        'CITATION' AS metric_source,

        CAST(NULL AS FLOAT64) AS executions,
        CAST(NULL AS FLOAT64) AS mentions_count,
        CAST(NULL AS FLOAT64) AS visibility_share_of_voice,
        CAST(NULL AS FLOAT64) AS visibility_score,
        SAFE_CAST(count AS FLOAT64) AS citation_count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS citation_share,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        TIMESTAMP(File_Load_datetime) AS file_load_datetime,
        Filename AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_cit_tag_monthly_tmo`
    WHERE UPPER(TRIM(tag)) = 'LOB - POSTPAID'
      AND LOWER(TRIM(root_domain)) IN ('t-mobile.com', 'att.com', 'verizon.com')
),

cit_brand AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY period_date, lob, channel, company, brand_type, metric_source
                ORDER BY
                    file_load_datetime DESC,
                    filename DESC,
                    insert_date DESC,
                    citation_count DESC,
                    citation_share DESC
            ) AS rn
        FROM cit_brand_base
        WHERE lob IS NOT NULL
          AND company IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    period_date,
    lob,
    channel,
    company,
    brand_type,
    metric_source,
    executions,
    mentions_count,
    visibility_share_of_voice,
    visibility_score,
    citation_count,
    citation_share
FROM vis_nonbrand

UNION ALL

SELECT
    period_date,
    lob,
    channel,
    company,
    brand_type,
    metric_source,
    executions,
    mentions_count,
    visibility_share_of_voice,
    visibility_score,
    citation_count,
    citation_share
FROM cit_nonbrand

UNION ALL

SELECT
    period_date,
    lob,
    channel,
    company,
    brand_type,
    metric_source,
    executions,
    mentions_count,
    visibility_share_of_voice,
    visibility_score,
    citation_count,
    citation_share
FROM vis_brand

UNION ALL

SELECT
    period_date,
    lob,
    channel,
    company,
    brand_type,
    metric_source,
    executions,
    mentions_count,
    visibility_share_of_voice,
    visibility_score,
    citation_count,
    citation_share
FROM cit_brand
;