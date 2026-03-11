/* =================================================================================================
FILE: 42_test_sdi_profound_bronze_vs_raw_monthly.sql
PURPOSE:
  Monthly Bronze vs Raw reconciliation checks for ProFound.
================================================================================================= */

-- Repeat same logic pattern as weekly, but for monthly raw + Bronze tables.

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, asset_id, asset_name, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibility_asset_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_monthly`) AS bronze_count;

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, asset_id, asset_name, tag, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibility_tag_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_monthly`) AS bronze_count;

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, asset_id, asset_name, tag, topic, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_topic_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibility_tag_topic_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_topic_monthly`) AS bronze_count;

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, root_domain, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_domain_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citation_domain_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citation_domain_monthly`) AS bronze_count;

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, root_domain, tag, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citation_tag_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citation_tag_monthly`) AS bronze_count;

WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id, root_domain, tag, topic, date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_topic_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citation_tag_topic_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citation_tag_topic_monthly`) AS bronze_count;