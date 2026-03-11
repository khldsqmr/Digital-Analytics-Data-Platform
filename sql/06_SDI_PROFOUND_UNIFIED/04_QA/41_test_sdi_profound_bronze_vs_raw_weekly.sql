/* =================================================================================================
FILE: 41_test_sdi_profound_bronze_vs_raw_weekly.sql
PURPOSE:
  Weekly Bronze vs Raw reconciliation checks for ProFound Unified.
  These tests compare raw latest-snapshot deduped output against Bronze row counts.

NOTES:
  - Run section by section
  - If counts mismatch, inspect grain logic, duplicate handling, or stale snapshot issues
================================================================================================= */

-- -------------------------------------------------------------------------------------------------
-- 41.1 Visibility Asset Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityAsset_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityAsset_weekly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 41.2 Visibility Tag Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      tag,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityTag_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityTag_weekly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 41.3 Visibility Tag Topic Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      tag,
      topic,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_topic_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityTagTopic_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityTagTopic_weekly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 41.4 Citation Domain Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_domain_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationDomain_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationDomain_weekly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 41.5 Citation Tag Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      tag,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationTag_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTag_weekly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 41.6 Citation Tag Topic Weekly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      tag,
      topic,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_topic_weekly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationTagTopic_weekly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTagTopic_weekly`) AS bronze_count;