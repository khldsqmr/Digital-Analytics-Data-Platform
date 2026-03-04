
/* =================================================================================================
FILE: 07_create_sdi_bronze_seo_profound_citations_topic_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_bronze_seo_profound_citations_topic_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo

PURPOSE:
  Canonical Bronze daily table for ProFound Citations by Root Domain + Topic:
    - Canonical DATE parsed from date_yyyymmdd
    - Keep raw INT64 date for lineage/debug
    - Preserve lineage fields (file_load_datetime, filename, __insert_date)
    - Dedupe per grain using latest file load

GRAIN:
  account_id + root_domain + topic + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_daily`
(
  account_id STRING,
  account_name STRING,
  root_domain STRING,
  topic STRING,

  date_yyyymmdd STRING,
  date DATE,
  raw_date_int64 INT64,

  count FLOAT64,
  share_of_voice FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, topic
OPTIONS(description="Bronze ProFound Citations Topic Daily. Canonical date + dedupe + lineage.");



/* =================================================================================================
FILE: 07_backfill_sdi_bronze_seo_profound_citations_topic_daily.sql
LAYER: Bronze (One-time / On-demand Backfill)
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TARGET TABLE: sdi_bronze_seo_profound_citations_topic_daily
SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo

PURPOSE:
  Backfill historical ProFound Citations Topic Daily into Bronze using the SAME logic as incremental.

GRAIN:
  account_id + root_domain + topic + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic
================================================================================================= */

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01');  -- <-- change
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();      -- <-- change
DECLARE chunk_days          INT64 DEFAULT 14;

DECLARE chunk_start DATE;
DECLARE chunk_end   DATE;

SET chunk_start = backfill_start_date;

LOOP
  IF chunk_start > backfill_end_date THEN LEAVE; END IF;
  SET chunk_end = LEAST(DATE_ADD(chunk_start, INTERVAL chunk_days - 1 DAY), backfill_end_date);

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(root_domain AS STRING)), '') AS root_domain,
        NULLIF(TRIM(SAFE_CAST(topic AS STRING)), '') AS topic,

        SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(date AS INT64) AS raw_date_int64,

        SAFE_CAST(count AS FLOAT64) AS count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo`
      WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
        BETWEEN chunk_start AND chunk_end
    ),
    cleaned AS (
      SELECT * FROM src
      WHERE date IS NOT NULL
        AND account_id IS NOT NULL
        AND root_domain IS NOT NULL
        AND topic IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, root_domain, topic, date_yyyymmdd
            ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
          ) AS rn
        FROM cleaned c
      )
      WHERE rn = 1
    )
    SELECT * FROM dedup
  ) S
  ON  T.account_id     = S.account_id
  AND T.root_domain    = S.root_domain
  AND T.topic          = S.topic
  AND T.date_yyyymmdd   = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    date = S.date,
    raw_date_int64 = S.raw_date_int64,
    count = S.count,
    share_of_voice = S.share_of_voice,
    insert_date = S.insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

  WHEN NOT MATCHED THEN INSERT ROW;

  SET chunk_start = DATE_ADD(chunk_end, INTERVAL 1 DAY);
END LOOP;


/* =================================================================================================
FILE: 08_create_sdi_bronze_seo_profound_citations_topic_tag_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_bronze_seo_profound_citations_topic_tag_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo

PURPOSE:
  Canonical Bronze daily table for ProFound Citations by Root Domain + Topic + Tag:
    - Canonical DATE parsed from date_yyyymmdd
    - Keep raw INT64 date for lineage/debug
    - Preserve lineage fields (file_load_datetime, filename, __insert_date)
    - Dedupe per grain using latest file load

GRAIN:
  account_id + root_domain + topic + tag + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic, tag
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_tag_daily`
(
  account_id STRING,
  account_name STRING,
  root_domain STRING,
  topic STRING,
  tag STRING,

  date_yyyymmdd STRING,
  date DATE,
  raw_date_int64 INT64,

  count FLOAT64,
  share_of_voice FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, topic, tag
OPTIONS(description="Bronze ProFound Citations Topic+Tag Daily. Canonical date + dedupe + lineage.");


/* =================================================================================================
FILE: 08_sp_merge_sdi_bronze_seo_profound_citations_topic_tag_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_merge_bronze_seo_profound_citations_topic_tag_daily
TARGET TABLE: sdi_bronze_seo_profound_citations_topic_tag_daily

PURPOSE:
  Incrementally upsert ProFound Citations Topic+Tag Daily into Bronze:
    - filter by File_Load_datetime lookback window
    - parse canonical DATE from date_yyyymmdd
    - dedupe per grain using latest (file_load_datetime, filename, insert_date)

GRAIN:
  account_id + root_domain + topic + tag + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic, tag
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_citations_topic_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_tag_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(root_domain AS STRING)), '') AS root_domain,
        NULLIF(TRIM(SAFE_CAST(topic AS STRING)), '') AS topic,
        NULLIF(TRIM(SAFE_CAST(tag AS STRING)), '') AS tag,

        SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(date AS INT64) AS raw_date_int64,

        SAFE_CAST(count AS FLOAT64) AS count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo`
      WHERE SAFE_CAST(File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
    ),
    cleaned AS (
      SELECT * FROM src
      WHERE date IS NOT NULL
        AND account_id IS NOT NULL
        AND root_domain IS NOT NULL
        AND topic IS NOT NULL
        AND tag IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, root_domain, topic, tag, date_yyyymmdd
            ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
          ) AS rn
        FROM cleaned c
      )
      WHERE rn = 1
    )
    SELECT * FROM dedup
  ) S
  ON  T.account_id     = S.account_id
  AND T.root_domain    = S.root_domain
  AND T.topic          = S.topic
  AND T.tag            = S.tag
  AND T.date_yyyymmdd   = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    date = S.date,
    raw_date_int64 = S.raw_date_int64,
    count = S.count,
    share_of_voice = S.share_of_voice,
    insert_date = S.insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

  WHEN NOT MATCHED THEN INSERT ROW;

END;


/* =================================================================================================
FILE: 08_backfill_sdi_bronze_seo_profound_citations_topic_tag_daily.sql
LAYER: Bronze (One-time / On-demand Backfill)
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TARGET TABLE: sdi_bronze_seo_profound_citations_topic_tag_daily
SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo

PURPOSE:
  Backfill historical ProFound Citations Topic+Tag Daily into Bronze using the SAME logic as incremental.

GRAIN:
  account_id + root_domain + topic + tag + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic, tag
================================================================================================= */

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01');  -- <-- change
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();      -- <-- change
DECLARE chunk_days          INT64 DEFAULT 14;

DECLARE chunk_start DATE;
DECLARE chunk_end   DATE;

SET chunk_start = backfill_start_date;

LOOP
  IF chunk_start > backfill_end_date THEN LEAVE; END IF;
  SET chunk_end = LEAST(DATE_ADD(chunk_start, INTERVAL chunk_days - 1 DAY), backfill_end_date);

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_tag_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(root_domain AS STRING)), '') AS root_domain,
        NULLIF(TRIM(SAFE_CAST(topic AS STRING)), '') AS topic,
        NULLIF(TRIM(SAFE_CAST(tag AS STRING)), '') AS tag,

        SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(date AS INT64) AS raw_date_int64,

        SAFE_CAST(count AS FLOAT64) AS count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo`
      WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
        BETWEEN chunk_start AND chunk_end
    ),
    cleaned AS (
      SELECT * FROM src
      WHERE date IS NOT NULL
        AND account_id IS NOT NULL
        AND root_domain IS NOT NULL
        AND topic IS NOT NULL
        AND tag IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, root_domain, topic, tag, date_yyyymmdd
            ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
          ) AS rn
        FROM cleaned c
      )
      WHERE rn = 1
    )
    SELECT * FROM dedup
  ) S
  ON  T.account_id     = S.account_id
  AND T.root_domain    = S.root_domain
  AND T.topic          = S.topic
  AND T.tag            = S.tag
  AND T.date_yyyymmdd   = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    date = S.date,
    raw_date_int64 = S.raw_date_int64,
    count = S.count,
    share_of_voice = S.share_of_voice,
    insert_date = S.insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

  WHEN NOT MATCHED THEN INSERT ROW;

  SET chunk_start = DATE_ADD(chunk_end, INTERVAL 1 DAY);
END LOOP;