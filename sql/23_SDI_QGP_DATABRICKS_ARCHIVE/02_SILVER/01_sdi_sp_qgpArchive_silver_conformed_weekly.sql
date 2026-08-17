/* =================================================================================================
FILE:         01_sdi_sp_qgpArchive_silver_conformed_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_qgpArchive_silver_conformed_weekly

PURPOSE:
  Conformed layer on top of sdi_tbl_qgpArchive_bronze_retained_weekly. Bronze deliberately
  mirrors the raw feed's own column names/casing/types as closely as possible (so PulseTMS's
  Bronze could repoint at it with a one-line change and nothing else). This layer is the
  opposite: light, general-purpose cleanup -- trimmed text, typed dates/numerics, a normalized
  is_future boolean, standard snake_case column names -- so any consumer of this archive
  (PulseTMS or otherwise) gets a clean base without re-deriving that normalization themselves.

  Still the FULL feed, not scoped to PulseTMS's 10 metrics -- same scope as Bronze. Narrowing
  happens at the point of consumption (PulseTMS's own Bronze, or any future consumer's), not
  here.

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_qgpArchive_bronze_retained_weekly.

REBUILD STRATEGY -- CREATE OR REPLACE, not MERGE:
  Unlike Bronze, this layer is safe to fully rebuild every run. The retention problem this
  whole archive exists to solve is specifically about the RAW SOURCE'S rolling window --
  Bronze is the layer directly exposed to that risk, which is why Bronze uses MERGE (see its
  own header). Once Bronze itself never loses data, a full CREATE OR REPLACE rebuild of this
  layer FROM Bronze carries forward everything Bronze has ever accumulated -- same reasoning
  already applied to why PulseTMS's own Silver doesn't need MERGE treatment either.

GRAIN:
  One row per week_ending x metric_id x date_context x metric_type x page -- unchanged from
  Bronze; this layer doesn't alter grain, only types/casing/naming.

NORMALIZATION APPLIED:
  - All text columns trimmed.
  - is_future: same normalization PulseTMS's own Bronze already applies when reading this feed
    ('Is Future' / 'IsFuture' -> TRUE, 'Is Past' -> FALSE) -- done once here so every future
    consumer of the archive gets it for free instead of re-implementing it.
  - Column names standardized to snake_case (metric_id, week_ending, etc.) -- deliberately
    DIFFERENT from Bronze's PascalCase mirror, since this layer's whole purpose is to be the
    clean/conformed version, not another raw mirror.
================================================================================================= */
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_qgpArchive_silver_conformed_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_qgpArchive_silver_conformed_weekly
  USING DELTA
  CLUSTER BY (week_ending, metric_id, date_context, metric_type)
  COMMENT 'QGP archive, conformed layer -- full feed (not scoped to PulseTMS), trimmed/typed/snake_case, is_future normalized to boolean. Rebuilt fully each run from sdi_tbl_qgpArchive_bronze_retained_weekly, which itself never loses history -- safe to CREATE OR REPLACE here. Refreshed weekly via sdi_sp_qgpArchive_silver_conformed_weekly, must run after sdi_sp_qgpArchive_bronze_retained_weekly.'
  AS
  SELECT
    TRY_CAST(b.PublishKey AS DATE)             AS publish_key,
    b.QuarterNum                                AS quarter_num,
    b.YearNum                                   AS year_num,
    b.WeekEnding                                AS week_ending,
    TRIM(b.MetricID)                            AS metric_id,
    TRIM(b.DateContext)                         AS date_context,
    TRIM(b.Page)                                AS page,
    TRIM(b.Section)                             AS section,
    TRIM(b.Header)                              AS header,
    TRIM(b.MetricName)                          AS metric_name,
    TRIM(b.MetricType)                          AS metric_type,
    TRIM(b.DisplayMetricType)                   AS display_metric_type,
    b.MetricFormat                              AS metric_format,
    b.DaysInArrears                             AS days_in_arrears,
    b.CumulativeDates                           AS cumulative_dates,
    -- Same normalization PulseTMS's own Bronze applies to this feed's inconsistent
    -- 'Is Future' / 'IsFuture' / 'Is Past' spacing and casing.
    CASE
      WHEN LOWER(REPLACE(TRIM(b.IsFuture), ' ', '')) = 'isfuture' THEN TRUE
      WHEN LOWER(REPLACE(TRIM(b.IsFuture), ' ', '')) = 'ispast'   THEN FALSE
      ELSE NULL
    END                                          AS is_future,
    b.MetricOrder                               AS metric_order,
    b.VarianceDirection                         AS variance_direction,
    b.LevelofPrecision                          AS level_of_precision,
    b.Amount                                    AS amount,
    b.DisplayAmount                             AS display_amount,
    b.VariancePercentage                        AS variance_percentage,
    b.VarianceColor                             AS variance_color,
    TRIM(b.MetricOwner)                         AS metric_owner,
    b.DataDictionaryURL                         AS data_dictionary_url,
    b.DrillDownURL1                             AS drill_down_url_1,
    b.DrillDownURL2                             AS drill_down_url_2,
    b.InsertDateTime                            AS insert_datetime
  FROM prdrzranalytics.lab42.sdi_tbl_qgpArchive_bronze_retained_weekly b;

END;
