/* =================================================================================================
FILE:         01_sdi_sp_qgpArchive_gold_curated_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_qgpArchive_gold_curated_weekly

PURPOSE:
  Curated, ready-to-query layer on top of sdi_tbl_qgpArchive_silver_conformed_weekly. This is
  a DATA architecture layer, not a dashboard/presentation one -- it deliberately does NOT
  reshape into the long/wide pivoted formats PulseTMS's own gold_unified_long / gold_unified_
  wide use for BI consumption. That reshaping is specific to how PulseTMS's dashboard consumes
  QGP data; this table stays in its natural one-row-per-metric-per-week shape so it's usable by
  PulseTMS or any other future consumer without assuming a particular presentation need.

  Still the FULL feed, not scoped to PulseTMS's 10 metrics -- same scope as Bronze and Silver.

WHAT DIFFERS FROM SILVER:
  Filtered to date_context = 'Normal' only. Every other date_context value (QTD, QTR, WoW,
  PQWA, ROQ, Monthly Normal) is a derived rollup of the Normal weekly values -- proven directly
  in this pipeline's own validation work: summing every Normal week in a quarter reproduces
  that quarter's QTR value exactly, and a partial sum reproduces QTD. Silver (and Bronze under
  it) still retain all of those rows forever -- nothing is deleted anywhere in this archive --
  Gold just doesn't surface redundant, reconstructable rows to whoever queries this table
  directly. If a consumer ever needs a QTD/QTR-style rollup, it's one SUM() away from this
  table's own Normal rows, or available as-is from Silver.

REBUILD STRATEGY -- CREATE OR REPLACE, same reasoning as Silver:
  Safe to fully rebuild every run because Silver (and Bronze under it) never lose data --
  this layer isn't the one exposed to the raw source's rolling retention, so it doesn't need
  MERGE's preserve-on-delete behavior.

GRAIN:
  One row per week_ending x metric_id x metric_type x page (date_context is now constant =
  'Normal', so it's still selected for clarity/filterability but no longer part of what makes
  a row unique).
================================================================================================= */
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_qgpArchive_gold_curated_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_qgpArchive_gold_curated_weekly
  USING DELTA
  CLUSTER BY (week_ending, metric_id, metric_type)
  COMMENT 'QGP archive, curated Gold layer -- full feed (not scoped to PulseTMS), date_context=Normal only (QTD/QTR/WoW/etc. rollups dropped here as reconstructable duplicates of Normal, still retained in Silver/Bronze). Not reshaped to long/wide -- that is PulseTMS-specific presentation logic, not archive logic. Rebuilt fully each run from sdi_tbl_qgpArchive_silver_conformed_weekly. Refreshed weekly via sdi_sp_qgpArchive_gold_curated_weekly, must run after sdi_sp_qgpArchive_silver_conformed_weekly.'
  AS
  SELECT
    publish_key,
    quarter_num,
    year_num,
    week_ending,
    metric_id,
    date_context,
    page,
    section,
    header,
    metric_name,
    metric_type,
    display_metric_type,
    metric_format,
    days_in_arrears,
    cumulative_dates,
    is_future,
    metric_order,
    variance_direction,
    level_of_precision,
    amount,
    display_amount,
    variance_percentage,
    variance_color,
    metric_owner,
    data_dictionary_url,
    drill_down_url_1,
    drill_down_url_2,
    insert_datetime
  FROM prdrzranalytics.lab42.sdi_tbl_qgpArchive_silver_conformed_weekly
  WHERE UPPER(TRIM(date_context)) = 'NORMAL';

END;
