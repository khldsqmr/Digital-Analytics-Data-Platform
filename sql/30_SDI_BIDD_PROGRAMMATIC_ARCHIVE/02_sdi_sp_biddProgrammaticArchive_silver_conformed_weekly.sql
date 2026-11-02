/* =================================================================================================
FILE:           02_sdi_sp_biddProgrammaticArchive_silver_conformed_weekly.sql
PLATFORM:       Databricks
PROJECT:        biddProgrammaticArchive (Biddable Programmatic spend archive)
LAYER:          Silver
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_biddProgrammaticArchive_silver_conformed_weekly
TABLE:          sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly

PURPOSE:
  Conformed copy of the archive: exactly ONE copy of each source date (the latest archive run),
  every raw column kept, plus four standardized columns that downstream layers use:

    spend_date      DATE    archive_date (the source date)
    lob_conformed   STRING  UPPER(TRIM(lob))        e.g. POSTPAID, HSI, PREPAID, TFB (no recoding)
    dsp_conformed   STRING  TRIM(DSP)               e.g. DV360, The Trade Desk (no recoding)
    spend_amount    DOUBLE  TRY_CAST(spend AS DOUBLE)

  No LOB, DSP or channel is filtered or recoded here: the archive is PulseTMS-agnostic. Business
  rules (LOB scope, DSP normalization) stay in the consuming pipeline (PulseTMS Biddable Bronze).

WHY ONE COPY PER DATE:
  Bronze replaces dates by INSERT-then-DELETE. If a run is interrupted between the two steps a
  date can briefly hold two runs; this layer keeps only the latest, so nothing downstream ever
  double counts.

REBUILD:
  Full CREATE OR REPLACE every run (safe: Bronze itself never loses dates), same as the QGP
  archive's Silver.

RUN ORDER:
  After sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly, before
  sdi_sp_biddProgrammaticArchive_gold_curated_weekly.

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_silver_conformed_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Biddable Programmatic archive, Silver: rebuilds sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly from Bronze with one copy per source date (latest archive run), all raw columns, plus spend_date, lob_conformed, dsp_conformed, spend_amount.'

AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly

  USING DELTA

  CLUSTER BY (spend_date, lob_conformed)

  COMMENT 'Biddable Programmatic archive, Silver (conformed). One copy of each source date from sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly (latest archive run). All raw source columns kept unchanged, plus spend_date (DATE), lob_conformed (UPPER TRIM lob, not recoded), dsp_conformed (TRIM DSP, not recoded), spend_amount (spend as DOUBLE). No filtering. Rebuilt by sdi_sp_biddProgrammaticArchive_silver_conformed_weekly.'

  AS

  SELECT
    b.archive_date                          AS spend_date,
    UPPER(TRIM(b.lob))                      AS lob_conformed,
    TRIM(b.DSP)                             AS dsp_conformed,
    TRY_CAST(b.spend AS DOUBLE)             AS spend_amount,
    b.*

  FROM
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly b

  WHERE
    b.archive_date IS NOT NULL

  QUALIFY
    DENSE_RANK() OVER (
      PARTITION BY b.archive_date
      ORDER BY     b.archive_loaded_ts DESC, b.archive_run_id DESC
    ) = 1;

END;