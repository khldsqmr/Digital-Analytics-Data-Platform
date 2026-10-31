/* =================================================================================================
FILE:           03_sdi_sp_biddProgrammaticArchive_gold_curated_weekly.sql
PLATFORM:       Databricks
PROJECT:        biddProgrammaticArchive (Biddable Programmatic spend archive)
LAYER:          Gold
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_biddProgrammaticArchive_gold_curated_weekly
TABLE:          sdi_tbl_biddProgrammaticArchive_gold_curated_weekly

PURPOSE:
  Curated, consumer-facing Programmatic spend: one row per spend_date x lob x dsp, all LOBs and
  DSPs, full archived history. This is the table PulseTMS reads (Biddable Bronze and validation).

COLUMNS:
  spend_date          DATE    Day the spend happened.
  lob                 STRING  UPPER(TRIM(source lob)), not recoded (POSTPAID, HSI, PREPAID, TFB, ...).
  dsp                 STRING  TRIM(source DSP), not recoded (DV360, The Trade Desk, Amazon DSP, ...).
  spend               DOUBLE  Sum of source spend for the day, LOB and DSP (not rounded, so it
                              reconciles to the source to the cent).
  source_row_count    BIGINT  Source rows summed into this row (traceability).
  archive_loaded_ts   TIMESTAMP  Archive run that loaded this date.

WHY THIS GRAIN:
  PulseTMS Biddable Bronze only needs date, LOB, DSP and spend, and it sums to week x LOB x
  platform anyway, so summing to day x LOB x DSP first gives identical PulseTMS results while
  keeping this table small. Finer detail (channel, buy type, account, campaign, placement)
  stays available in Silver for any future use.

  Exact equivalence with reading the raw source:
    - Same dates, LOBs and DSP spellings (TRIM only; PulseTMS still applies its own UPPER/TRIM
      DSP normalization and LOB scope on top).
    - spend = SUM(TRY_CAST(spend AS DOUBLE)); PulseTMS sums COALESCE(spend, 0) afterwards, so a
      group whose spend is all NULL lands as 0 in PulseTMS either way.

REBUILD:
  Full CREATE OR REPLACE every run (safe: Bronze never loses dates).

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_gold_curated_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Biddable Programmatic archive, Gold: rebuilds sdi_tbl_biddProgrammaticArchive_gold_curated_weekly, daily Programmatic spend by spend_date x lob x dsp from the conformed Silver, all LOBs and DSPs, full archived history. Read by PulseTMS Biddable Bronze and validation.'

AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly

  USING DELTA

  CLUSTER BY (spend_date, lob)

  COMMENT 'Biddable Programmatic archive, Gold (curated). Daily Programmatic spend by spend_date x lob x dsp, all LOBs and DSPs, full history kept by the archive (the raw source pbi_programmatic_browsers_currentyr only holds the current year). Read by PulseTMS Biddable Bronze and validation. Rebuilt by sdi_sp_biddProgrammaticArchive_gold_curated_weekly.'

  AS

  SELECT
    s.spend_date,
    s.lob_conformed                         AS lob,
    s.dsp_conformed                         AS dsp,
    SUM(s.spend_amount)                     AS spend,
    COUNT(*)                                AS source_row_count,
    MAX(s.archive_loaded_ts)                AS archive_loaded_ts

  FROM
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly s

  WHERE
    s.spend_date IS NOT NULL

  GROUP BY
    s.spend_date,
    s.lob_conformed,
    s.dsp_conformed;


  /* Column descriptions (set after the rebuild; CREATE OR REPLACE resets them). */
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN spend_date        COMMENT 'Day the spend happened (source date).';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN lob               COMMENT 'Source LOB, UPPER(TRIM()) only, not recoded: POSTPAID, HSI, PREPAID, TFB, ... Consumers apply their own LOB scope and canonicalization.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN dsp               COMMENT 'Source DSP, TRIM() only, not recoded: DV360, The Trade Desk, Amazon DSP, Blis, ... Consumers apply their own platform normalization.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN spend             COMMENT 'Sum of source spend (TRY_CAST to DOUBLE) for spend_date x lob x dsp. Not rounded, so it reconciles to the source.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN source_row_count  COMMENT 'Number of source rows summed into this row.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN archive_loaded_ts COMMENT 'Timestamp of the archive run that loaded this date.';

END;