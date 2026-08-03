/* =================================================================================================
FILE:         00_sdi_sp_qgpArchive_call_all_weekly.sql
PURPOSE:
  Weekly orchestration entry point for the standalone QGP archive pipeline (Bronze -> Silver ->
  Gold). Separate from PulseTMS's own orchestration script (00_sdi_sp_dashboardPulseTms_call_
  all_weekly.sql) because this archive is deliberately PulseTMS-agnostic -- a durable, general-
  purpose mirror of the full QGP scorecard feed that any consumer could build on, not just
  PulseTMS. Kept as its own job rather than folded into PulseTMS's script.

CROSS-PIPELINE DEPENDENCY -- READ BEFORE SCHEDULING:
  PulseTMS's own Bronze (sdi_sp_dashboardPulseTms_bronze_qgp_weekly) now reads FROM
  sdi_tbl_qgpArchive_bronze_retained_weekly instead of the raw feed directly (see that
  procedure's own header). That means THIS entire script must run to completion, successfully,
  BEFORE PulseTMS's orchestration script runs each week -- not just before PulseTMS's own QGP
  Bronze step, before the whole PulseTMS job, since Databricks Job SQL File tasks don't have
  per-statement dependency awareness of a separate job. Two ways to enforce that ordering:
    a) Two separate Databricks Jobs, this one and PulseTMS's, wired with a Job dependency
       ("run after") so PulseTMS's job literally cannot start until this one finishes.
    b) Merge this script's 3 CALLs into the TOP of PulseTMS's own orchestration file, ahead of
       its Bronze step, and retire this file as a separate job. Simpler operationally (one job,
       one schedule) at the cost of coupling the archive's schedule to PulseTMS's -- reasonable
       if nothing else is expected to consume this archive independently any time soon.
  Whichever you pick, do NOT schedule these as two independent jobs on a shared clock with no
  dependency edge (e.g. "this one runs at 2am, PulseTMS's runs at 3am") -- a delayed or retried
  run here would silently let PulseTMS's Bronze read a stale archive with no error raised.

HOW TO SCHEDULE (within this script itself):
  Databricks Jobs > Tasks > Type = SQL > SQL task = File > Source = Workspace, point at this
  file, select a serverless or pro SQL warehouse. One task, statements run top-to-bottom in the
  order below.

DEPENDENCY ORDER (strict -- each layer reads from the one before it):
  1. Bronze (sdi_sp_qgpArchive_bronze_retained_weekly) -- MERGE-upserts the full raw feed.
     Must run first; everything below reads from its output.
  2. Silver (sdi_sp_qgpArchive_silver_conformed_weekly) -- full rebuild from Bronze, safe
     because Bronze itself never loses data.
  3. Gold (sdi_sp_qgpArchive_gold_curated_weekly) -- full rebuild from Silver, Normal-only.
================================================================================================= */

-- ===========================================================================
-- STEP 1: Bronze -- MERGE into the never-shrinking archive
-- ===========================================================================
CALL prdrzranalytics.lab42.sdi_sp_qgpArchive_bronze_retained_weekly();

-- ===========================================================================
-- STEP 2: Silver -- conformed full rebuild
-- ===========================================================================
CALL prdrzranalytics.lab42.sdi_sp_qgpArchive_silver_conformed_weekly();

-- ===========================================================================
-- STEP 3: Gold -- curated (Normal-only) full rebuild
-- ===========================================================================
CALL prdrzranalytics.lab42.sdi_sp_qgpArchive_gold_curated_weekly();