/* =================================================================================================
FILE:           03_sdi_sp_biddProgrammaticArchive_gold_curated_weekly.sql
PLATFORM:       Databricks
PROJECT:        biddProgrammaticArchive (Biddable Programmatic spend archive)
LAYER:          Gold
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_biddProgrammaticArchive_gold_curated_weekly
TABLE:          sdi_tbl_biddProgrammaticArchive_gold_curated_weekly

PURPOSE:
  Curated, consumer-facing Programmatic spend: one row per spend_date x lob x dsp, full archived
  history. This is the table PulseTMS reads (Biddable Bronze and validation).

  CHANGED 2026-11-15: Gold now carries Programmatic spend AS THE PROGRAMMATIC TEAM REPORTS IT
  (previously: every source row of every campaign). Silver is unchanged and still holds every raw
  row and column, so nothing is lost; only this curated layer applies the team's rules. PulseTMS
  Biddable Bronze and the validation procedure read this table by the same columns as before and
  need no change.

THE PROGRAMMATIC TEAM'S DEFINITION (ported from their BigQuery query):

  1. ROWS KEPT
       spend > 0
       AND campaign_name LIKE 'TMO%' OR 'MBTM%'                  (case-sensitive)
       AND campaign_name does NOT contain '-INI', '-OTT' or '_OTT' (case-sensitive)
     Everything else is dropped, including the whole ARCHIVED LOB campaigns (none starts with an
     uppercase TMO / MBTM), INI and OTT campaigns, and zero or negative spend rows.
     No date filter: the rules apply to every archived date (the archive starts in 2026-01 and the
     naming holds in every month, checked 2026-11-15).

  2. EXCEPTION CAMPAIGN (impression-share re-allocation)
     The campaign listed in the ExceptionCampaigns CTE (TMO_PostpaidHINT_VoiceHINTFiber_...
     _DV360_X_US_DSP-CMP-001) has its spend summed per DSP x day and split between two LOBs by
     impressions:
         placement contains HINT or FIBER (any case) -> HSI
         anything else                               -> POSTPAID
     Its source LOB is ignored. Spend = day total x (LOB impressions / day impressions).
     A DSP x day with no impressions has no share, so that day's exception spend is not carried
     (same as the team's query). To treat another campaign the same way, add it to the list.

  3. OUTPUT LOB
     UPPER(TRIM(lob)) as before and not recoded (POSTPAID, HSI, PREPAID, TFB, ARCHIVED, ...). Every
     LOB that survives rule 1 is kept: consumers apply their own LOB scope.

COLUMNS:
  spend_date           DATE       Day the spend happened.
  lob                  STRING     UPPER(TRIM(source lob)); HSI / POSTPAID for the exception campaign.
  dsp                  STRING     TRIM(source DSP), not recoded.
  spend                DOUBLE     Reported spend (rules above). Not rounded.
  source_row_count     BIGINT     Source rows kept and summed into this row. 0 for spend that comes
                                  only from the exception campaign re-allocation.
  archive_loaded_ts    TIMESTAMP  Archive run that loaded the dates behind this row.
  spend_reallocated    DOUBLE     Part of spend that comes from the exception campaign re-allocation.

HOW THIS RELATES TO THE EARLIER GOLD:
  Earlier Gold = SUM of every source row. This Gold = the team's reported number. The difference is
  large (about 45-55 percent of Postpaid + HSI spend in 2026-08..10: INI / OTT campaigns and campaigns
  without a TMO / MBTM prefix). Silver still has all of it for any other use.

REBUILD:
  Full CREATE OR REPLACE every run (safe: Bronze never loses dates).

REQUIRED SILVER COLUMNS (raw source columns carried by Silver):
  campaign_name, placement, impressions, plus the conformed spend_date, lob_conformed,
  dsp_conformed, spend_amount, archive_loaded_ts.

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_gold_curated_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Biddable Programmatic archive, Gold: rebuilds sdi_tbl_biddProgrammaticArchive_gold_curated_weekly, daily Programmatic spend by spend_date x lob x dsp as the Programmatic team reports it (TMO / MBTM campaigns, INI and OTT excluded, exception campaign split by impression share), full archived history. Read by PulseTMS Biddable Bronze and validation.'

AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly

  USING DELTA

  CLUSTER BY (spend_date, lob)

  COMMENT 'Biddable Programmatic archive, Gold (curated). Daily Programmatic spend by spend_date x lob x dsp as the Programmatic team reports it: spend > 0, campaigns starting TMO or MBTM, names containing -INI / -OTT / _OTT excluded, and the exception campaign re-allocated between HSI and POSTPAID by impression share. Full history kept by the archive. Read by PulseTMS Biddable Bronze and validation. Rebuilt by sdi_sp_biddProgrammaticArchive_gold_curated_weekly.'

  AS

  WITH
  
    /* ===============================================================================================
       CAMPAIGNS RE-ALLOCATED BY IMPRESSION SHARE
       The Programmatic team reports this campaign's spend split between HSI and Postpaid by where it
       delivered. To add another campaign with the same treatment, add one line to this list.
       =============================================================================================== */
  
    ExceptionCampaigns AS (
      SELECT campaign_name
      FROM VALUES
        ('TMO_PostpaidHINT_VoiceHINTFiber_X_PSTPHINTPro_Perf_VINT_X_EL_DIS_2026_Multi-Opt_PR-OEX_DV360_X_US_DSP-CMP-001')
      AS t(campaign_name)
    ),
  
  
    /* ===============================================================================================
       BASE: THE PROGRAMMATIC TEAM'S ROW RULES (case-sensitive, exactly as in their query)
         - spend > 0
         - campaign name starts with TMO or MBTM
         - campaign name does NOT contain -INI, -OTT or _OTT
       =============================================================================================== */
  
    Base AS (
      SELECT
        s.spend_date,
        s.dsp_conformed                         AS dsp,
        s.lob_conformed                         AS lob,
        s.campaign_name,
        s.placement,
        s.spend_amount                          AS spend,
        TRY_CAST(s.impressions AS DOUBLE)       AS impressions,
        s.archive_loaded_ts
      FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly s
      WHERE s.spend_date IS NOT NULL
        AND s.spend_amount > 0
        AND (s.campaign_name LIKE 'TMO%' OR s.campaign_name LIKE 'MBTM%')
        AND NOT CONTAINS(s.campaign_name, '-INI')
        AND NOT CONTAINS(s.campaign_name, '-OTT')
        AND NOT CONTAINS(s.campaign_name, '_OTT')
    ),
  
    BaseFlagged AS (
      SELECT
        b.*,
        (e.campaign_name IS NOT NULL)           AS is_exception
      FROM Base b
      LEFT JOIN ExceptionCampaigns e
        ON b.campaign_name = e.campaign_name
    ),
  
  
    /* ===============================================================================================
       NORMAL CAMPAIGNS: spend stays on the source LOB
       =============================================================================================== */
  
    NormalCampaigns AS (
      SELECT
        spend_date,
        dsp,
        lob,
        SUM(spend)                              AS spend,
        CAST(COUNT(*) AS BIGINT)                AS source_row_count,
        MAX(archive_loaded_ts)                  AS archive_loaded_ts
      FROM BaseFlagged
      WHERE NOT is_exception
      GROUP BY spend_date, dsp, lob
    ),
  
  
    /* ===============================================================================================
       EXCEPTION CAMPAIGNS: total spend per DSP x day, split by impression share
         placement contains HINT or FIBER -> HSI, anything else -> POSTPAID
       =============================================================================================== */
  
    ExceptionRows AS (
      SELECT
        spend_date,
        dsp,
        CASE
          WHEN UPPER(placement) RLIKE 'HINT|FIBER' THEN 'HSI'
          ELSE 'POSTPAID'
        END                                     AS delivery_lob,
        spend,
        impressions,
        archive_loaded_ts
      FROM BaseFlagged
      WHERE is_exception
    ),
  
    ExceptionTotalSpend AS (
      SELECT
        spend_date,
        dsp,
        SUM(spend)                              AS total_spend,
        MAX(archive_loaded_ts)                  AS archive_loaded_ts
      FROM ExceptionRows
      GROUP BY spend_date, dsp
    ),
  
    ExceptionDelivery AS (
      SELECT
        spend_date,
        dsp,
        delivery_lob                            AS lob,
        SUM(impressions)                        AS lob_impressions
      FROM ExceptionRows
      GROUP BY spend_date, dsp, delivery_lob
    ),
  
    ExceptionDeliveryShare AS (
      SELECT
        spend_date,
        dsp,
        lob,
        try_divide(
          lob_impressions,
          SUM(lob_impressions) OVER (PARTITION BY dsp, spend_date)
        )                                       AS impression_share
      FROM ExceptionDelivery
    ),
  
    /* A DSP x day whose impressions are all NULL or 0 has no share, so its spend cannot be split and
       is not carried (same as the Programmatic team's query). */
    ExceptionReallocated AS (
      SELECT
        t.spend_date,
        t.dsp,
        d.lob,
        t.total_spend * d.impression_share      AS spend,
        CAST(0 AS BIGINT)                       AS source_row_count,
        t.archive_loaded_ts
      FROM ExceptionTotalSpend t
      JOIN ExceptionDeliveryShare d
        ON  t.dsp        = d.dsp
        AND t.spend_date = d.spend_date
      WHERE t.total_spend * d.impression_share IS NOT NULL
    ),
  
  
    Combined AS (
      SELECT spend_date, dsp, lob, spend, CAST(0 AS DOUBLE) AS spend_reallocated, source_row_count, archive_loaded_ts
      FROM NormalCampaigns
  
      UNION ALL
  
      SELECT spend_date, dsp, lob, spend, spend AS spend_reallocated, source_row_count, archive_loaded_ts
      FROM ExceptionReallocated
    )
  
    SELECT
      spend_date,
      lob,
      dsp,
      SUM(spend)                                AS spend,
      SUM(source_row_count) AS source_row_count,
      MAX(archive_loaded_ts)                    AS archive_loaded_ts,
      SUM(spend_reallocated)                    AS spend_reallocated
    FROM Combined
    GROUP BY spend_date, lob, dsp;


  /* Column descriptions (set after the rebuild; CREATE OR REPLACE resets them). */
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN spend_date        COMMENT 'Day the spend happened (source date).';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN lob               COMMENT 'Source LOB, UPPER(TRIM()) only, not recoded: POSTPAID, HSI, PREPAID, TFB, ... The exception campaign is re-allocated to HSI / POSTPAID. Consumers apply their own LOB scope and canonicalization.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN dsp               COMMENT 'Source DSP, TRIM() only, not recoded: DV360, The Trade Desk, Amazon DSP, Blis, ... Consumers apply their own platform normalization.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN spend             COMMENT 'Programmatic spend as the Programmatic team reports it (see procedure header). Not rounded.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN source_row_count  COMMENT 'Source rows kept and summed into this row; 0 when the spend comes only from the exception campaign re-allocation.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN archive_loaded_ts COMMENT 'Timestamp of the archive run that loaded the dates behind this row.';
  ALTER TABLE prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
    ALTER COLUMN spend_reallocated COMMENT 'Part of spend that comes from the exception campaign impression-share re-allocation.';

END;