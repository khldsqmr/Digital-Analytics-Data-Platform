/*
  01_sdi_vw_dashboardPulseTms_gold_appendix_long.sql
  ------------------------------------------------------------------------
  PulseTMS dashboard appendix (Gold): same grain as Silver (one row per
  header/bullet), adds the dashboard-facing derived columns Tableau
  actually binds to. Ported from BigQuery's
  vw_sdi_pulse_gold_pulseTab1_appendix (unnamed in the source -- named
  here to match this project's sdi_vw_dashboardPulseTms_<layer>_<purpose>
  convention).

  Port notes (BQ -> Databricks):
  - This query uses no BQ-specific functions at all (CASE/WHEN only), so
    the only change is the FROM: the BQ three-part table id becomes the
    fully-qualified Unity Catalog name of the Silver view above (adjust
    the catalog/schema below if yours differs from prdrzranalytics.lab42).
*/

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_appendix_long
COMMENT 'PulseTMS dashboard appendix (Gold): Silver plus the dashboard-facing derived columns (funnel stage key/label, label options) that Tableau binds to.'
AS
select
    s.*,
    case s.apx_funnel_stage
        when 'Top Funnel'    then 'top_funnel'
        when 'Mid Funnel'    then 'mid_funnel'
        when 'Bottom Funnel' then 'bottom_funnel'
        else                      'glossary'
    end as apx_funnel_stage_key,
    case s.apx_funnel_stage
        when 'Top Funnel'    then 'Top Funnel'
        when 'Mid Funnel'    then 'Mid Funnel'
        when 'Bottom Funnel' then 'Bottom Funnel'
        else                      'Glossary'
    end as apx_funnel_stage_label,
    case
        when s.apx_row_type in ('metric_header', 'glossary_header')
            then s.apx_label
        else null
    end as apx_label_options
from prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_silver_appendix_long s;