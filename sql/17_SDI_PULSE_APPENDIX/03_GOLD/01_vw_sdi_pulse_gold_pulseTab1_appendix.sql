CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_gold_pulseTab1_appendix`
OPTIONS(description="Gold: Tableau-ready layer on top of silver. Adds funnel_stage_key, funnel_stage_label, and apx_label_options. All boolean fields removed - use apx_row_type directly in Tableau calculated fields to avoid True/False string type issues.")
AS
SELECT
  s.*,
  CASE s.apx_funnel_stage
    WHEN 'Top Funnel'    THEN 'top_funnel'
    WHEN 'Mid Funnel'    THEN 'mid_funnel'
    WHEN 'Bottom Funnel' THEN 'bottom_funnel'
    ELSE                      'glossary'
  END AS apx_funnel_stage_key,
  CASE s.apx_funnel_stage
    WHEN 'Top Funnel'    THEN 'Top Funnel'
    WHEN 'Mid Funnel'    THEN 'Mid Funnel'
    WHEN 'Bottom Funnel' THEN 'Bottom Funnel'
    ELSE                      'Glossary'
  END AS apx_funnel_stage_label,
  CASE
    WHEN s.apx_row_type IN ('metric_header','glossary_header')
    THEN s.apx_label
    ELSE NULL
  END AS apx_label_options
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_silver_pulseTab1_appendix` s;