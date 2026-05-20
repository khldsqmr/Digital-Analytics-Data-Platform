CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_gold_pulseTab1_appendix`
OPTIONS(description="Gold: Tableau-ready layer. Adds funnel_stage_key, display labels, boolean filter flags, and label_options for parameter dropdown. Connect Tableau to this view only. Filter on apx_row_type to drive each panel section.")
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
  s.apx_row_type IN ('metric_header','bullet_build','bullet_excl','bullet_source') AS apx_is_metric,
  s.apx_row_type = 'glossary_header'                                               AS apx_is_glossary,
  CASE
    WHEN s.apx_row_type IN ('metric_header','glossary_header') THEN s.apx_label
    ELSE CAST(NULL AS STRING)
  END AS apx_label_options
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_silver_pulseTab1_appendix` s;