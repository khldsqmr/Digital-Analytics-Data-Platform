WITH qgp_daily AS (
  SELECT
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
    SUM(COALESCE(cost, 0)) AS daily_cost,
    SUM(COALESCE(cart_start, 0)) AS daily_cart_start,
    SUM(COALESCE(postpaid_pspv, 0)) AS daily_postpaid_pspv
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
  WHERE date IS NOT NULL
  GROUP BY 1
),

qgp_weekly_wide AS (
  SELECT
    qgp_week,
    SUM(COALESCE(cost, 0)) AS wide_cost,
    SUM(COALESCE(cart_start, 0)) AS wide_cart_start,
    SUM(COALESCE(postpaid_pspv, 0)) AS wide_postpaid_pspv
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
  GROUP BY 1
),

qgp_weekly_long AS (
  SELECT
    qgp_week,
    SUM(CASE WHEN metric_name = 'cost' THEN COALESCE(metric_value, 0) ELSE 0 END) AS long_cost,
    SUM(CASE WHEN metric_name = 'cart_start' THEN COALESCE(metric_value, 0) ELSE 0 END) AS long_cart_start,
    SUM(CASE WHEN metric_name = 'postpaid_pspv' THEN COALESCE(metric_value, 0) ELSE 0 END) AS long_postpaid_pspv
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
  WHERE metric_name IN ('cost', 'cart_start', 'postpaid_pspv')
  GROUP BY 1
)

SELECT
  q.qgp_week,

  -- Daily bucketed
  d.daily_cost,
  d.daily_cart_start,
  d.daily_postpaid_pspv,

  -- Weekly wide
  w.wide_cost,
  w.wide_cart_start,
  w.wide_postpaid_pspv,

  -- Weekly long
  l.long_cost,
  l.long_cart_start,
  l.long_postpaid_pspv,

  -- Diffs (Wide vs Daily)
  (w.wide_cost - d.daily_cost) AS diff_wide_vs_daily_cost,
  (w.wide_cart_start - d.daily_cart_start) AS diff_wide_vs_daily_cart_start,
  (w.wide_postpaid_pspv - d.daily_postpaid_pspv) AS diff_wide_vs_daily_postpaid_pspv,

  -- Diffs (Long vs Wide)
  (l.long_cost - w.wide_cost) AS diff_long_vs_wide_cost,
  (l.long_cart_start - w.wide_cart_start) AS diff_long_vs_wide_cart_start,
  (l.long_postpaid_pspv - w.wide_postpaid_pspv) AS diff_long_vs_wide_postpaid_pspv

FROM (
  SELECT DISTINCT qgp_week
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
  WHERE qgp_week <= CURRENT_DATE()
) q
LEFT JOIN qgp_daily d USING (qgp_week)
LEFT JOIN qgp_weekly_wide w USING (qgp_week)
LEFT JOIN qgp_weekly_long l USING (qgp_week)
ORDER BY q.qgp_week DESC;