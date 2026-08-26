-- ------------------------------------------------------------
-- 2. CHANNEL MIX SUMS TO 100% PER WEEK
-- pctUvnbByodOfTotal across 6 channels must sum to 1.0
-- Denominator is now sum of channels (not allChannels)
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    ROUND(SUM(metric_value), 4)                             AS total_pct,
    CASE
        WHEN ABS(ROUND(SUM(metric_value), 4) - 1.0) < 0.0001 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE metric_name LIKE 'adobe_pctUvnbByodOfTotal_%'
GROUP BY 1
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 3. ORDERS TOTAL = UNASSISTED + ASSISTED (allChannels)
-- diff should be 0.0 every week
-- NULL total with non-null unassisted = expected behavior
-- (means assisted was NULL that week)
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'      THEN metric_value END) AS orders_total,
    MAX(CASE WHEN metric_name = 'adobe_ordersUnassistedByod_allChannels' THEN metric_value END) AS orders_unassisted,
    MAX(CASE WHEN metric_name = 'adobe_ordersAssistedByod_allChannels'   THEN metric_value END) AS orders_assisted,
    ROUND(
        MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'      THEN metric_value END)
      - MAX(CASE WHEN metric_name = 'adobe_ordersUnassistedByod_allChannels' THEN metric_value END)
      - MAX(CASE WHEN metric_name = 'adobe_ordersAssistedByod_allChannels'   THEN metric_value END)
    , 2)                                                    AS diff,
    CASE
        WHEN MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels' THEN metric_value END) IS NULL THEN 'NULL TOTAL — CHECK ASSISTED'
        WHEN ABS(ROUND(
            MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'      THEN metric_value END)
          - MAX(CASE WHEN metric_name = 'adobe_ordersUnassistedByod_allChannels' THEN metric_value END)
          - MAX(CASE WHEN metric_name = 'adobe_ordersAssistedByod_allChannels'   THEN metric_value END)
        , 2)) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE data_source = 'ADOBE'
  AND metric_name IN (
      'adobe_ordersTotalByod_allChannels',
      'adobe_ordersUnassistedByod_allChannels',
      'adobe_ordersAssistedByod_allChannels'
  )
GROUP BY 1
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 4. CVR BYOD = ordersTotalByod / uvnbByod (allChannels)
-- stored value should match recomputed value
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    ROUND(MAX(CASE WHEN metric_name = 'adobe_cvrByod_allChannels'           THEN metric_value END), 6) AS cvr_stored,
    ROUND(
        MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'     THEN metric_value END)
      / NULLIF(MAX(CASE WHEN metric_name = 'adobe_uvnbByod_allChannels'     THEN metric_value END), 0)
    , 6)                                                    AS cvr_computed,
    ROUND(
        MAX(CASE WHEN metric_name = 'adobe_cvrByod_allChannels'             THEN metric_value END)
      - MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'     THEN metric_value END)
      / NULLIF(MAX(CASE WHEN metric_name = 'adobe_uvnbByod_allChannels'     THEN metric_value END), 0)
    , 6)                                                    AS diff,
    CASE
        WHEN ABS(ROUND(
            MAX(CASE WHEN metric_name = 'adobe_cvrByod_allChannels'         THEN metric_value END)
          - MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels' THEN metric_value END)
          / NULLIF(MAX(CASE WHEN metric_name = 'adobe_uvnbByod_allChannels' THEN metric_value END), 0)
        , 6)) < 0.000001 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE data_source = 'ADOBE'
  AND metric_name IN (
      'adobe_cvrByod_allChannels',
      'adobe_ordersTotalByod_allChannels',
      'adobe_uvnbByod_allChannels'
  )
GROUP BY 1
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 5. CVR SITE = ordersTotal / uvnbTotal (allChannels)
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    ROUND(MAX(CASE WHEN metric_name = 'adobe_cvrSite_allChannels'           THEN metric_value END), 6) AS cvr_stored,
    ROUND(
        MAX(CASE WHEN metric_name = 'adobe_ordersTotal_allChannels'         THEN metric_value END)
      / NULLIF(MAX(CASE WHEN metric_name = 'adobe_uvnbTotal_allChannels'    THEN metric_value END), 0)
    , 6)                                                    AS cvr_computed,
    CASE
        WHEN ABS(ROUND(
            MAX(CASE WHEN metric_name = 'adobe_cvrSite_allChannels'         THEN metric_value END)
          - MAX(CASE WHEN metric_name = 'adobe_ordersTotal_allChannels'     THEN metric_value END)
          / NULLIF(MAX(CASE WHEN metric_name = 'adobe_uvnbTotal_allChannels' THEN metric_value END), 0)
        , 6)) < 0.000001 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE data_source = 'ADOBE'
  AND metric_name IN (
      'adobe_cvrSite_allChannels',
      'adobe_ordersTotal_allChannels',
      'adobe_uvnbTotal_allChannels'
  )
GROUP BY 1
ORDER BY 1 DESC;

-- ------------------------------------------------------------
-- 6. WOW PCT IS CORRECT (spot check on orders total)
-- diff should be 0 or near-zero (float rounding only)
-- swap metric_name to test any other metric
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    metric_value,
    metric_value_wow,
    wow_pct                                                 AS wow_pct_stored,
    ROUND((metric_value - metric_value_wow) / NULLIF(metric_value_wow, 0), 6) AS wow_pct_computed,
    ROUND(wow_pct - (metric_value - metric_value_wow) / NULLIF(metric_value_wow, 0), 6) AS diff,
    CASE
        WHEN metric_value_wow IS NULL THEN 'NO PRIOR WEEK'
        WHEN ABS(ROUND(wow_pct - (metric_value - metric_value_wow) / NULLIF(metric_value_wow, 0), 6)) < 0.000002 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE metric_name = 'adobe_ordersTotalByod_allChannels'
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 7. YOY PCT IS CORRECT (same logic, LY column)
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    metric_value,
    metric_value_ly,
    yoy_pct                                                 AS yoy_pct_stored,
    ROUND((metric_value - metric_value_ly) / NULLIF(metric_value_ly, 0), 6) AS yoy_pct_computed,
    ROUND(yoy_pct - (metric_value - metric_value_ly) / NULLIF(metric_value_ly, 0), 6) AS diff,
    CASE
        WHEN metric_value_ly IS NULL THEN 'NO LY WEEK'
        WHEN ABS(ROUND(yoy_pct - (metric_value - metric_value_ly) / NULLIF(metric_value_ly, 0), 6)) < 0.000002 THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE metric_name = 'adobe_ordersTotalByod_allChannels'
ORDER BY 1 DESC;

-- ------------------------------------------------------------
-- 8. WOW IS ACTUALLY PRIOR WEEK (not 2 weeks ago etc)
-- metric_value this week should equal metric_value_wow next week
-- any FAIL row = gap in weekly cadence
-- ------------------------------------------------------------
SELECT
    a.week_sun_to_sat,
    a.metric_value                                          AS this_week,
    b.metric_value_wow                                      AS next_week_wow,
    CASE
        WHEN b.metric_value_wow IS NULL THEN 'NO NEXT WEEK'
        WHEN a.metric_value = b.metric_value_wow THEN 'PASS'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long` a
JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long` b
  ON b.week_sun_to_sat = DATE_ADD(a.week_sun_to_sat, INTERVAL 7 DAY)
 AND b.metric_name     = a.metric_name
WHERE a.metric_name = 'adobe_ordersTotalByod_allChannels'
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 9. LY MATCHES SAME WEEK NUM (52 weeks prior)
-- confirms LY join is hitting the right week not ±1
-- ------------------------------------------------------------
SELECT
    a.week_sun_to_sat                                       AS current_week,
    b.week_sun_to_sat                                       AS ly_week,
    DATE_DIFF(a.week_sun_to_sat, b.week_sun_to_sat, DAY)    AS days_apart,
    CASE
        WHEN b.week_sun_to_sat IS NULL THEN 'NO LY WEEK'
        WHEN DATE_DIFF(a.week_sun_to_sat, b.week_sun_to_sat, DAY) IN (364, 371) THEN 'PASS'
        ELSE 'FAIL — CHECK WEEK ANCHOR'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long` a
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long` b
  ON b.metric_name   = a.metric_name
 AND b.metric_value  = a.metric_value_ly
 AND b.data_source   = a.data_source
WHERE a.metric_name      = 'adobe_ordersTotalByod_allChannels'
  AND a.metric_value_ly IS NOT NULL
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 10. NO DUPLICATE ROWS (one row per week + metric)
-- any count > 1 = serious dedup issue upstream
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    metric_name,
    COUNT(*)                                                AS row_count,
    CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL — DUPLICATE' END AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE dimension_name IS NULL   -- exclude keyword rows (rank + metric_name is the grain there)
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY 1 DESC;
-- no rows returned = all clear

-- ------------------------------------------------------------
-- 11. NULL COVERAGE — which sources have gaps and when
-- helpful for understanding data availability by source
-- ------------------------------------------------------------
SELECT
    data_source,
    COUNT(DISTINCT week_sun_to_sat)                         AS weeks_with_data,
    MIN(week_sun_to_sat)                                    AS first_week,
    MAX(week_sun_to_sat)                                    AS latest_week,
    MAX(max_data_date)                                      AS max_data_date
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
GROUP BY 1
ORDER BY 1;


-- ------------------------------------------------------------
-- 12. PROFOUND / GOFISH COMPETITOR ROWS EXIST
-- validates the Bronze asset_name dedup fix is working
-- should see tmo, verizon, AND att rows every week
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    data_source,
    SUM(CASE WHEN metric_name LIKE '%_tmo_%'     THEN 1 ELSE 0 END) AS tmo_rows,
    SUM(CASE WHEN metric_name LIKE '%_verizon_%' THEN 1 ELSE 0 END) AS verizon_rows,
    SUM(CASE WHEN metric_name LIKE '%_att_%'     THEN 1 ELSE 0 END) AS att_rows,
    CASE
        WHEN SUM(CASE WHEN metric_name LIKE '%_verizon_%' THEN 1 ELSE 0 END) = 0 THEN 'FAIL — VERIZON MISSING'
        WHEN SUM(CASE WHEN metric_name LIKE '%_att_%'     THEN 1 ELSE 0 END) = 0 THEN 'FAIL — ATT MISSING'
        ELSE 'PASS'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE data_source IN ('PROFOUND', 'GOFISH')
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- ------------------------------------------------------------
-- 13. TRENDS KEYWORDS — 5 ranks present from 2026-05-09
-- earlier weeks should have 0 keyword rows (index only)
-- ------------------------------------------------------------
SELECT
    week_sun_to_sat,
    COUNT(DISTINCT dimension_name)                          AS distinct_ranks,
    COUNT(*)                                                AS keyword_rows,
    CASE
        WHEN week_sun_to_sat >= '2026-05-09' AND COUNT(DISTINCT dimension_name) = 5 THEN 'PASS'
        WHEN week_sun_to_sat <  '2026-05-09' AND COUNT(*) = 0                        THEN 'PASS — PRE BACKFILL'
        ELSE 'FAIL'
    END                                                     AS result
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
WHERE data_source = 'TRENDS'
  AND dimension_name IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- 14. OVERALL PASS / FAIL SUMMARY
-- quick single-number health check across key validations
-- ------------------------------------------------------------
WITH channel_mix AS (
    SELECT
        week_sun_to_sat,
        CASE WHEN ABS(ROUND(SUM(metric_value), 4) - 1.0) < 0.0001 THEN 'PASS' ELSE 'FAIL' END AS result
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
    WHERE metric_name LIKE 'adobe_pctUvnbByodOfTotal_%'
    GROUP BY 1
),
orders_check AS (
    SELECT
        week_sun_to_sat,
        CASE
            WHEN MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels' THEN metric_value END) IS NULL THEN 'SKIP'
            WHEN ABS(ROUND(
                MAX(CASE WHEN metric_name = 'adobe_ordersTotalByod_allChannels'      THEN metric_value END)
              - MAX(CASE WHEN metric_name = 'adobe_ordersUnassistedByod_allChannels' THEN metric_value END)
              - MAX(CASE WHEN metric_name = 'adobe_ordersAssistedByod_allChannels'   THEN metric_value END)
            , 2)) < 0.01 THEN 'PASS'
            ELSE 'FAIL'
        END AS result
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
    WHERE data_source = 'ADOBE'
      AND metric_name IN ('adobe_ordersTotalByod_allChannels','adobe_ordersUnassistedByod_allChannels','adobe_ordersAssistedByod_allChannels')
    GROUP BY 1
)
SELECT
    'channel_mix_sums_to_100pct' AS check_name,
    COUNTIF(result = 'PASS')     AS pass_weeks,
    COUNTIF(result = 'FAIL')     AS fail_weeks
FROM channel_mix
UNION ALL
SELECT
    'orders_total_equals_sum',
    COUNTIF(result = 'PASS'),
    COUNTIF(result = 'FAIL')
FROM orders_check;