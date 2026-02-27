INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_<layer>_test_results`

SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    'sdi_profound_<layer>_<entity>_daily' AS table_name,
    '<test_name>' AS test_name,
    'HIGH' AS severity,
    expected_value,
    actual_value,
    actual_value - expected_value AS variance_value,
    CASE
        WHEN actual_value = expected_value THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    CASE
        WHEN actual_value = expected_value THEN NULL
        ELSE 'Investigate duplicate or metric drift.'
    END AS failure_reason,
    'Review merge window and dedupe logic.' AS next_step

FROM (
    SELECT
        COUNT(*) AS actual_value,
        COUNT(*) AS expected_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_<layer>_<entity>_daily`
);