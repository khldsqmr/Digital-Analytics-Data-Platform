MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_<entity>_daily` T
USING (

    SELECT *
    FROM (
        SELECT
            account_name,
            <entity_key_1>,
            <entity_key_2>,
            DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS event_date,
            share_of_voice,
            visibility_score,
            count,
            mentions_count,
            executions,
            filename,
            file_load_datetime,
            ROW_NUMBER() OVER (
                PARTITION BY account_name, <entity_key_1>, <entity_key_2>, date_yyyymmdd
                ORDER BY file_load_datetime DESC
            ) AS rn
        FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_<raw_table>`
        WHERE file_load_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )
    WHERE rn = 1

) S
ON
    T.account_name = S.account_name
    AND T.<entity_key_1> = S.<entity_key_1>
    AND T.<entity_key_2> = S.<entity_key_2>
    AND T.event_date = S.event_date

WHEN MATCHED THEN UPDATE SET
    share_of_voice = S.share_of_voice,
    visibility_score = S.visibility_score,
    count = S.count,
    mentions_count = S.mentions_count,
    executions = S.executions,
    filename = S.filename,
    file_load_datetime = S.file_load_datetime

WHEN NOT MATCHED THEN INSERT ROW;