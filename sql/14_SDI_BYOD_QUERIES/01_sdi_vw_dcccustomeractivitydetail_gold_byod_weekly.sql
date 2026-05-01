--select * from prdrzranalytics.lab42.sdi_vw_dcccustomeractivitydetail_gold_byod_weekly;

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_dcccustomeractivitydetail_gold_byod_weekly

COMMENT 'Gold layer weekly BYOD metrics for all LOBs and Brands.
Wide format — one row per Week_Ending_Sat + LOB + Brand.
Source: prdrzranalytics.lab42.fact_cdh_dcccustomeractivitydetail
LOB and Brand derived from dim_cdh_lob_brand — same logic as cdh_dcc_actuals_postpaid_broadband.
Week logic: week ending Saturday with quarter-end date splits.
Product type priority: Voice > BTS > IOT > Other (mutually exclusive).
Scope: Consumer Postpaid + Broadband (T-Mobile) only for SDI BYOD dashboard.
Created: 2026-04-26.'

AS

WITH

-- ============================================================
-- STEP 1: Derive QGP Week from daily ActivityDate
-- ============================================================
week_spine AS (
    SELECT
        ActivityDate,
        CASE QUARTER(ActivityDate)
            WHEN 1 THEN DATE(CONCAT(YEAR(ActivityDate), '-03-31'))
            WHEN 2 THEN DATE(CONCAT(YEAR(ActivityDate), '-06-30'))
            WHEN 3 THEN DATE(CONCAT(YEAR(ActivityDate), '-09-30'))
            WHEN 4 THEN DATE(CONCAT(YEAR(ActivityDate), '-12-31'))
        END                                         AS quarter_end_date,

        CASE
            WHEN DATE_ADD(ActivityDate, 7 - DAYOFWEEK(ActivityDate))
                <=
                CASE QUARTER(ActivityDate)
                    WHEN 1 THEN DATE(CONCAT(YEAR(ActivityDate), '-03-31'))
                    WHEN 2 THEN DATE(CONCAT(YEAR(ActivityDate), '-06-30'))
                    WHEN 3 THEN DATE(CONCAT(YEAR(ActivityDate), '-09-30'))
                    WHEN 4 THEN DATE(CONCAT(YEAR(ActivityDate), '-12-31'))
                END
            THEN DATE_ADD(ActivityDate, 7 - DAYOFWEEK(ActivityDate))
            ELSE
                CASE QUARTER(ActivityDate)
                    WHEN 1 THEN DATE(CONCAT(YEAR(ActivityDate), '-03-31'))
                    WHEN 2 THEN DATE(CONCAT(YEAR(ActivityDate), '-06-30'))
                    WHEN 3 THEN DATE(CONCAT(YEAR(ActivityDate), '-09-30'))
                    WHEN 4 THEN DATE(CONCAT(YEAR(ActivityDate), '-12-31'))
                END
        END                                         AS QGP_Date

    FROM prdrzranalytics.lab42.fact_cdh_dcccustomeractivitydetail
    GROUP BY ActivityDate
),

-- ============================================================
-- STEP 2: Join DCC data with week spine and LOB/Brand dim
-- Using same dim join logic as cdh_dcc_actuals_postpaid_broadband
-- ============================================================
base AS (
    SELECT
        ws.QGP_Date,
        lob.LOB,
        lob.Brand,

        dcc.isVoice,
        dcc.isBTS,
        dcc.isHSI,
        dcc.isIOT,
        dcc.isBYOD,
        dcc.EventType,
        dcc.DccExpected,
        dcc.TotalLines,
        dcc.ChargedCount,
        dcc.Revenue_Charged_ASC

    FROM prdrzranalytics.lab42.fact_cdh_dcccustomeractivitydetail dcc
    INNER JOIN week_spine ws
        ON ws.ActivityDate = dcc.ActivityDate
    LEFT JOIN prdrzranalytics.lab42.dim_cdh_lob_brand lob
        ON lob.Raw_Value = CONCAT_WS('|', dcc.isPostpaid, dcc.isHSI, dcc.isBTS, dcc.isConsumer)
        AND lob.table = 'prdrzranalytics.lab42.fact_cdh_dcccustomeractivitydetail'

    WHERE dcc.isAct = 1
    -- Scope to Consumer Postpaid + Broadband T-Mobile only
    AND lob.Brand = 'T-Mobile'
    AND lob.LOB IN ('Consumer Postpaid', 'Broadband')
),

-- ============================================================
-- STEP 3: Aggregate to weekly grain per LOB + Brand
-- ============================================================
weekly_aggregated AS (
    SELECT
        QGP_Date,
        LOB,
        Brand,

        -- ── Total Activation Volumes ─────────────────────────
        SUM(TotalLines)                             AS total_acts,
        SUM(CASE WHEN isBYOD = 1
            THEN TotalLines ELSE 0 END)             AS byod_acts,
        SUM(CASE WHEN isBYOD = 0
            THEN TotalLines ELSE 0 END)             AS non_byod_acts,
        ROUND(
            SUM(CASE WHEN isBYOD = 1
                THEN TotalLines ELSE 0 END) * 1.0 /
            NULLIF(SUM(TotalLines), 0)
        , 4)                                        AS byod_mix_pct,

        -- ── DCC Revenue and Attach Metrics ───────────────────
        SUM(CASE WHEN isBYOD = 1
            THEN Revenue_Charged_ASC ELSE 0 END)    AS byod_dcc_revenue,
        SUM(CASE WHEN isBYOD = 1
            THEN ChargedCount ELSE 0 END)           AS byod_dcc_charged_count,
        ROUND(
            SUM(CASE WHEN isBYOD = 1
                THEN ChargedCount ELSE 0 END) * 1.0 /
            NULLIF(SUM(CASE WHEN isBYOD = 1
                THEN TotalLines ELSE 0 END), 0)
        , 4)                                        AS byod_dcc_attach_rate,
        SUM(CASE WHEN isBYOD = 1
            AND DccExpected = 1
            THEN TotalLines ELSE 0 END)             AS byod_dcc_expected_lines,
        SUM(CASE WHEN isBYOD = 1
            AND DccExpected = 1
            AND ChargedCount = 0
            THEN TotalLines ELSE 0 END)             AS byod_dcc_missed_lines,

        -- ── Postpaid Product Type Breakdown ──────────────────
        -- Priority: Voice > BTS > IOT > Other
        -- Only meaningful for Consumer Postpaid rows
        SUM(CASE WHEN isBYOD = 1
            AND isVoice = 1
            AND isHSI = 0
            THEN TotalLines ELSE 0 END)             AS byod_acts_voice,
        SUM(CASE WHEN isBYOD = 1
            AND isBTS = 1
            AND isVoice = 0
            AND isHSI = 0
            THEN TotalLines ELSE 0 END)             AS byod_acts_bts,
        SUM(CASE WHEN isBYOD = 1
            AND isIOT = 1
            AND isVoice = 0
            AND isBTS = 0
            AND isHSI = 0
            THEN TotalLines ELSE 0 END)             AS byod_acts_iot,
        SUM(CASE WHEN isBYOD = 1
            AND isVoice = 0
            AND isBTS = 0
            AND isIOT = 0
            AND isHSI = 0
            THEN TotalLines ELSE 0 END)             AS byod_acts_other,

        -- ── Broadband Product Type ───────────────────────────
        -- Only meaningful for Broadband rows
        SUM(CASE WHEN isBYOD = 1
            AND isHSI = 1
            AND isIOT = 0
            THEN TotalLines ELSE 0 END)             AS byod_acts_broadband_hsi,
        SUM(CASE WHEN isBYOD = 1
            AND isHSI = 1
            AND isIOT = 1
            THEN TotalLines ELSE 0 END)             AS byod_acts_broadband_hsi_iot,

        -- ── DCC Revenue by Product Type ──────────────────────
        SUM(CASE WHEN isBYOD = 1
            AND isVoice = 1
            AND isHSI = 0
            THEN Revenue_Charged_ASC ELSE 0 END)    AS byod_dcc_revenue_voice,
        SUM(CASE WHEN isBYOD = 1
            AND isBTS = 1
            AND isVoice = 0
            AND isHSI = 0
            THEN Revenue_Charged_ASC ELSE 0 END)    AS byod_dcc_revenue_bts,
        SUM(CASE WHEN isBYOD = 1
            AND isIOT = 1
            AND isVoice = 0
            AND isBTS = 0
            AND isHSI = 0
            THEN Revenue_Charged_ASC ELSE 0 END)    AS byod_dcc_revenue_iot,
        SUM(CASE WHEN isBYOD = 1
            AND isHSI = 1
            THEN Revenue_Charged_ASC ELSE 0 END)    AS byod_dcc_revenue_broadband,

        -- ── Event Type Breakdown ─────────────────────────────
        SUM(CASE WHEN isBYOD = 1
            AND EventType = 'ACT'
            THEN TotalLines ELSE 0 END)             AS byod_new_activations,
        SUM(CASE WHEN isBYOD = 1
            AND EventType = 'REACT'
            THEN TotalLines ELSE 0 END)             AS byod_reactivations

    FROM base
    WHERE LOB IS NOT NULL
    AND Brand IS NOT NULL
    GROUP BY QGP_Date, LOB, Brand
)

-- ============================================================
-- FINAL SELECT
-- ============================================================
SELECT
    QGP_Date                                 AS QGP_Date,
    LOB,
    Brand,
    total_acts                                      AS Total_Acts,
    byod_acts                                       AS BYOD_Acts,
    non_byod_acts                                   AS Non_BYOD_Acts,
    byod_mix_pct                                    AS BYOD_Mix_Pct,
    byod_dcc_revenue                                AS BYOD_DCC_Revenue,
    byod_dcc_charged_count                          AS BYOD_DCC_Charged_Count,
    byod_dcc_attach_rate                            AS BYOD_DCC_Attach_Rate,
    byod_dcc_expected_lines                         AS BYOD_DCC_Expected_Lines,
    byod_dcc_missed_lines                           AS BYOD_DCC_Missed_Lines,
    byod_acts_voice                                 AS BYOD_Acts_Voice,
    byod_acts_bts                                   AS BYOD_Acts_BTS,
    byod_acts_iot                                   AS BYOD_Acts_IOT,
    byod_acts_other                                 AS BYOD_Acts_Other,
    byod_acts_broadband_hsi                         AS BYOD_Acts_Broadband_HSI,
    byod_acts_broadband_hsi_iot                     AS BYOD_Acts_Broadband_HSI_IOT,
    byod_dcc_revenue_voice                          AS BYOD_DCC_Revenue_Voice,
    byod_dcc_revenue_bts                            AS BYOD_DCC_Revenue_BTS,
    byod_dcc_revenue_iot                            AS BYOD_DCC_Revenue_IOT,
    byod_dcc_revenue_broadband                      AS BYOD_DCC_Revenue_Broadband,
    byod_new_activations                            AS BYOD_New_Activations,
    byod_reactivations                              AS BYOD_Reactivations,
    CURRENT_TIMESTAMP()                             AS Last_Updated

FROM weekly_aggregated
WHERE QGP_Date IS NOT NULL
ORDER BY QGP_Date ASC, LOB ASC, Brand ASC;