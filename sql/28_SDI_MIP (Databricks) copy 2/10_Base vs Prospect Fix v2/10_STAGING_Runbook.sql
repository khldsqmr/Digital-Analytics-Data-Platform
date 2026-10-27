-- OPERATOR RUNBOOK. MANUAL EXECUTION, ONE CALL AT A TIME; inspect every result.
-- RECOMMENDED: stop conflicting staging jobs and run a staging-only test.
-- Step 1: run 00_CreateStagingClones.sql (once), then deploy the six staging procedures.
-- Step 2: record predeployment Delta versions of all six production tables.
-- Step 3: run the latest week preflight. These do not execute the actual transforms.

-- latest-week PREFLIGHT: 2026-10-17 / 1 week(s); verify results before next section.
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE);

-- latest-week REBUILD: 2026-10-17 / 1 week(s); verify results before next section.
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long_stg(p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE);

-- older-five PREFLIGHT: 2026-10-10 / 5 week(s); verify results before next section.
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE);

-- older-five REBUILD: 2026-10-10 / 5 week(s); verify results before next section.
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long_stg(p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE);

-- Step 4: run 20_StagingQualityChecks.sql; rerun the latest week to test idempotence.
-- Step 5: reconcile staging filter=All vs the original production filter=All.
-- Step 6: only after runtime acceptance: plan production procedure deployment and
--         a six-table controlled cutover. Six Delta table writes are NOT atomic together.
-- DO NOT promote a staged table by renaming while active APIs reference the old table.
-- DO NOT run the production procedure CALLs until you have a cutover plan.
