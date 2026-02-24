-- BRONZE BUILD
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration`();

-- BRONZE QA
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_qa_master_orchestration`();

-- SILVER BUILD
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration`();

-- SILVER QA
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_qa_master_orchestration`();

-- GOLD BUILD
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration`();

-- GOLD QA
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_qa_master_orchestration`();

