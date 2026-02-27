CREATE OR REPLACE PROCEDURE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_master_orchestration`()
BEGIN

    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_visibility_asset`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_visibility_tag`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_visibility_topic`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_visibility_topic_tag`();

    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_citations_domain`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_citations_tag`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_citations_topic`();
    CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_profound_bronze_merge_citations_topic_tag`();

END;