-- =============================================
-- BRONZE: Channel Group Mapping Reference (Wide)
-- Readable reference table showing every
-- Channel + Tactic combination and its
-- Channel Group mapping with reasoning.
-- Scoped to pulseMFC pipeline filters.
-- =============================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_pulseMFC_bronze_channelMapping_wide AS

SELECT
  UPPER(TRIM(Channel))  AS Channel,
  UPPER(TRIM(Tactic))   AS Tactic,
  CASE
    WHEN UPPER(TRIM(Channel)) = 'PAID SEARCH'
      THEN 'Paid Search'
    WHEN UPPER(TRIM(Channel)) = 'PAID SOCIAL'
      THEN 'Paid Social'
    WHEN UPPER(TRIM(Channel)) IN ('DISPLAY', 'OLV', 'AUDIO')
      THEN 'Programmatic'
    WHEN UPPER(TRIM(Channel)) = 'OTT'
      AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'Programmatic'
    WHEN UPPER(TRIM(Channel)) = 'OOH'
      AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'Programmatic'
    ELSE 'Other'
  END AS Channel_Group,

  CASE
    WHEN UPPER(TRIM(Channel)) = 'PAID SEARCH'
      THEN 'Direct match to Paid Search group'
    WHEN UPPER(TRIM(Channel)) = 'PAID SOCIAL'
      THEN 'Direct match to Paid Social group'
    WHEN UPPER(TRIM(Channel)) IN ('DISPLAY', 'OLV', 'AUDIO')
      THEN 'All tactics are programmatic digital'
    WHEN UPPER(TRIM(Channel)) = 'OTT'
      AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'OTT tactic contains PROGRAMMATIC'
    WHEN UPPER(TRIM(Channel)) = 'OOH'
      AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
      THEN 'OOH tactic contains PROGRAMMATIC'
    ELSE 'Does not meet programmatic or paid search/social criteria'
  END AS Mapping_Reason

FROM (
  SELECT DISTINCT
    UPPER(TRIM(Channel)) AS Channel,
    UPPER(TRIM(Tactic))  AS Tactic
  FROM prdrzranalytics.lab42.raw_media_flowchart
  WHERE UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND')
    AND WM_V_NWM = 'Working'
    AND Channel IS NOT NULL
    AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Unallocated', 'Budget Held')
    AND Tactic IS NOT NULL
    AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
    AND UPPER(TRIM(Message)) NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
)
ORDER BY
  Channel_Group,
  Channel,
  Tactic;

