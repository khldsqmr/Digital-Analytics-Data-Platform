-- =============================================================================
-- VIEW:    prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
-- LAYER:   Bronze  (v2 — adds 7 derived classification columns)
-- SOURCE:  prdrzrlakehouse.qgp_restricted.qgpweeklyview
-- AUTHOR:  SDI / Lab42
-- =============================================================================
--
-- PURPOSE
-- -------
-- Bronze (clean) layer view of QGP weekly metrics data.
-- Transformations applied:
--
--   1. Excludes layout rows (MetricID LIKE 'Header%' or 'Blank%').
--   2. Excludes 'Variance to QGP' and 'Variance to QGP %' MetricType rows.
--   3. Trims whitespace from all string columns.
--   4. Standardizes IsFuture to exactly 'Is Past' / 'Is Future'.
--   5. Deduplicates page-level row triplication (same MetricID on primary
--      page + Outcomes 1 + Outcomes 2 with identical Amount values).
--   6. Adds 7 derived classification columns — see DERIVED COLUMNS section.
--
-- GRAIN
-- -----
-- One row per: MetricID + WeekEnding + DateContext + MetricType
--
-- TABLEAU USAGE PATTERNS
-- ----------------------
-- Actuals (past periods only):
--   MetricType = 'Actuals/Outlook' AND DateContext = 'Normal' AND IsFuture = 'Is Past'
--
-- QGP / Forecast:
--   MetricType = 'QGP' AND DateContext = 'Normal'
--   (No IsFuture filter needed — dedup has already resolved page triplication.)
--
-- Pure Actuals (store traffic, BYOD%, NPS):
--   MetricType = 'Actuals' AND DateContext = 'Normal'
--   (Future weeks have null Amount for this MetricType.)
--
-- DERIVED COLUMNS
-- ---------------
-- The following classification columns are derived from MetricID and MetricName
-- patterns. They reduce the need for manual CASE logic in Tableau calculated
-- fields and enable consistent cross-metric filtering.
--
-- 1. LOB          — Line of business owning the metric.
--                   Values: Consumer | Business | Enterprise | Metro |
--                           Mint-Ultra | USCC | IT | TLife-Digital | Cross-LOB
--
-- 2. ReportChannel — Channel context derived from Page column.
--                   Values: BR-ARN | BR-Experience | BR-Neighborhood | BR-SiS |
--                           BR-Total | Virtual Retail | Digital | National Retail |
--                           Care | D2C | TFB | USCC | Cross-Channel
--
-- 3. Product       — Primary telecom product the metric covers.
--                   Values: Phone | BTS | 5G Broadband | Fiber | IoT |
--                           Mint-Ultra | T-Satellite | SyncUp | P360-VAS |
--                           Cross-Product
--
-- 4. CustomerType  — New Account vs. Add-a-Line vs. Total/Upgrade.
--                   Values: New Account | AAL | Upgrade | Total | Other
--
-- 5. MetricCategory — Functional category of the metric.
--                   Values: Activations | Disconnects | Net Adds | Net Accounts |
--                           New Accounts | Full Account Disconnects | MRC |
--                           Upgrades | Traffic | Conversion Rate | NPS |
--                           Credit Apps | Survival | Port Ratio | SoPI-SoPO |
--                           Rate Plan Mix | Credit Class Mix | Prime Mix |
--                           Spend | ARPU-ARPA | Lines per Account | BYOD |
--                           VAF-Accessory | VAS-Enrollment | Churn-Disconnect Mix |
--                           Migration | Handset Mix | Digital Mix | App-Digital |
--                           Store Ops | Care-Support | IT-Ops | Other
--
-- 6. AssistanceType — Digital assistance context.
--                   Values: No Assistance | Assisted | Total | N/A
--
-- 7. DataSource    — How the metric value is sourced/calculated.
--                   Values: Automated | Manual | TM1 Mapped | Calculated | Unknown
--
-- VALIDATION
-- ----------
-- After creating the view, validate derived columns with:
--   SELECT LOB, COUNT(DISTINCT MetricID) AS metrics
--   FROM prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
--   WHERE DateContext = 'Normal' GROUP BY LOB ORDER BY metrics DESC;
--
-- DOWNSTREAM
-- ----------
-- Silver layer will join with MFC data and qgpchanneludd to add explicit
-- LOB and Channel dimensions unavailable at metric level in this source.
--
-- =============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_prdrzrlakehouseRestricted_bronze_qgp_weekly
COMMENT 'Bronze layer view of QGP weekly metrics. Source: prdrzrlakehouse.qgp_restricted.qgpweeklyview. Deduplicates page triplication, standardizes MetricType and IsFuture, excludes layout/variance rows. Adds 7 derived classification columns (LOB, ReportChannel, Product, CustomerType, MetricCategory, AssistanceType, DataSource). Grain: MetricID + WeekEnding + DateContext + MetricType.'
AS

WITH deduped AS (

  SELECT

    -- -------------------------------------------------------------------------
    -- TIME DIMENSIONS
    -- -------------------------------------------------------------------------
    PublishKey,
    WeekEnding,
    QuarterNum,
    YearNum,
    TRIM(DateContext)       AS DateContext,
    TRIM(CumulativeDates)   AS CumulativeDates,
    DaysInArrears,

    -- -------------------------------------------------------------------------
    -- METRIC IDENTITY
    -- -------------------------------------------------------------------------
    MetricID,
    TRIM(MetricName)        AS MetricName,
    TRIM(MetricType)        AS MetricType,
    TRIM(DisplayMetricType) AS DisplayMetricType,
    TRIM(MetricFormat)      AS MetricFormat,
    MetricOrder,
    TRIM(MetricOwner)       AS MetricOwner,

    -- -------------------------------------------------------------------------
    -- REPORT STRUCTURE
    -- -------------------------------------------------------------------------
    TRIM(Page)              AS Page,

    -- -------------------------------------------------------------------------
    -- PERIOD FLAG  (standardized from 3 dirty source variants to 2)
    -- -------------------------------------------------------------------------
    CASE
      WHEN LOWER(TRIM(IsFuture)) IN ('is future', 'isfuture') THEN 'Is Future'
      WHEN LOWER(TRIM(IsFuture)) = 'is past'                  THEN 'Is Past'
      ELSE TRIM(IsFuture)
    END                     AS IsFuture,

    -- -------------------------------------------------------------------------
    -- METRIC VALUES
    -- -------------------------------------------------------------------------
    Amount,
    VariancePercentage,
    VarianceDirection,
    VarianceColor,
    LevelofPrecision,

    -- -------------------------------------------------------------------------
    -- METADATA / REFERENCE
    -- -------------------------------------------------------------------------
    DataDictionaryURL,
    DrillDownURL1,
    DrillDownURL2,
    InsertDateTime,

    -- -------------------------------------------------------------------------
    -- LINEAGE
    -- -------------------------------------------------------------------------
    'prdrzrlakehouse.qgp_restricted.qgpweeklyview' AS SourceTable,
    CURRENT_TIMESTAMP()    AS BronzeCreatedAt,

    -- -------------------------------------------------------------------------
    -- DEDUP ROW NUMBER (internal)
    -- -------------------------------------------------------------------------
    ROW_NUMBER() OVER (
      PARTITION BY
        MetricID,
        WeekEnding,
        TRIM(DateContext),
        TRIM(MetricType)
      ORDER BY TRIM(Page) ASC
    )                       AS _rn

  FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview

  WHERE
    MetricID NOT LIKE 'Header%'
    AND MetricID NOT LIKE 'Blank%'
    AND TRIM(MetricType) NOT IN ('Variance to QGP', 'Variance to QGP %')

)

-- =============================================================================
-- FINAL SELECT  — includes all base columns + 7 derived classification columns
-- =============================================================================
SELECT

  -- Base columns (same as v1)
  PublishKey,
  WeekEnding,
  QuarterNum,
  YearNum,
  DateContext,
  CumulativeDates,
  DaysInArrears,
  MetricID,
  MetricName,
  MetricType,
  DisplayMetricType,
  MetricFormat,
  MetricOrder,
  MetricOwner,
  Page,
  IsFuture,
  Amount,
  VariancePercentage,
  VarianceDirection,
  VarianceColor,
  LevelofPrecision,
  DataDictionaryURL,
  DrillDownURL1,
  DrillDownURL2,
  InsertDateTime,
  SourceTable,
  BronzeCreatedAt,

  -- ===========================================================================
  -- DERIVED COLUMN 1: LOB
  -- Logic: MetricID prefix is the primary signal.
  -- Order matters — USCC before Consumer/Business to avoid USCC metrics being
  -- misclassified. TFB (T-Mobile for Business) maps to Business.
  -- Blank-prefix Postpaid/StoreTraffic/VR/Digital/Core etc. are Cross-LOB.
  -- ===========================================================================
  CASE
    -- IT
    WHEN MetricID LIKE 'IT%'               THEN 'IT'

    -- T-Life / Digital App
    WHEN MetricID LIKE 'TLife%'            THEN 'TLife-Digital'
    WHEN MetricID LIKE 'TlifeApp%'         THEN 'TLife-Digital'

    -- USCC (must precede Consumer/Business to avoid false match)
    WHEN MetricID LIKE 'USCC%'             THEN 'USCC'
    WHEN MetricID LIKE 'ConsumerPrepaidUSCC%' THEN 'USCC'

    -- TFB / Business / Enterprise / Government / SMB
    WHEN MetricID LIKE 'TFB%'             THEN 'Business'
    WHEN MetricID LIKE 'Business%'        THEN 'Business'
    WHEN MetricID LIKE 'Enterprise%'      THEN 'Enterprise'

    -- Metro
    WHEN MetricID LIKE 'Metro%'           THEN 'Metro'
    WHEN MetricID LIKE 'ConsumerMetro%'   THEN 'Metro'
    WHEN MetricID LIKE 'MetroCare%'       THEN 'Metro'
    WHEN MetricID LIKE 'MetroVR%'         THEN 'Metro'

    -- Mint / Ultra
    WHEN MetricID LIKE 'Mint%'            THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'MINT%'            THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'Ultra%'           THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'MintUltra%'       THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'MintMobile%'      THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'UltraMobile%'     THEN 'Mint-Ultra'
    WHEN MetricID LIKE 'EnterprisePrepaidMint%' THEN 'Mint-Ultra'

    -- Consumer (catch all Consumer prefix after more specific ones above)
    WHEN MetricID LIKE 'Consumer%'        THEN 'Consumer'
    WHEN MetricID LIKE 'ConsumerBR%'      THEN 'Consumer'
    WHEN MetricID LIKE 'DigitalPctof%'    THEN 'Consumer'
    WHEN MetricID LIKE 'DigitalConsumer%' THEN 'Consumer'

    -- Cross-LOB summary / shared metrics (blank prefix Postpaid, Digital%, etc.)
    ELSE 'Cross-LOB'
  END AS LOB,

  -- ===========================================================================
  -- DERIVED COLUMN 2: ReportChannel
  -- Logic: Derived from Page column (the deduped primary page name).
  -- ===========================================================================
  CASE
    WHEN Page LIKE '%ARN%'                         THEN 'BR-ARN'
    WHEN Page LIKE '%Experience%'
         AND Page LIKE '%Retail%'                  THEN 'BR-Experience'
    WHEN Page LIKE '%Neighborhood%'                THEN 'BR-Neighborhood'
    WHEN Page LIKE '%Store-In-Store%'
         OR Page LIKE '%SiS%'                      THEN 'BR-SiS'
    WHEN Page LIKE 'Branded Retail%'
         OR Page LIKE '%BR Total%'
         OR Page LIKE 'ConsumerBR%'
         OR Page = 'Branded Retail'                THEN 'BR-Total'
    WHEN Page LIKE 'Virtual Retail%'
         OR Page LIKE '%VR%'
         OR Page LIKE 'Virtual Business%'          THEN 'Virtual Retail'
    WHEN Page LIKE 'Digital%'                      THEN 'Digital'
    WHEN Page LIKE 'National Retail%'              THEN 'National Retail'
    WHEN Page LIKE 'Care%'
         OR Page LIKE '%Care'                      THEN 'Care'
    WHEN Page LIKE 'Direct to Consumer%'
         OR Page LIKE 'D2C%'
         OR Page LIKE 'D2D%'                       THEN 'D2C'
    WHEN Page LIKE 'TFB%'
         OR Page LIKE 'Business%'                  THEN 'TFB'
    WHEN Page LIKE 'USCC%'                         THEN 'USCC'
    WHEN Page LIKE 'Metro%'                        THEN 'Metro'
    WHEN Page LIKE 'Mint%'
         OR Page LIKE 'MINT%'
         OR Page LIKE 'Ultra%'                     THEN 'Mint-Ultra'
    WHEN Page LIKE 'IT%'                           THEN 'IT'
    WHEN Page LIKE 'T-Life%'
         OR Page LIKE 'TLife%'                     THEN 'TLife'
    WHEN Page LIKE 'Enterprise%'                   THEN 'Enterprise'
    ELSE 'Cross-Channel'
  END AS ReportChannel,

  -- ===========================================================================
  -- DERIVED COLUMN 3: Product
  -- Logic: MetricID keyword matching with priority order.
  -- P360/VAS checked before Phone to avoid misfiring on PhoneP360 patterns.
  -- SyncUp checked before BTS (SyncUP metrics have BTS-like content).
  -- T-Satellite / SPACEX / T911 / ALC checked as a group.
  -- ===========================================================================
  CASE
    -- T-Satellite (SPACEX platform, ALC/Prospect/Bundled sub-products)
    WHEN MetricID LIKE '%SPACEX%'
         OR MetricID LIKE '%Satellite%'
         OR MetricID LIKE '%T911%'
         OR MetricID LIKE '%TSatellite%'           THEN 'T-Satellite'

    -- SyncUp (Drive, KidsWatch, Tracker)
    WHEN MetricID LIKE '%SyncUp%'
         OR MetricID LIKE 'SyncUpDrive%'
         OR MetricID LIKE 'SyncUpKids%'
         OR MetricID LIKE 'SyncUpTracker%'         THEN 'SyncUp'

    -- P360 / VAS (protection, insurance, international, ScamShield)
    WHEN MetricID LIKE '%P360%'
         OR MetricID LIKE '%VAS%'
         OR MetricID LIKE 'ConsumerVAS%'
         OR MetricID LIKE 'PostpaidVAS%'
         OR MetricID LIKE 'ConsumerRevenueTMOPhoneVAS' THEN 'P360-VAS'

    -- Fiber
    WHEN MetricID LIKE '%Fiber%'
         OR MetricID LIKE 'FiberNet%'
         OR MetricID LIKE 'FiberOrder%'
         OR MetricID LIKE 'TotalFiber%'
         OR MetricID LIKE 'Lumos%'
         OR MetricID LIKE 'Metronet%'
         OR MetricID LIKE 'Wholesale%'             THEN 'Fiber'

    -- IoT
    WHEN MetricID LIKE '%IOT%'
         OR MetricID LIKE '%IoT%'                  THEN 'IoT'

    -- Mint / Ultra (these are also products within Mint-Ultra LOB)
    WHEN MetricID LIKE 'Mint%'
         OR MetricID LIKE 'MINT%'
         OR MetricID LIKE 'Ultra%'
         OR MetricID LIKE 'MintMobile%'
         OR MetricID LIKE 'UltraMobile%'
         OR MetricID LIKE 'MintUltra%'
         OR MetricID LIKE 'EnterprisePrepaidMint%' THEN 'Mint-Ultra'

    -- 5G Broadband / HSI / HomeInternet
    WHEN MetricID LIKE '%HSI%'
         OR MetricID LIKE '%HomeInternet%'
         OR MetricID LIKE '%Broadband%'            THEN '5G Broadband'

    -- BTS (Business in a Box, Tablet, SyncUP, other data lines)
    WHEN MetricID LIKE '%BTS%'                     THEN 'BTS'

    -- Phone (voice lines)
    WHEN MetricID LIKE '%Phone%'                   THEN 'Phone'

    -- Everything else — legitimately cross-product (store ops, care, digital %, etc.)
    ELSE 'Cross-Product'
  END AS Product,

  -- ===========================================================================
  -- DERIVED COLUMN 4: CustomerType
  -- Logic: MetricID keyword pattern for customer segment.
  -- ===========================================================================
  CASE
    WHEN MetricID LIKE '%AAL%'
         OR MetricID LIKE '%ExistingAccount%'
         OR MetricID LIKE '%LPAL%'                 THEN 'AAL'
    WHEN MetricID LIKE '%NewAccount%'
         OR MetricID LIKE '%NewBAN%'
         OR MetricID LIKE '%NewPostpaid%'
         OR MetricID LIKE '%New_Account%'          THEN 'New Account'
    WHEN MetricID LIKE '%Upgrade%'                 THEN 'Upgrade'
    ELSE 'Total'
  END AS CustomerType,

  -- ===========================================================================
  -- DERIVED COLUMN 5: MetricCategory
  -- Logic: MetricID keyword matching with priority order.
  -- More specific categories checked before general ones.
  -- Validated against Query 1 (Product=Other) and Query 2 (Category=Other)
  -- results to ensure full coverage of previously uncategorized MetricIDs.
  -- ===========================================================================
  CASE
    -- ---- NPS (must precede Activations to avoid AALNPSfor... misfire) ----
    WHEN MetricID LIKE '%NPS%'
         OR MetricID LIKE 'AALNPSfor%'
         OR MetricID LIKE 'UpgradeNPS%'            THEN 'NPS'

    -- ---- Survival Rate ----
    WHEN MetricID LIKE '%Survival%'
         OR MetricID LIKE '%DaySurvival%'          THEN 'Survival'

    -- ---- Credit Apps ----
    WHEN MetricID LIKE '%CreditApp%'
         OR MetricID LIKE '%CreditApps%'           THEN 'Credit Apps'

    -- ---- Credit Class Mix ----
    WHEN MetricID LIKE '%CreditClass%'             THEN 'Credit Class Mix'

    -- ---- Prime Mix ----
    WHEN MetricID LIKE '%PrimeMix%'
         OR MetricID LIKE '%PrimeMixof%'           THEN 'Prime Mix'

    -- ---- Rate Plan Mix ----
    WHEN MetricID LIKE '%RatePlan%'
         OR MetricID LIKE '%RatePlanMix%'
         OR MetricID LIKE '%BestRatePlan%'
         OR MetricID LIKE '%BetterBest%'
         OR MetricID LIKE '%BetterRatePlan%'
         OR MetricID LIKE 'MixConsumerNew%'
         OR MetricID LIKE '%ActPlanMix%'
         OR MetricID LIKE '%ActivationPlanMix%'
         OR MetricID LIKE 'PostpaidHSIAct%PlanPct%'
         OR MetricID LIKE '%PlanPctOfTotal%'
         OR MetricID LIKE '%MixToEssentials%'
         OR MetricID LIKE '%MixToMore%'
         OR MetricID LIKE 'MixDigital%'
         OR MetricID LIKE '%Biller%RatePlan%'      THEN 'Rate Plan Mix'

    -- ---- Port Ratio ----
    WHEN MetricID LIKE '%PortRatio%'
         OR MetricID LIKE '%PortingRatio%'         THEN 'Port Ratio'

    -- ---- SoPI / SoPO (Share of Portfolio / Industry) ----
    WHEN MetricID LIKE '%SoPO%'
         OR MetricID LIKE '%SoPI%'
         OR MetricID LIKE '%SOPI%'
         OR MetricID LIKE '%SOPO%'                 THEN 'SoPI-SoPO'

    -- ---- ARPU / ARPA ----
    WHEN MetricID LIKE '%ARPU%'
         OR MetricID LIKE '%ARPA%'                 THEN 'ARPU-ARPA'

    -- ---- MRC ----
    WHEN MetricID LIKE '%MRC%'                     THEN 'MRC'

    -- ---- Full Account Disconnects ----
    WHEN MetricID LIKE '%FullAccountDisco%'
         OR MetricID LIKE '%FullDisconnects%'
         OR MetricID LIKE '%FullAcctDisco%'        THEN 'Full Account Disconnects'

    -- ---- Activations (must precede Net Adds to avoid double-fire) ----
    WHEN MetricID LIKE '%Acts%'
         OR MetricID LIKE '%Activations%'          THEN 'Activations'

    -- ---- Disconnects ----
    WHEN MetricID LIKE '%Discos%'
         OR MetricID LIKE '%Disconnects%'          THEN 'Disconnects'

    -- ---- Net Adds ----
    WHEN MetricID LIKE '%NetAdds%'                 THEN 'Net Adds'

    -- ---- Net Accounts ----
    WHEN MetricID LIKE '%NetAccount%'              THEN 'Net Accounts'

    -- ---- New Accounts ----
    WHEN MetricID LIKE '%NewAccount%'              THEN 'New Accounts'

    -- ---- Nets (catch-all for lines like MetroPrepaidNetPhone) ----
    WHEN MetricID LIKE '%Nets%'
         AND MetricID NOT LIKE '%NetAdds%'
         AND MetricID NOT LIKE '%NetAccount%'      THEN 'Net Adds'

    -- ---- Upgrades ----
    WHEN MetricID LIKE '%Upgrade%'                 THEN 'Upgrades'

    -- ---- Traffic (store foot traffic) ----
    WHEN MetricID LIKE '%StoreTraffic%'
         OR MetricID LIKE '%SameStore%'
         OR MetricID LIKE 'StoreTraffic%'
         OR MetricID LIKE 'SAMEStore%'             THEN 'Traffic'

    -- ---- Conversion Rate ----
    WHEN MetricID LIKE '%ConversionRate%'
         OR MetricID LIKE '%Conv%Rate%'
         OR MetricID LIKE '%TrafficConv%'
         OR MetricID LIKE '%Conversion%'           THEN 'Conversion Rate'

    -- ---- BYOD ----
    WHEN MetricID LIKE '%BYOD%'                    THEN 'BYOD'

    -- ---- Lines per Account ----
    WHEN MetricID LIKE '%LinesperNew%'
         OR MetricID LIKE '%LinesperAccount%'
         OR MetricID LIKE '%LPB%'                  THEN 'Lines per Account'

    -- ---- Spend (media, promo) ----
    WHEN MetricID LIKE '%Spend%'                   THEN 'Spend'

    -- ---- VAS / Subscription Enrollments (P360, insurance, international) ----
    WHEN MetricID LIKE '%Enrollment%'
         OR MetricID LIKE '%Enrollments%'
         OR MetricID LIKE 'VAS%'
         OR MetricID LIKE 'ConsumerVAS%'
         OR MetricID LIKE 'PostpaidVAS%'           THEN 'VAS-Enrollment'

    -- ---- Account Churn / Disconnect Mix ----
    WHEN MetricID LIKE '%AccountChurn%'
         OR MetricID LIKE 'PostpaidAccounts%Churn%'
         OR MetricID LIKE '%DisconnectMix%'
         OR MetricID LIKE '%TenureMix%'
         OR MetricID LIKE '%ChurnDeact%'
         OR MetricID LIKE '%TotalChurnPct%'
         OR MetricID LIKE '%ChurnPct%'             THEN 'Churn-Disconnect Mix'

    -- ---- Migration ----
    WHEN MetricID LIKE '%Migration%'
         OR MetricID LIKE '%Migrations%'
         OR MetricID LIKE 'PreToPost%'
         OR MetricID LIKE 'PostToPre%'
         OR MetricID LIKE 'NETMigrations%'
         OR MetricID LIKE 'MintUltra%Migration%'   THEN 'Migration'

    -- ---- Handset Mix / Device Share ----
    WHEN MetricID LIKE '%ShareofPostpaid%'
         OR MetricID LIKE '%ShareofPrepaid%'
         OR MetricID LIKE 'REVVL%'
         OR MetricID LIKE '%DeviceSalesVolume%'
         OR MetricID LIKE '%HandsetOrders%'
         OR MetricID LIKE '%HandsetSubsidy%'
         OR MetricID LIKE '%TradeIn%'
         OR MetricID LIKE '%TabletOrders%'
         OR MetricID LIKE '%WatchOrders%'          THEN 'Handset Mix'

    -- ---- Digital Mix (% of activations / upgrades that are digital/unassisted) ----
    WHEN MetricID LIKE 'DigitalPct%'
         OR MetricID LIKE '%DigitalPct%'
         OR MetricID LIKE '%PctofAAL%'
         OR MetricID LIKE '%PctofNew%Digital%'
         OR MetricID LIKE '%UnassistedMix%'
         OR MetricID LIKE '%NonAssistDigitalMix%'
         OR MetricID LIKE 'BusinessTotalPostpaidActivations%Mix' THEN 'Digital Mix'

    -- ---- VAF / Accessory ----
    WHEN MetricID LIKE '%VAF%'
         OR MetricID LIKE '%Accessory%'
         OR MetricID LIKE '%AccessoryRevenue%'
         OR MetricID LIKE '%AccessoryMargin%'
         OR MetricID LIKE '%AccessoryAttach%'      THEN 'VAF-Accessory'

    -- ---- App / Digital (T-Life app metrics, WAU, downloads) ----
    WHEN MetricID LIKE 'TLife%'
         OR MetricID LIKE 'TlifeApp%'              THEN 'App-Digital'

    -- ---- Store Ops (store count, PSA, DCC compliance, wait time) ----
    WHEN MetricID LIKE '%StoreCounts%'
         OR MetricID LIKE '%StoreCount%'
         OR MetricID LIKE '%PSA%'
         OR MetricID LIKE '%DCC%'
         OR MetricID LIKE '%AverageWaitTime%'
         OR MetricID LIKE '%SalesPerLabor%'
         OR MetricID LIKE '%AccountInteractions%'
         OR MetricID LIKE '%Interactions%'
         OR MetricID LIKE 'FrontlineARN%'
         OR MetricID LIKE 'FrontlineAll%'
         OR MetricID LIKE 'FrontlineExp%'
         OR MetricID LIKE 'FrontlineNeighb%'
         OR MetricID LIKE 'FrontlineSiS%'          THEN 'Store Ops'

    -- ---- Care / Support ----
    WHEN MetricID LIKE '%CallsOffered%'
         OR MetricID LIKE '%CallsHandled%'
         OR MetricID LIKE '%CallsperSubscriber%'
         OR MetricID LIKE '%CallsperCustomer%'
         OR MetricID LIKE '%ContactsOffered%'
         OR MetricID LIKE '%ContactsAbandoned%'
         OR MetricID LIKE '%ContactsperAccount%'
         OR MetricID LIKE '%FirstContact%'
         OR MetricID LIKE '%FirstCall%'
         OR MetricID LIKE '%CallResolutionTime%'
         OR MetricID LIKE '%SpeedofAnswer%'
         OR MetricID LIKE '%ManualCA%'
         OR MetricID LIKE '%CareRatePlan%'
         OR MetricID LIKE '%CareStayConnected%'
         OR MetricID LIKE '%CareUpperFunnel%'
         OR MetricID LIKE '%CareVolNonPort%'
         OR MetricID LIKE '%CareNetPromoter%'
         OR MetricID LIKE '%CareCallers%'
         OR MetricID LIKE '%ExpertOccupancy%'
         OR MetricID LIKE '%InternalCallMix%'
         OR MetricID LIKE 'TFBCare%'              THEN 'Care-Support'

    -- ---- IT Operations ----
    WHEN MetricID LIKE 'IT%'                       THEN 'IT-Ops'

    -- ---- Port In Volume (industry-level) ----
    WHEN MetricID LIKE 'Industry%Port%'            THEN 'Port Ratio'

    -- ---- Non-Port / Vol Disconnects (operational) ----
    WHEN MetricID LIKE '%NonPort%'
         OR MetricID LIKE '%VolNonPort%'
         OR MetricID LIKE '%OperationalNonPort%'   THEN 'Churn-Disconnect Mix'

    -- ---- Fiber Household / Orders ----
    WHEN MetricID LIKE 'FiberNet%'
         OR MetricID LIKE 'FiberOrder%'
         OR MetricID LIKE '%HomesPassed%'
         OR MetricID LIKE '%Sellable%'             THEN 'Activations'

    -- ---- Remaining ----
    ELSE 'Other'
  END AS MetricCategory,

  -- ===========================================================================
  -- DERIVED COLUMN 6: AssistanceType
  -- Logic: MetricID keyword pattern.
  -- 'No Assistance' / 'Unassisted' -> No Assistance
  -- 'Assisted' (without 'Unassisted') -> Assisted
  -- If neither keyword present -> Total (the default aggregated view)
  -- ===========================================================================
  CASE
    WHEN MetricID LIKE '%Unassisted%'
         OR MetricID LIKE '%NoAssist%'
         OR MetricID LIKE '%NonAssist%'
         OR MetricID LIKE '%NOAssist%'
         OR MetricID LIKE '%BOPIS%'               THEN 'No Assistance'
    WHEN MetricID LIKE '%Assisted%'
         AND MetricID NOT LIKE '%Unassisted%'
         AND MetricID NOT LIKE '%NonAssist%'       THEN 'Assisted'
    WHEN MetricID LIKE '%AssistedandUnassisted%'
         OR MetricID LIKE '%UnassistedANDAssisted%'
         OR MetricID LIKE '%UnassistedPlusAssist%' THEN 'Total'
    ELSE 'Total'
  END AS AssistanceType,

  -- ===========================================================================
  -- DERIVED COLUMN 7: DataSource
  -- Logic: MetricID suffix keyword pattern.
  -- Source indicators in MetricID:
  --   'Automated' / 'AUTO'  -> Automated
  --   'Manual'              -> Manual
  --   'TM1Mapped' / 'DB'    -> TM1 Mapped
  --   'Calc' / 'Calculated' -> Calculated
  --   None of the above     -> Unknown
  -- ===========================================================================
  CASE
    WHEN MetricID LIKE '%Automated%'
         OR MetricID LIKE '%AUTO%'                THEN 'Automated'
    WHEN MetricID LIKE '%Manual%'                 THEN 'Manual'
    WHEN MetricID LIKE '%TM1Mapped%'
         OR MetricID LIKE '%TM1Map%'
         OR MetricID LIKE '%DB%'                  THEN 'TM1 Mapped'
    WHEN MetricID LIKE '%Calc%'
         OR MetricID LIKE '%Calculated%'          THEN 'Calculated'
    ELSE 'Unknown'
  END AS DataSource

FROM deduped
WHERE _rn = 1;  -- Keep only the primary page row per grain (dedup applied).