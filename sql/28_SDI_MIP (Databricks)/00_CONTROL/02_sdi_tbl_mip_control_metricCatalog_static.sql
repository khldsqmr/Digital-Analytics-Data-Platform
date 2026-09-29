
-- ###########################################################################
-- BEGIN control/02_sdi_tbl_mip_control_metricCatalog_static.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 02_sdi_tbl_mip_control_metricCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Metric labels, count/ratio metadata, display behavior and funnel/overview flags.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Metric catalog
-- Gold stores ingredients. Views/API calculate final value / % / pp.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_control_metricCatalog_static (
  metricName             STRING,
  metricLabel            STRING,
  metricDescription      STRING,
  metricKind             STRING COMMENT 'count | ratio',
  numeratorMetric        STRING COMMENT 'for ratio metrics only',
  denominatorMetric      STRING COMMENT 'for ratio metrics only',
  displayFormat          STRING COMMENT 'number | percent',
  changeUnit             STRING COMMENT 'pct | pp',
  showOnOverview         BOOLEAN,
  showOnFunnel           BOOLEAN,
  hasForecast            BOOLEAN,
  definitionStatus       STRING COMMENT 'final | proposed',
  isActive               BOOLEAN,
  sortOrder              INT,
  notes                  STRING
)
USING DELTA
COMMENT 'Control: metric definitions and UI metadata; no data classification rules.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_control_metricCatalog_static()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  INSERT OVERWRITE sdi_tbl_mip_control_metricCatalog_static
  SELECT * FROM VALUES
    ('nbv', 'NB Visitors',
     'Unique visitors with at least one non-bounced session (2+ real page views) in the week.',
     'count', NULL, NULL, 'number', 'pct', true, false, true, 'final', true, 10, 'Prototype may label this Total UPV.'),

    ('pageViews', 'Page Views',
     'Total page-view events from non-bounced sessions.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 20, ''),

    ('nbvBuyFlow', 'NBV Buy Flow',
     'Unique NB visitors with qualifying flow_name activity after the documented exclusion list.',
     'count', NULL, NULL, 'number', 'pct', true, true, false, 'final', true, 30, ''),

    ('orders', 'Orders',
     'Unique NB visitors with at least one event_purchase = 1.',
     'count', NULL, NULL, 'number', 'pct', true, true, false, 'final', true, 40, ''),

    ('ordersUnassisted', 'Unassisted Orders',
     'Unique ordering visitors with no assisted purchase event in the week.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 50, ''),

    ('ordersAssisted', 'Assisted Orders',
     'Unique ordering visitors with at least one assisted purchase event in the week.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 60, ''),

    ('vrCalls', 'VR Call',
     'Unique NB visitors with event_click_to_call = 1.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 70, ''),

    ('vrChats', 'VR Chat',
     'Unique NB visitors with event_chat_engage = 1.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 80, ''),

    ('storeLocator', 'Store Locator',
     'Unique NB visitors with event_store_search = 1.',
     'count', NULL, NULL, 'number', 'pct', true, false, false, 'final', true, 90, ''),

    ('nbvConfigure', 'NBV Configure',
     'Unique NB visitors with event_cart_add > 0. Funnel definition remains proposed.',
     'count', NULL, NULL, 'number', 'pct', false, true, false, 'proposed', true, 100, ''),

    ('nbvCheckoutStart', 'NBV Checkout Start',
     'Unique NB visitors with event_cart_checkout > 0. Funnel definition remains proposed.',
     'count', NULL, NULL, 'number', 'pct', false, true, false, 'proposed', true, 110, ''),

    ('ordersAcquisition', 'Orders Acquisition',
     'Unique ordering visitors with at least one order placed while customer_type = Prospect.',
     'count', NULL, NULL, 'number', 'pct', false, false, false, 'final', true, 120, ''),

    ('ordersBase', 'Orders Base',
     'Unique ordering visitors without a Prospect order in the week.',
     'count', NULL, NULL, 'number', 'pct', false, false, false, 'final', true, 130, ''),

    ('orderCount', 'Order Count',
     'Raw count of event_purchase events in non-bounced sessions.',
     'count', NULL, NULL, 'number', 'pct', false, false, false, 'final', true, 140, ''),

    ('sessionCount', 'Sessions',
     'Count of non-bounced sessions.',
     'count', NULL, NULL, 'number', 'pct', false, false, false, 'final', true, 150, ''),

    ('nbvBuyFlowPerNbv', 'NBV Buy Flow / NBV',
     'Share of NB visitors entering buy flow.',
     'ratio', 'nbvBuyFlow', 'nbv', 'percent', 'pp', true, true, false, 'final', true, 200, ''),

    ('ordersPerNbvBuyFlow', 'Orders / NBV Buy Flow',
     'Share of buy-flow visitors who ordered.',
     'ratio', 'orders', 'nbvBuyFlow', 'percent', 'pp', true, true, false, 'final', true, 210, ''),

    ('ordersPerNbv', 'Orders / NBV',
     'Share of NB visitors who ordered.',
     'ratio', 'orders', 'nbv', 'percent', 'pp', true, false, false, 'final', true, 220, ''),

    ('buyFlowToConfigureRate', 'Buy flow → Configure',
     'Funnel conversion from NBV Buy Flow to NBV Configure.',
     'ratio', 'nbvConfigure', 'nbvBuyFlow', 'percent', 'pp', false, true, false, 'proposed', true, 300, ''),

    ('configureToCheckoutRate', 'Configure → Checkout start',
     'Funnel conversion from NBV Configure to NBV Checkout Start.',
     'ratio', 'nbvCheckoutStart', 'nbvConfigure', 'percent', 'pp', false, true, false, 'proposed', true, 310, ''),

    ('checkoutToOrderRate', 'Checkout start → Order',
     'Funnel conversion from NBV Checkout Start to Orders.',
     'ratio', 'orders', 'nbvCheckoutStart', 'percent', 'pp', false, true, false, 'proposed', true, 320, '')
  AS t(
    metricName, metricLabel, metricDescription, metricKind,
    numeratorMetric, denominatorMetric, displayFormat, changeUnit,
    showOnOverview, showOnFunnel, hasForecast,
    definitionStatus, isActive, sortOrder, notes
  );
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END control/02_sdi_tbl_mip_control_metricCatalog_static.sql
-- ###########################################################################

