-- ============================================================================
-- FILE  : 05_sdi_vw_mip_control_validationRules_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Validation thresholds, criticality, ownership and next-step guidance.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

CREATE OR REPLACE VIEW sdi_vw_mip_control_validationRules_static
COMMENT 'Control view: validation thresholds and next-step guidance; validation results live in Validation layer.'
AS

SELECT *
FROM VALUES
    ('detailsVsBronzeRowCountPctDiff', 'MAX_ALLOWED', 0.000001D, 0.001D, true,
     'MIP Data Engineering',
     'Inspect the rewritten date partition.',
     'Stop downstream processing and reconcile Bronze hits to detailsPerHit.',
     true, ''),

    ('sessionizationCoveragePct', 'MIN_REQUIRED', 0.97D, 0.90D, false,
     'EDL Engineering',
     'Review latest SESSION_EVENT_FACT refresh and known unmatched-hit issue.',
     'Escalate severe loss of session assignment before relying on session metrics.',
     true, 'Known upstream gap; warning is informational unless orchestration chooses strict mode.'),

    ('unmatchedPurchaseEvents', 'MAX_ALLOWED', 1D, 10000D, false,
     'EDL Engineering',
     'Inspect purchase hits where isSessionized = 0.',
     'Escalate major purchase-sessionization loss.',
     true, 'Known upstream issue.'),

    ('invalidSessionRows', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Review non-bounced/session-status filters.',
     'Stop pipeline and correct the session Silver logic.',
     true, ''),

    ('duplicateSessionKeys', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Inspect duplicate session IDs.',
     'Stop pipeline and resolve duplicate session grain.',
     true, ''),

    ('duplicateSessionPageCategoryKeys', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Inspect duplicate session × page-category keys.',
     'Stop pipeline and resolve duplicate action grain.',
     true, ''),

    ('visitorWeekPairMismatch', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Compare attributesPerVisitorWeek and actionsPerVisitorWeek keys.',
     'Stop pipeline and restore 1:1 visitor-week coverage.',
     true, ''),

    ('visitorWeekOrderSplitViolations', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Review weekly assisted/unassisted and acquisition/base flags.',
     'Stop pipeline and correct metric split logic.',
     true, ''),

    ('goldOverviewReconciliationPctDiff', 'MAX_ALLOWED', 0.000001D, 0.001D, true,
     'MIP Data Engineering',
     'Compare Overview Gold to weekly Silver count ingredients.',
     'Stop pipeline; Overview Gold does not reconcile to Silver.',
     true, ''),

    ('goldBreakoutToplinePctDiff', 'MAX_ALLOWED', 0.000001D, 0.001D, true,
     'MIP Data Engineering',
     'Inspect weekly attributed breakout bucketing.',
     'Stop pipeline; exclusive breakout values no longer reconcile to topline.',
     true, ''),

    ('goldCrosstabToplinePctDiff', 'MAX_ALLOWED', 0.000001D, 0.001D, true,
     'MIP Data Engineering',
     'Inspect weekly attributed crosstab pair construction.',
     'Stop pipeline; crosstab pair cells no longer reconcile to topline.',
     true, ''),

    ('duplicateGoldKeys', 'MAX_ALLOWED', 1D, 1D, true,
     'MIP Data Engineering',
     'Inspect duplicate Gold serving keys.',
     'Stop pipeline and resolve the duplicate Gold grain.',
     true, '')

AS t(
    checkName,
    thresholdDirection,
    warningThreshold,
    failureThreshold,
    isCritical,
    ownerTeam,
    warningNextSteps,
    failureNextSteps,
    isActive,
    notes
);
