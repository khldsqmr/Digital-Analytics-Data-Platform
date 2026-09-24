# Databricks notebook source

# ==================================================================================================
# Media Intelligence - App Test Metrics Orchestration
#
# NOTEBOOK:
#   sdi_nb_mediaIntelligence_orchestration_appTestMetrics_perRun
#
# PURPOSE:
#   Executes the Media Intelligence App Test Metrics orchestration procedure.
#
# ORCHESTRATION:
#   1. Refresh Gold Long
#   2. Refresh Gold Wide
#
# UPSTREAM:
#   Existing PulseTMS Adobe and MFC Silver tables must already be refreshed.
#
# PROCEDURE:
#   sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun
# ==================================================================================================

from datetime import datetime, timezone


# --------------------------------------------------------------------------------------------------
# Orchestration procedure
# --------------------------------------------------------------------------------------------------

procedure_name = (
    "prdrzranalytics.lab42."
    "sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun"
)


# --------------------------------------------------------------------------------------------------
# Start
# --------------------------------------------------------------------------------------------------

start_time = datetime.now(timezone.utc)

print("Starting Media Intelligence App Test Metrics orchestration")
print(f"Start UTC: {start_time.isoformat()}")


# --------------------------------------------------------------------------------------------------
# Execute orchestration
# --------------------------------------------------------------------------------------------------

try:

    result = spark.sql(
        f"""
        CALL {procedure_name}()
        """
    )

    # Ensure the stored procedure fully completes.
    result.collect()


    end_time = datetime.now(timezone.utc)


    print("")
    print("Media Intelligence App Test Metrics orchestration completed successfully.")
    print(f"End UTC:  {end_time.isoformat()}")
    print(f"Duration: {end_time - start_time}")


except Exception as error:

    end_time = datetime.now(timezone.utc)


    print("")
    print("Media Intelligence App Test Metrics orchestration FAILED.")
    print(f"End UTC:  {end_time.isoformat()}")
    print(f"Duration: {end_time - start_time}")
    print(f"Error:    {error}")


    # Re-raise so Databricks marks the notebook / Job task as Failed.
    raise