import logging
import azure.functions as func
import pandas as pd
from datetime import datetime
import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from HHH_Scheduler_v20 import OrToolsScheduler, employees, SHIFT_TYPES, get_schedule_dataframe

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Roosterfunctie gestart.")

    try:
        # Optioneel: parameters ophalen vanuit de request
        # max_days = int(req.params.get('MAX_CONSECUTIVE_DAYS', 5))

        # Scheduler runnen
        scheduler = OrToolsScheduler(employees, year=2025, month=8)
        solver = scheduler.model
        callback = scheduler._callback = scheduler._debug_callback = scheduler.debug_callback = scheduler.DebugCallback(
            scheduler.works, employees, SHIFT_TYPES
        )
        cp_solver = scheduler.run()
        schedule = callback.get_best_schedule()

        df_result = get_schedule_dataframe(schedule)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rooster_{timestamp}.xlsx"

        # Schrijf naar in-memory Excel
        output = BytesIO()
        df_result.to_excel(output, index=False)
        output.seek(0)

        # Upload naar Azure Blob Storage
        blob_conn_str = os.environ.get("AzureWebJobsStorage")
        container_name = "roosters"  # Zorg dat deze bestaat

        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(output, overwrite=True)

        return func.HttpResponse(f"Rooster succesvol opgeslagen als {filename} in blob container '{container_name}'.", status_code=200)

    except Exception as e:
        logging.error(f"Fout tijdens uitvoeren: {e}")
        return func.HttpResponse("Er is een fout opgetreden tijdens het genereren van het rooster.", status_code=500)
