from fastapi import FastAPI, Query, BackgroundTasks
from typing import Optional
import os, logging
from apscheduler.schedulers.background import BackgroundScheduler
from sgx_downloader import download_all_files, DOWNLOAD_DIR

app = FastAPI()
scheduler = BackgroundScheduler()

def scheduled_download():
    """Scheduled job to download the most recent available data every day."""
    logging.info("Running scheduled daily download job...")
    result = download_all_files()
    if result:
        logging.info(f"Scheduled download completed for {result}")
    else:
        logging.warning("Scheduled download found no data to download.")

@app.on_event("startup")
def startup_event():
    # Schedule job to run every day at 07:00 UTC (adjust as needed)
    scheduler.add_job(scheduled_download, "cron", hour=7, minute=0)
    scheduler.start()
    logging.info("Background scheduler started. Daily download job is active.")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

@app.get("/")
def root():
    return {"status": "SGX Downloader is running"}

@app.get("/files")
def list_files():
    return {"files": os.listdir(DOWNLOAD_DIR) if os.path.exists(DOWNLOAD_DIR) else []}

@app.post("/download")
def trigger_download(date: Optional[str] = Query(None, description="Date format: DD/MM/YYYY")):
    """Manually trigger a download for a specific date or latest available."""
    logging.info(f"Manual download triggered for date: {date or 'latest'}")
    actual_date = download_all_files(date)
    files = os.listdir(DOWNLOAD_DIR) if os.path.exists(DOWNLOAD_DIR) else []
    return {
        "message": "Download complete" if actual_date else "No data downloaded",
        "date": actual_date,
        "files": files
    }
