# SGX Derivatives Daily Downloader

## Overview
This project automates the **daily download of Singapore Exchange (SGX) derivatives data**.

- Runs automatically every day at **07:00 UTC** (via APScheduler in FastAPI).
- Stores all downloaded files in **MinIO** (organized into `raw`, `derivative_reference`, and `derivative_data`).
- Applies **SCD Type 2 versioning** for reference files (`TickData_structure.dat`, `TC_structure.dat`).
- Supports **manual re-download** of data for any date (via API).

This provides a reliable, reproducible way to collect, store, and manage SGX data for downstream analytics.

## Project Structure

```
ScrapeJob/
├── docker-compose.yml        # Spins up MinIO, Iceberg REST, Trino, Downloader API
├── sgx_downloader/           # Downloader source code
│   ├── sgx_downloader.py     # Main download & storage logic
│   ├── app.py                # FastAPI service with scheduled job
│   └── requirements.txt      # Python dependencies
├── downloads/                # (Mounted) Local mirror of /data/raw inside container
└── logs/                     # (Optional) Downloader logs if mounted
```

## Setup Instructions

### 1. Clone Repo

```bash
git clone https://github.com/joshphuocD/sgx_downloader
cd sgx_downloader
```

### 2. Build & Start Containers

```bash
docker compose build sgx-downloader
docker compose up -d
```

This will start:
- MinIO (object storage) on `localhost:9000`
- MinIO Console on `localhost:9001`
- Downloader API on `localhost:5022`
- (Optional) Iceberg REST + Trino for future analytics

### 3. Check MinIO Buckets

```bash
docker compose exec minio sh -c "mc alias set local http://minio:9000 minio minio123 && mc ls --recursive local/datalake"
```

You should see:

```
[DATE]  0B  STANDARD  raw/
[DATE]  0B  STANDARD  derivative_reference/
[DATE]  0B  STANDARD  derivative_data/
```

### 4. Trigger a Manual Download

```bash
curl -X POST "http://localhost:5022/download"
```

Expected result:

```json
{
  "message": "Download complete",
  "date": "18 Sep 2025",
  "files": [
    "TC_20250918_2025-09-20.txt",
    "WEBPXTICK_DT-20250918_2025-09-20.zip"
  ]
}
```

### 5. Inspect Downloaded Files

```bash
docker compose exec sgx-downloader ls -l /data/raw
```

Or check MinIO:

```bash
docker compose exec minio sh -c "mc alias set local http://minio:9000 minio minio123 && mc ls --recursive local/datalake/raw"
```

### 6. Test Automatic Scheduler

Change the `hour` in `app.py` to your current hour or wait until 07:00 UTC and run:

```bash
docker compose logs -f sgx-downloader
```

You should see the scheduled job log messages.

### 7. Re-Download for Specific Date

```bash
curl -X POST "http://localhost:5022/download?date=17/09/2025"
```

If available, it will fetch and store the data.

## Reset / Clean Up

```bash
docker compose down -v
docker compose build --no-cache sgx-downloader
docker compose up -d
```

This removes all stored data and restarts fresh.

## Testing Checklist

- [x] Containers start successfully.
- [x] `/data/raw` is created and mounted to `downloads/`.
- [x] Manual and scheduled downloads store files and upload to MinIO.
- [x] SCD2 works: no duplicate uploads for unchanged files.
- [x] New version is created when reference file changes.
- [x] Manual re-download works for available dates.
