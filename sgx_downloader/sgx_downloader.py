import os, sys, logging, requests, zipfile, hashlib, shutil, sqlite3
from datetime import date, datetime
from pathlib import Path
import boto3
from typing import Optional, List, Dict, Tuple

BASE_URL = "https://api3.sgx.com/infofeed/Apps"
LINKS_BASE = "https://links.sgx.com/1.0.0/derivatives-historical"
PARAMS = {"A": "COW_Tickdownload_Content", "B": "TimeSalesData", "C_T": "20"}

# --- Directories ---
DEFAULT_DOWNLOAD_DIR = "/data/raw"
REFERENCE_DIR = "/data/reference"
WAREHOUSE_DIR = "/data/warehouse"
METADATA_DB = "/data/metadata.db"

running_in_docker = os.path.exists("/.dockerenv")
if running_in_docker:
    DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", DEFAULT_DOWNLOAD_DIR)
else:
    DOWNLOAD_DIR = os.path.abspath("." + DEFAULT_DOWNLOAD_DIR)
    REFERENCE_DIR = os.path.abspath("." + REFERENCE_DIR)
    WAREHOUSE_DIR = os.path.abspath("." + WAREHOUSE_DIR)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(REFERENCE_DIR, exist_ok=True)
os.makedirs(WAREHOUSE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(METADATA_DB), exist_ok=True)

LOG_FILE = "sgx_downloader.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode="a")]
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

# ---------- Helpers: dates and items ----------
def _parse_input_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def _parse_item_date(s: str) -> date:
    return datetime.strptime(s, "%d %b %Y").date()

def _available_items() -> List[Dict]:
    logging.info("Fetching items from SGX API (last ~5 market days)...")
    r = requests.get(BASE_URL, params=PARAMS, headers=HEADERS)
    r.raise_for_status()
    items = r.json().get("items", [])
    items = [i for i in items if "Date" in i and "key" in i]
    items.sort(key=lambda x: _parse_item_date(x["Date"]), reverse=True)
    return items

def _summarize_available_dates(items: List[Dict]) -> str:
    return ", ".join(i["Date"] for i in items[:10])

def _select_item_for_date(items: List[Dict], target: Optional[date]) -> Optional[Dict]:
    if not items:
        return None
    if target is None:
        return items[0]
    by_date = {_parse_item_date(i["Date"]): i for i in items}
    if target in by_date:
        return by_date[target]
    logging.warning(
        "Requested date %s not in SGX last-5-day window. Available dates: %s",
        target.strftime("%d %b %Y"),
        _summarize_available_dates(items),
    )
    return None

# ---------- SCD Type 2 ----------
def init_metadata_db():
    conn = sqlite3.connect(METADATA_DB)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS file_versions (
        file_name TEXT,
        version_date TEXT,
        checksum TEXT,
        valid_from TEXT,
        valid_to TEXT
    )
    """)
    conn.commit()
    conn.close()

def compute_checksum(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def store_if_changed(file_path: str, final_dir: str) -> Optional[str]:
    conn = sqlite3.connect(METADATA_DB)
    file_name = os.path.basename(file_path)
    checksum = compute_checksum(file_path)
    today = str(date.today())

    latest = conn.execute("""
        SELECT rowid, checksum FROM file_versions
        WHERE file_name = ? AND valid_to IS NULL
        ORDER BY rowid DESC LIMIT 1
    """, (file_name,)).fetchone()

    if latest and latest[1] == checksum:
        logging.info("%s unchanged (checksum match), skipping store.", file_name)
        os.remove(file_path)
        conn.close()
        return None

    versioned_name = f"{Path(file_name).stem}_{today}{Path(file_name).suffix}"
    final_path = os.path.join(final_dir, versioned_name)
    os.makedirs(final_dir, exist_ok=True)
    shutil.move(file_path, final_path)

    if latest:
        conn.execute("UPDATE file_versions SET valid_to = ? WHERE rowid = ?", (today, latest[0]))
    conn.execute("""
        INSERT INTO file_versions (file_name, version_date, checksum, valid_from, valid_to)
        VALUES (?, ?, ?, ?, NULL)
    """, (file_name, today, checksum, today))
    conn.commit()
    conn.close()

    logging.info("Stored new version: %s", final_path)
    return final_path

# ---------- MinIO ----------
def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    )

def push_to_minio(file_path: str, bucket="datalake", prefix="raw"):
    try:
        key = f"{prefix}/{os.path.basename(file_path)}"
        _s3().upload_file(file_path, bucket, key)
        logging.info("Pushed to MinIO: s3://%s/%s", bucket, key)
    except Exception as e:
        logging.error("Failed to upload %s to MinIO: %s", file_path, e)

def upload_warehouse_file(local_path: str, table: str, d: date, bucket="datalake", root_prefix="derivative_data"):
    key = f"{root_prefix}/{table}/year={d.year}/month={d.month:02d}/day={d.day:02d}/{os.path.basename(local_path)}"
    try:
        _s3().upload_file(local_path, bucket, key)
        logging.info("Uploaded warehouse object: s3://%s/%s", bucket, key)
    except Exception as e:
        logging.error("Failed to upload warehouse file %s: %s", local_path, e)


# ---------- Download / Extract / Warehouse ----------
def build_download_url(key: str, filename: str) -> str:
    return f"{LINKS_BASE}/{key}/{filename}"

def download(url: str, filename: str, target_dir: str) -> Optional[str]:
    os.makedirs(target_dir, exist_ok=True)
    path = os.path.join(target_dir, filename)
    logging.info("Downloading %s from %s", filename, url)
    r = requests.get(url, headers=HEADERS, stream=True)
    if r.status_code != 200:
        logging.error("Failed to download %s: HTTP %s", filename, r.status_code)
        return None
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    logging.info("Saved: %s", path)
    return path

def _partition_dir(table_name: str, d: date) -> str:
    return os.path.join(WAREHOUSE_DIR, table_name, f"year={d.year}", f"month={d.month:02d}", f"day={d.day:02d}")

def move_to_warehouse_and_upload(extract_dir: str, table_name: str, d: date):
    local_target = _partition_dir(table_name, d)
    os.makedirs(local_target, exist_ok=True)
    for fn in os.listdir(extract_dir):
        src = os.path.join(extract_dir, fn)
        dst = os.path.join(local_target, fn)
        shutil.move(src, dst)
        logging.info("Moved %s to warehouse: %s", fn, local_target)
        upload_warehouse_file(dst, table_name, d)
    shutil.rmtree(extract_dir, ignore_errors=True)

def process_file(file_path: str, final_dir: str, upload_prefix: str, day_for_partition: date, table_for_zip: Optional[str] = None):
    stored_path = store_if_changed(file_path, final_dir)
    if not stored_path:
        return None

    # Upload original file (raw or reference)
    push_to_minio(stored_path, prefix=upload_prefix)

    if table_for_zip and stored_path.lower().endswith(".zip"):
        try:
            extract_dir = os.path.join("/tmp", Path(stored_path).stem)
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(stored_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
                logging.info("Extracted %s -> %s", stored_path, extract_dir)
            move_to_warehouse_and_upload(extract_dir, table_for_zip, day_for_partition)
            # DO NOT delete stored_path: keep raw zip locally
            logging.info("Kept ZIP in raw after loading to warehouse: %s", stored_path)
        except zipfile.BadZipFile:
            logging.error("Failed to extract %s: Bad zip file", stored_path)

    return stored_path

# ---------- Public API ----------
def download_all_files(target_date: Optional[str] = None) -> Optional[str]:
    init_metadata_db()
    items = _available_items()
    if not items:
        logging.error("No data available from SGX API")
        return None

    target = _parse_input_date(target_date) if target_date else None
    selected = _select_item_for_date(items, target)
    if not selected:
        return None

    selected_disp_date = selected["Date"]
    selected_key = selected["key"]
    selected_day = _parse_item_date(selected_disp_date)
    logging.info("Selected date: %s (key=%s)", selected_disp_date, selected_key)

    file_keys: List[Tuple[str, str]] = [
        ("Data File Link", "Data File"),
        ("Tick Data Structure File Link", "Tick Data Structure File"),
        ("TC Data File Link", "TC Data File"),
        ("TC Data Structure File Link", "TC Data Structure File"),
    ]

    new_files = []
    for link_key, name_key in file_keys:
        link = selected.get(link_key)
        filename = selected.get(name_key)
        if not (link and filename):
            continue

        direct_url = build_download_url(selected_key, filename)
        temp_file = download(direct_url, filename, DOWNLOAD_DIR)
        if not temp_file:
            continue

        is_schema = filename.endswith("_structure.dat")
        final_dir = REFERENCE_DIR if is_schema else DOWNLOAD_DIR
        upload_prefix = "derivative_reference" if is_schema else "raw"
        table_for_zip = "WEBPXTICK_DT" if filename.lower().endswith(".zip") else None

        stored_path = process_file(
            temp_file,
            final_dir,
            upload_prefix=upload_prefix,
            day_for_partition=selected_day,
            table_for_zip=table_for_zip,
        )
        if stored_path:
            new_files.append(stored_path)

    return selected_disp_date if new_files else None

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    logging.info("Using download directory: %s", DOWNLOAD_DIR)
    result = download_all_files(date_arg)
    if result:
        logging.info("Download completed for %s", result)
    else:
        logging.info("No data downloaded")
    logging.info("SGX Downloader finished.")
