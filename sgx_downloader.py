import os
import sys
import logging
import requests
import zipfile
from typing import Optional, List, Dict

BASE_URL = "https://api3.sgx.com/infofeed/Apps"
LINKS_BASE = "https://links.sgx.com/1.0.0/derivatives-historical"
PARAMS = {"A": "COW_Tickdownload_Content", "B": "TimeSalesData", "C_T": "20"}

DOWNLOAD_DIR = "downloads"
LOG_FILE = "sgx_downloader.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode="a")]
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def fetch_items() -> List[Dict]:
    """Fetch available SGX items from API."""
    logging.info("Fetching items from SGX API...")
    response = requests.get(BASE_URL, params=PARAMS, headers=HEADERS)
    response.raise_for_status()
    items = response.json().get("items", [])
    # Filter out invalid objects that do not have a Date or key
    return [i for i in items if "Date" in i and "key" in i]


def available_dates() -> List[str]:
    """Return a list of all available dates from SGX."""
    items = fetch_items()
    return [i["Date"] for i in items]


def build_download_url(key: str, filename: str) -> str:
    """Construct the full download URL."""
    return f"{LINKS_BASE}/{key}/{filename}"


def extract_if_zip(filepath: str) -> None:
    """Extract a zip file and remove it after extraction."""
    if not filepath.lower().endswith(".zip"):
        return
    try:
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            extract_path = os.path.dirname(filepath)
            zip_ref.extractall(extract_path)
            logging.info(f"Extracted: {filepath} → {extract_path}")
        os.remove(filepath)
        logging.info(f"Deleted zip file: {filepath}")
    except zipfile.BadZipFile:
        logging.error(f"Failed to extract {filepath}: Bad zip file")


def download(url: str, filename: str) -> None:
    """Download a file and extract if needed."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(path):
        logging.info(f"Skipping {filename} (already exists)")
        return

    logging.info(f"Downloading {filename} from {url}")
    response = requests.get(url, headers=HEADERS, stream=True)

    if response.status_code != 200:
        logging.error(f"Failed to download {filename}: HTTP {response.status_code}")
        return
    if len(response.content) < 10:
        logging.error(f"Empty or invalid response for {filename}, skipping.")
        return

    with open(path, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)

    logging.info(f"Saved: {path} ({len(response.content) / 1024:.1f} KB)")
    extract_if_zip(path)


def download_all_files(target_date: Optional[str] = None) -> Optional[str]:
    """Download SGX tick/TC data for a given date or latest available date."""
    items = fetch_items()
    if not items:
        logging.error("No data available from SGX API")
        return None

    selected = None
    if target_date:
        selected = next((i for i in items if i.get("Date") == target_date), None)
        if not selected:
            logging.error(f"Date {target_date} is not available. Available dates: {[i['Date'] for i in items]}")
            return None
    else:
        selected = items[0]  # Latest available by default

    logging.info(f"Selected date: {selected['Date']} (key={selected['key']})")

    file_keys = [
        ("Data File Link", "Data File"),
        ("Tick Data Structure File Link", "Tick Data Structure File"),
        ("TC Data File Link", "TC Data File"),
        ("TC Data Structure File Link", "TC Data Structure File"),
    ]

    for link_key, name_key in file_keys:
        link = selected.get(link_key)
        filename = selected.get(name_key)
        if link and filename:
            try:
                download(build_download_url(selected["key"], filename), filename)
            except Exception as e:
                logging.error(f"Failed to download {filename}: {e}")

    return selected["Date"]


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    logging.info("SGX Downloader started.")
    result = download_all_files(date_arg)
    if result:
        logging.info(f"Download completed for {result}")
    logging.info("SGX Downloader finished.")
