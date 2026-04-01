import os, io, requests, msal, pandas as pd
from datetime import datetime, timedelta

# ========= CONFIG =========
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH     = os.getenv("SITE_PATH")

SRC_FILE_PATH = "/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"
SRC_SHEET     = "TabelaAutomatica"

DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/Historico Sell In.xlsx"
DST_SHEET     = "Historico"

DATE_COLUMN   = "Data Entrega"
# ==========================

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ---- Auth ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# ---- Graph helpers ----
def get_site_id():
    return requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=headers).json()["id"]

def get_drive_id(site_id):
    return requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=headers).json()["id"]

def get_item_id(drive_id, path):
    return requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{path}", headers=headers).json()["id"]

def download_file(drive_id, item_id) -> bytes:
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content", headers=headers)
    r.raise_for_status()
    return r.content

def upload_file(drive_id, item_id, content: bytes):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/createUploadSession",
        headers={**headers, "Content-Type": "application/json"},
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}
    )
    r.raise_for_status()
    upload_url = r.json()["uploadUrl"]

    chunk_size = 10 * 1024 * 1024
    total = len(content)
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total) - 1
        chunk = content[start:end + 1]
        chunk_headers = {
            "Content-Length": str(len(chunk)),
            "Content-Range": f"bytes {start}-{end}/{total}"
        }
        r = requests.put(upload_url, headers=chunk_headers, data=chunk)
        r.raise_for_status()
        print(f"Uploaded bytes {start}-{end}/{total}")

# ---- Two month bounds ----
def two_month_bounds(d: datetime):
    # End = last day of current month
    if d.month == 12:
        last = datetime(d.year + 1, 1, 1).date() - timedelta(days=1)
    else:
        last = datetime(d.year, d.month + 1, 1).date() - timedelta(days=1)

    # Start = first day of previous month
    if d.month == 1:
        first = datetime(d.year - 1, 12, 1).date()
    else:
        first = datetime(d.year, d.month - 1, 1).date()

    return first, last

# ---- Main flow ----
site_id  = get_site_id()
drive_id = get_drive_id(site_id)
src_id   = get_item_id(drive_id, SRC_FILE_PATH)
dst_id   = get_item_id(drive_id, DST_FILE_PATH)

today = datetime.today()
month_start, month_end = two_month_bounds(today)
print(f"Target window: {month_start} to {month_end}")

# Download both files
print("Downloading source file...")
src_bytes = download_file(drive_id, src_id)

print("Downloading destination file...")
dst_bytes = download_file(drive_id, dst_id)

# Load into pandas
print("Loading into pandas...")
df_src = pd.read_excel(io.BytesIO(src_bytes), sheet_name=SRC_SHEET, engine="openpyxl")
df_dst = pd.read_excel(io.BytesIO(dst_bytes), sheet_name=DST_SHEET, engine="openpyxl")

print(f"Source rows: {len(df_src)}")
print(f"Destination rows (before): {len(df_dst)}")

# Ensure date column is datetime
df_src[DATE_COLUMN] = pd.to_datetime(df_src[DATE_COLUMN], dayfirst=True, errors="coerce")
df_dst[DATE_COLUMN] = pd.to_datetime(df_dst[DATE_COLUMN], dayfirst=True, errors="coerce")

# Filter source to two month window
mask_src = (df_src[DATE_COLUMN].dt.date >= month_start) & (df_src[DATE_COLUMN].dt.date <= month_end)
to_import = df_src[mask_src].copy()
print(f"Rows to import from source (two months): {len(to_import)}")

if to_import.empty:
    print("Nothing to import. Exiting.")
else:
    # Reorder source columns to match destination
    to_import = to_import.reindex(columns=df_dst.columns)

    # Remove two month window rows from destination
    mask_dst = (df_dst[DATE_COLUMN].dt.date >= month_start) & (df_dst[DATE_COLUMN].dt.date <= month_end)
    df_dst = df_dst[~mask_dst]
    print(f"Destination rows after removing two month window: {len(df_dst)}")

    # Append new rows
    df_final = pd.concat([df_dst, to_import], ignore_index=True)
    print(f"Destination rows after import: {len(df_final)}")

    # Write back to xlsx in memory
    print("Writing updated file to memory...")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_final.to_excel(writer, sheet_name=DST_SHEET, index=False)
    output.seek(0)
    updated_bytes = output.read()

    # Upload back to SharePoint
    print("Uploading updated file to SharePoint...")
    upload_file(drive_id, dst_id, updated_bytes)
    print(f"Done. {len(to_import)} rows imported, {mask_dst.sum()} old rows replaced.")
