import os, io, requests, msal, pandas as pd, openpyxl
from datetime import datetime, timedelta

# ========= CONFIG =========
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH     = os.getenv("SITE_PATH")

SRC_FILE_PATH = "/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"
DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/Historico Sell In.xlsx"

# Sheet names - hardcode after confirming from logs
SRC_SHEET0 = 0
SRC_SHEET1 = 1
DST_SHEET0 = 0
DST_SHEET1 = 1

DATE_COLUMN    = "Data Entrega"
SRC_JOIN_COL   = "Refª Visita"   # in Sheet 1
DST_JOIN_COL   = "Refª"          # in Sheet 0
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
    if d.month == 12:
        last = datetime(d.year + 1, 1, 1).date() - timedelta(days=1)
    else:
        last = datetime(d.year, d.month + 1, 1).date() - timedelta(days=1)
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

# ================================================================
# SHEET 1 (already working logic)
# ================================================================
print("\n--- Processing Sheet 1 ---")
df_src1 = pd.read_excel(io.BytesIO(src_bytes), sheet_name=SRC_SHEET1, engine="openpyxl")
df_dst1 = pd.read_excel(io.BytesIO(dst_bytes), sheet_name=DST_SHEET1, engine="openpyxl")

print(f"Source Sheet1 rows: {len(df_src1)}")
print(f"Destination Sheet1 rows (before): {len(df_dst1)}")

df_src1[DATE_COLUMN] = pd.to_datetime(df_src1[DATE_COLUMN], dayfirst=True, errors="coerce")
df_dst1[DATE_COLUMN] = pd.to_datetime(df_dst1[DATE_COLUMN], dayfirst=True, errors="coerce")

mask_src1 = (df_src1[DATE_COLUMN].dt.date >= month_start) & (df_src1[DATE_COLUMN].dt.date <= month_end)
to_import1 = df_src1[mask_src1].copy()
print(f"Rows to import Sheet1 (two months): {len(to_import1)}")

if to_import1.empty:
    print("Sheet1: Nothing to import.")
    df_final1 = df_dst1
else:
    to_import1 = to_import1.reindex(columns=df_dst1.columns)
    mask_dst1 = (df_dst1[DATE_COLUMN].dt.date >= month_start) & (df_dst1[DATE_COLUMN].dt.date <= month_end)
    df_dst1 = df_dst1[~mask_dst1]
    df_final1 = pd.concat([df_dst1, to_import1], ignore_index=True)
    print(f"Destination Sheet1 rows after import: {len(df_final1)}")

# ================================================================
# SHEET 0
# ================================================================
print("\n--- Processing Sheet 0 ---")
df_src0 = pd.read_excel(io.BytesIO(src_bytes), sheet_name=SRC_SHEET0, engine="openpyxl")
df_dst0 = pd.read_excel(io.BytesIO(dst_bytes), sheet_name=DST_SHEET0, engine="openpyxl")

print(f"Source Sheet0 rows: {len(df_src0)}")
print(f"Destination Sheet0 rows (before): {len(df_dst0)}")

# Build date lookup from source Sheet 1 only
date_lookup = (
    df_src1[[SRC_JOIN_COL, DATE_COLUMN]]
    .dropna(subset=[SRC_JOIN_COL])
    .drop_duplicates(subset=[SRC_JOIN_COL], keep="first")
    .rename(columns={SRC_JOIN_COL: DST_JOIN_COL})
)

# Join Data Entrega into source Sheet 0 to identify two month rows
df_src0 = df_src0.merge(date_lookup, on=DST_JOIN_COL, how="left")
df_src0[DATE_COLUMN] = pd.to_datetime(df_src0[DATE_COLUMN], dayfirst=True, errors="coerce")

# Get the Refª values that fall in the two month window
mask_src0 = (df_src0[DATE_COLUMN].dt.date >= month_start) & (df_src0[DATE_COLUMN].dt.date <= month_end)
to_import0 = df_src0[mask_src0].copy()
refs_in_window = to_import0[DST_JOIN_COL].unique()
print(f"Rows to import Sheet0 (two months): {len(to_import0)}")
print(f"Unique Refª in window: {len(refs_in_window)}")

if to_import0.empty:
    print("Sheet0: Nothing to import.")
    df_final0 = df_dst0
else:
    # Drop Data Entrega from import rows — not permanent in Sheet 0
    original_cols0 = [c for c in df_dst0.columns]
    to_import0 = to_import0[original_cols0]

    # Remove rows from destination whose Refª is in the two month window
    df_dst0 = df_dst0[~df_dst0[DST_JOIN_COL].isin(refs_in_window)]
    print(f"Destination Sheet0 rows after removing two month window: {len(df_dst0)}")

    df_final0 = pd.concat([df_dst0, to_import0], ignore_index=True)
    print(f"Destination Sheet0 rows after import: {len(df_final0)}")

# ================================================================
# Write both sheets back to the destination file and upload
# ================================================================
print("\nWriting updated file to memory...")
output = io.BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    df_final0.to_excel(writer, sheet_name=DST_SHEET0, index=False)
    df_final1.to_excel(writer, sheet_name=DST_SHEET1, index=False)
output.seek(0)
updated_bytes = output.read()

print("Uploading updated file to SharePoint...")
upload_file(drive_id, dst_id, updated_bytes)
print("Done.")
