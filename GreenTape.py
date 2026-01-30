import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")

SRC_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
SRC_TABLE = "Historico"
SRC_SHEET = "LstPrd"

DST_SHEET = "Historico24M"    # Sheet nova
DST_TABLE = "Historico24M"    # Nome que queremos para a nova tabela

DATE_COLUMN = "Data Entrega"

DEFAULT_TOP = 5000
IMPORT_CHUNK_SIZE = 2000
CUTOFF_MODE = "rolling"
# ==========================


# ================== AUTENTICAÇÃO ==================
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ================== HELPERS ==================
def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_site_id():
    return requests.get(
        f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}",
        headers=base_headers
    ).json()["id"]

def get_drive_id(site_id):
    return requests.get(
        f"{GRAPH_BASE}/sites/{site_id}/drive",
        headers=base_headers
    ).json()["id"]

def get_item_id(drive_id, path):
    return requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/root:{path}",
        headers=base_headers
    ).json()["id"]

def create_session(drive_id, item_id):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers, data=json.dumps({"persistChanges": True})
    )
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = workbook_headers(session_id)
    requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession",
        headers=h
    )


# ================== HEADERS ==================
def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)

    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange", headers=h)
    if r.ok:
        vals = r.json().get("values", [[]])
        if vals and vals[0]:
            return [str(x) for x in vals[0]]

    rc = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns", headers=h)
    if rc.ok:
        cols = rc.json().get("value", [])
        return [c["name"] for c in cols]

    rr = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range", headers=h)
    if rr.ok:
        vals = rr.json().get("values", [[]])
        if vals and vals[0]:
            return [str(x) for x in vals[0]]

    raise RuntimeError("Não consegui obter headers.")


# ================== LEITURA PAGINADA ==================
def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=DEFAULT_TOP):
    h = workbook_headers(session_id)
    base = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"

    skip = 0
    while True:
        r = requests.get(f"{base}?$top={top}&$skip={skip}", headers=h)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        for row in batch:
            yield row
        skip += top


# ================== DATAS ==================
def months_ago(dt, months):
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)

def cutoff_datetime(mode="rolling"):
    now = datetime.now(timezone.utc) - timedelta(days=1)
    if mode == "fullmonth":
        start = now.replace(day=1, hour=0, minute=0, second=0)
        return months_ago(start, 24)
    return months_ago(now, 24)

def parse_date_any(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        excel_epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
        return excel_epoch + timedelta(days=float(v))
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(v.strip(), fmt).replace(tzinfo=timezone.utc)
            except:
                pass
    return None


# ================== SHEET ==================
def ensure_sheet_exists(drive_id, item_id, session_id, sheet_name):
    h = workbook_headers(session_id)

    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets", headers=h)
    r.raise_for_status()
    sheets = r.json().get("value", [])

    # procurar sheet existente
    for s in sheets:
        if s["name"].lower() == sheet_name.lower():
            return s["name"]

    # criar sheet
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/add",
        headers=h,
        data=json.dumps({"name": sheet_name})
    )
    r.raise_for_status()
    return sheet_name


# ================== TABELA NOVA ==================
def create_and_get_table_name(drive_id, item_id, session_id, sheet_name, headers):
    h = workbook_headers(session_id)

    # range da header row
    col_start = "A"
    col_end = chr(ord("A") + len(headers) - 1)
    address = f"'{sheet_name}'!{col_start}1:{col_end}1"

    # criar tabela (nome automático)
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables/add",
        headers=h,
        data=json.dumps({"address": address, "hasHeaders": True})
    )
    r.raise_for_status()

    # descobrir nome real da tabela criada
    r2 = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables",
        headers=h
    )
    r2.raise_for_status()
    tables = r2.json().get("value", [])

    # a tabela recém criada é a última
    real_name = tables[-1]["name"]

    return real_name


# ================== INSERIR DADOS ==================
def add_rows_chunked(drive_id, item_id, table_name, session_id, rows, chunk=2000):
    if not rows:
        return 0

    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"

    total = 0
    start = 0
    n = len(rows)

    while start < n:
        end = min(start + chunk, n)
        body = {"index": None, "values": rows[start:end]}
        r = requests.post(url, headers=h, data=json.dumps(body))

        if r.status_code == 429:
            import time
            time.sleep(int(r.headers.get("Retry-After", "5")))
            continue

        r.raise_for_status()

        total += end - start
        start = end

    return total


# ================== MAIN ==================
def keep_last_24_months():

    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, SRC_FILE_PATH)
    session_id = create_session(drive_id, item_id)

    try:
        headers = get_table_headers_safe(drive_id, item_id, SRC_TABLE, session_id)
        date_idx = headers.index(DATE_COLUMN)

        cutoff = cutoff_datetime(CUTOFF_MODE)
        print("[INFO] Cutoff =", cutoff.date())

        # LER + filtrar
        rows_filtered = []
        for r in list_table_rows_paged(drive_id, item_id, SRC_TABLE, session_id):
            vals = (r.get("values", [[]])[0] or [])
            dt = parse_date_any(vals[date_idx])
            if dt and dt >= cutoff:
                rows_filtered.append(vals)

        print("[INFO] Linhas selecionadas:", len(rows_filtered))

        # garantir sheet destino
        sheet_real = ensure_sheet_exists(drive_id, item_id, session_id, DST_SHEET)

        # criar tabela nova + obter nome real
        real_dst_table = create_and_get_table_name(drive_id, item_id, session_id, sheet_real, headers)
        print("[INFO] Tabela criada →", real_dst_table)

        # inserir linhas
        inserted = add_rows_chunked(drive_id, item_id, real_dst_table, session_id, rows_filtered)
        print(f"[INFO] Inseridas {inserted} linhas na nova tabela.")

    finally:
        close_session(drive_id, item_id, session_id)



# ================== RUN ==================
if __name__ == "__main__":
    keep_last_24_months()
