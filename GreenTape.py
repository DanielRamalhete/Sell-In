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

DST_SHEET = "Historico24M"
DST_TABLE = "Historico24M"

DATE_COLUMN = "Data Entrega"

DEFAULT_TOP = 5000
IMPORT_CHUNK_SIZE = 2000
# ==========================


# ========= AUTH =========
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
base_headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}


def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h


# ========= HELPERS =========
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


# ========= READ HEADERS =========
def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)

    # 1) headerRowRange
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange",
        headers=h
    )
    if r.ok:
        vals = r.json().get("values", [[]])
        if vals and vals[0]:
            return [str(x) for x in vals[0]]

    # 2) columns
    rc = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns",
        headers=h
    )
    if rc.ok:
        return [c["name"] for c in rc.json().get("value", [])]

    # 3) range (fallback)
    rr = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    if rr.ok:
        vals = rr.json().get("values", [[]])
        return [str(x) for x in vals[0]]

    raise RuntimeError("Não consegui obter headers.")


# ========= PAGINATION =========
def list_rows(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)
    base = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"

    skip = 0
    while True:
        r = requests.get(f"{base}?$top={DEFAULT_TOP}&$skip={skip}", headers=h)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        for row in batch:
            yield row
        skip += DEFAULT_TOP


# ========= DATE HELPERS =========
def parse_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
        return epoch + timedelta(days=float(v))
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(v.strip(), fmt).replace(tzinfo=timezone.utc)
            except:
                pass
    return None


def cutoff_datetime():
    now = datetime.now(timezone.utc) - timedelta(days=1)
    return now.replace(year=now.year - 2)


# ========= SEED SHEET (CRITICAL!) =========
def seed_sheet_if_empty(drive_id, item_id, session_id, sheet_name):
    """
    Escreve "seed" na célula A1 para garantir que o Excel cria a estrutura interna.
    Necessário antes de tables/add numa sheet vazia.
    """
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='A1')"
    body = {"values": [["seed"]]}
    r = requests.patch(url, headers=h, data=json.dumps(body))
    r.raise_for_status()


# ========= ENSURE SHEET EXISTS =========
def get_or_create_sheet(drive_id, item_id, session_id, sheet_name):
    h = workbook_headers(session_id)

    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets",
        headers=h
    )
    r.raise_for_status()

    sheets = r.json().get("value", [])
    for s in sheets:
        if s["name"].strip().lower() == sheet_name.lower():
            return s["name"]

    # criar sheet
    r2 = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/add",
        headers=h,
        data=json.dumps({"name": sheet_name})
    )
    r2.raise_for_status()
    return sheet_name


# ========= TABLE CREATION =========
def create_table_and_get_name(drive_id, item_id, session_id, sheet_name, col_count):
    h = workbook_headers(session_id)

    col_start = "A"
    col_end = chr(ord("A") + col_count - 1)
    address = f"'{sheet_name}'!A1:{col_end}1"

    # criar tabela (só funciona se sheet tiver pelo menos A1!)
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables/add",
        headers=h, data=json.dumps({"address": address, "hasHeaders": True})
    )
    r.raise_for_status()

    # obter nome real da tabela criada
    r2 = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables",
        headers=h
    )
    r2.raise_for_status()
    return r2.json()["value"][-1]["name"]


# ========= INSERT ROWS =========
def add_rows_chunked(drive_id, item_id, table_name, session_id, rows):
    if not rows:
        return 0

    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"

    total = 0
    start = 0
    n = len(rows)

    while start < n:
        end = min(start + IMPORT_CHUNK_SIZE, n)
        r = requests.post(url, headers=h, data=json.dumps({
            "index": None,
            "values": rows[start:end]
        }))
        if r.status_code == 429:
            import time
            time.sleep(int(r.headers.get("Retry-After", "5")))
            continue
        r.raise_for_status()

        total += (end - start)
        start = end

    return total


# ========= MAIN =========
def keep_last_24_months():

    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, SRC_FILE_PATH)
    session_id = create_session(drive_id, item_id)

    try:
        headers = get_table_headers_safe(drive_id, item_id, SRC_TABLE, session_id)
        date_idx = headers.index(DATE_COLUMN)

        cutoff = cutoff_datetime()
        print("[INFO] Cutoff:", cutoff.date())

        rows_filtered = []
        for row in list_rows(drive_id, item_id, SRC_TABLE, session_id):
            vals = row["values"][0]
            dt = parse_date(vals[date_idx])
            if dt and dt >= cutoff:
                rows_filtered.append(vals)

        print("[INFO] Linhas filtradas:", len(rows_filtered))

        # garantir sheet
        real_sheet = get_or_create_sheet(drive_id, item_id, session_id, DST_SHEET)

        # seed obrigatório se sheet estiver vazia
        seed_sheet_if_empty(drive_id, item_id, session_id, real_sheet)

        # criar tabela
        real_table = create_table_and_get_name(
            drive_id, item_id, session_id, real_sheet, len(headers)
        )

        print("[INFO] Tabela criada:", real_table)

        # inserir dados
        inserted = add_rows_chunked(drive_id, item_id, real_table, session_id, rows_filtered)
        print("[INFO] Inseridas:", inserted)

    finally:
        close_session(drive_id, item_id, session_id)



# ========= RUN =========
if __name__ == "__main__":
    keep_last_24_months()
