
import os, json, requests, msal
from datetime import datetime, timedelta
from calendar import monthrange

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

SRC_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"
SRC_TABLE      = "TabelaAutomatica"

DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Historico Sell In.xlsx"
DST_TABLE      = "Historico"

DATE_COLUMN    = "Data Entrega"
# ==========================

# ---- Autenticação ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ---- Helpers ----
def get_site_id():
    return requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=base_headers).json()["id"]

def get_drive_id(site_id):
    return requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=base_headers).json()["id"]

def get_item_id(drive_id, path):
    return requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{path}", headers=base_headers).json()["id"]

def create_session(drive_id, item_id):
    r = requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
                      headers=base_headers, data=json.dumps({"persistChanges": True}))
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

def get_table_headers(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange", headers=h)
    return [str(x) for x in (r.json().get("values", [[]])[0] or [])]

def list_table_rows(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    return requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows", headers=h).json().get("value", [])

def add_rows(drive_id, item_id, table_name, session_id, values_2d):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    body = {"index": None, "values": values_2d}
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add", headers=h, data=json.dumps(body)).raise_for_status()

# ---- DELETE helpers ----
def batch_delete_rows(drive_id, item_id, table_name, session_id, indices):
    payload = {
        "requests": [
            {
                "id": str(i+1),
                "method": "DELETE",
                "url": f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{idx}",
                "headers": { "workbook-session-id": session_id }
            }
            for i, idx in enumerate(indices)
        ]
    }
    r = requests.post(f"{GRAPH_BASE}/$batch", headers=base_headers, data=json.dumps(payload))
    if not r.ok:
        print(f"[ERRO] $batch: {r.status_code} {r.text}")
    r.raise_for_status()
    resp = r.json()
    for it in resp.get("responses", []):
        if it.get("status") != 200:
            print(f"[ERRO] Delete id {it.get('id')} status {it.get('status')}: {it.get('body')}")

# ---- Utilidades ----
def excel_value_to_date(v):
    if isinstance(v, (int, float)):
        return datetime(1899, 12, 30) + timedelta(days=float(v))
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try: return datetime.strptime(v, fmt)
            except: pass
    return None

def reorder_values_by_headers(src_headers, dst_headers, row_values):
    src_pos = {name: i for i, name in enumerate(src_headers)}
    return [row_values[src_pos.get(name)] if src_pos.get(name) is not None else None for name in dst_headers]

# ---- Fluxo principal ----
site_id  = get_site_id()
drive_id = get_drive_id(site_id)
src_id   = get_item_id(drive_id, SRC_FILE_PATH)
dst_id   = get_item_id(drive_id, DST_FILE_PATH)

src_sid  = create_session(drive_id, src_id)
dst_sid  = create_session(drive_id, dst_id)

try:
    src_headers = get_table_headers(drive_id, src_id, SRC_TABLE, src_sid)
    dst_headers = get_table_headers(drive_id, dst_id, DST_TABLE, dst_sid)

    if DATE_COLUMN not in src_headers or DATE_COLUMN not in dst_headers:
        raise Exception(f"A coluna '{DATE_COLUMN}' não existe em uma das tabelas.")

    date_idx_src = src_headers.index(DATE_COLUMN)
    date_idx_dst = dst_headers.index(DATE_COLUMN)

    today = datetime.today()
    last_day = monthrange(today.year, today.month)[1]
    month_start = datetime(today.year, today.month, 1).date()
    month_end = datetime(today.year, today.month, last_day).date()

    src_rows = list_table_rows(drive_id, src_id, SRC_TABLE, src_sid)
    src_values = [r.get("values", [[]])[0] for r in src_rows]

    to_import = []
    for vals in src_values:
        d = excel_value_to_date(vals[date_idx_src])
        if d and month_start <= d.date() <= month_end:
            to_import.append(reorder_values_by_headers(src_headers, dst_headers, vals))

    if not to_import:
        print("Nada para importar.")
    else:
        dst_rows = list_table_rows(drive_id, dst_id, DST_TABLE, dst_sid)
        indices_to_delete = []
        for i, r in enumerate(dst_rows):
            vals = (r.get("values", [[]])[0] or [])
            if len(vals) > date_idx_dst:
                d = excel_value_to_date(vals[date_idx_dst])
                if d and month_start <= d.date() <= month_end:
                    indices_to_delete.append(r.get("index", i))

        if indices_to_delete:
            indices_to_delete = sorted(set(indices_to_delete))
            print(f"[DEBUG] Vou apagar {len(indices_to_delete)} linhas do mês atual.")

            CHUNK = 20
            for i in range(0, len(indices_to_delete), CHUNK):
                batch = indices_to_delete[i:i+CHUNK]
                batch_delete_rows(drive_id, dst_id, DST_TABLE, dst_sid, batch)

            print(f"[OK] Apaguei {len(indices_to_delete)} linhas do mês atual no destino.")

        add_rows(drive_id, dst_id, DST_TABLE, dst_sid, to_import)
        print(f"[OK] Inseridas {len(to_import)} linhas do mês atual no destino.")

finally:
    close_session(drive_id, src_id, src_sid)
    close_session(drive_id, dst_id, dst_sid)
