
import os, json, requests, msal
from datetime import datetime, timedelta

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

# Ficheiro FONTE (mês atual)
SRC_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Ficheiro1.xlsx"
SRC_TABLE      = "TabelaAutomatica"

# Ficheiro DESTINO (consolidado)
DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"
DST_TABLE      = "Tabela25"

DATE_COLUMN    = "Data"
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
    r = requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def get_drive_id(site_id):
    r = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def get_item_id(drive_id, path):
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{path}", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def create_session(drive_id, item_id):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers, data=json.dumps({"persistChanges": True})
    )
    r.raise_for_status()
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

def get_table_headers(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange",
        headers=h
    )
    r.raise_for_status()
    return [str(x) for x in (r.json().get("values", [[]])[0] or [])]

def list_table_rows(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows",
        headers=h
    )
    r.raise_for_status()
    return r.json().get("value", [])

def add_rows(drive_id, item_id, table_name, session_id, values_2d):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    body = {"index": None, "values": values_2d}
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add",
        headers=h, data=json.dumps(body)
    )
    r.raise_for_status()

def delete_row_by_index(drive_id, item_id, table_name, session_id, idx):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    r = requests.delete(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{idx}",
        headers=h
    )
    if r.status_code not in (200, 204):
        raise Exception(f"Erro a eliminar linha {idx}: {r.status_code} {r.text}")

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

    # Definir mês atual
    today = datetime.today()
    month_start = datetime(today.year, today.month, 1).date()
    month_end = datetime(today.year, today.month, 31).date()

    # Ler linhas da fonte
    src_rows = list_table_rows(drive_id, src_id, SRC_TABLE, src_sid)
    src_values = [r.get("values", [[]])[0] for r in src_rows]

    # Filtrar linhas do mês atual
    to_import = []
    for vals in src_values:
        d = excel_value_to_date(vals[date_idx_src])
        if d and month_start <= d.date() <= month_end:
            to_import.append(reorder_values_by_headers(src_headers, dst_headers, vals))

    if not to_import:
        print("Nada para importar.")
    else:
        # Apagar linhas do mês atual no destino
        dst_rows = list_table_rows(drive_id, dst_id, DST_TABLE, dst_sid)
        to_delete_idx = []
        for r in dst_rows:
            vals = (r.get("values", [[]])[0] or [])
            if len(vals) > date_idx_dst:
                d = excel_value_to_date(vals[date_idx_dst])
                if d and month_start <= d.date() <= month_end:
                    to_delete_idx.append(r.get("index"))

        for idx in sorted(to_delete_idx, reverse=True):
            delete_row_by_index(drive_id, dst_id, DST_TABLE, dst_sid, idx)

        # Inserir novas linhas
        add_rows(drive_id, dst_id, DST_TABLE, dst_sid, to_import)
        print(f"[OK] Atualizado mês atual com {len(to_import)} linhas.")

finally:
    close_session(drive_id, src_id, src_sid)
    close_session(drive_id, dst_id, dst_sid)
