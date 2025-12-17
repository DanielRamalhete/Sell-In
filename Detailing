
import os, json, requests, msal
from datetime import datetime, timedelta

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

SRC_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Detailings Mensal.xlsx"
SRC_TABLE      = "TabelaAutomatica"

DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Detailings.xlsx"
DST_TABLE      = "Historico"

DATE_COLUMN    = "Data da visita"
# ==========================

# ---- Autenticação ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ---- Helpers base Graph ----
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

# ---- DEBUG helpers ----
def list_tables(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][list_tables] URL:", url)
        print("[DEBUG][list_tables] STATUS:", r.status_code)
        try:
            print("[DEBUG][list_tables] JSON:", r.json())
        except Exception:
            print("[DEBUG][list_tables] TEXT:", r.text)
        r.raise_for_status()
    data = r.json().get("value", [])
    print(f"[DEBUG] Tabelas no ficheiro (item_id={item_id}):")
    for t in data:
        ws_name = (t.get("worksheet") or {}).get("name")
        print(" - id:", t.get("id"),
              "| name:", t.get("name"),
              "| showHeaders:", t.get("showHeaders"),
              "| worksheet:", ws_name)
    return data

def get_table_headers(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][headerRowRange] URL:", url)
        print("[DEBUG][headerRowRange] STATUS:", r.status_code)
        try:
            print("[DEBUG][headerRowRange] JSON:", r.json())
        except Exception:
            print("[DEBUG][headerRowRange] TEXT:", r.text)
        r.raise_for_status()
    rng = r.json()
    values = rng.get("values", [[]])
    close_session(drive_id, dst_id, dst_sid)
