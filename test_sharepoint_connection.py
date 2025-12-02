import os
import requests
import msal
import json

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")
FILE_PATH = "/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"
TABLE_NAME = "Tabela25"  # Nome da tabela no Excel
COLUMN_NAME = "DIM"       # Nome da coluna a remover

# 1) Token
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# 2) Resolver site
site_url = f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}"
site_id = requests.get(site_url, headers=headers).json().get("id")

# 3) Obter drive e item
drive_id = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=headers).json().get("id")
item = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{FILE_PATH}", headers=headers).json()
item_id = item.get("id")

# 4) Criar sessão persistente
session = requests.post(
    f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
    headers=headers,
    data=json.dumps({"persistChanges": True})
).json()
session_id = session.get("id")
excel_headers = dict(headers)
excel_headers["workbook-session-id"] = session_id

# 5) Obter colunas da tabela para confirmar índice
cols_resp = requests.get(
    f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{TABLE_NAME}/columns",
    headers=excel_headers
).json()
columns = cols_resp.get("value", [])
target_index = None
for idx, col in enumerate(columns):
    if col.get("name") == COLUMN_NAME:
        target_index = idx
        break

if target_index is None:
    raise Exception(f"Coluna '{COLUMN_NAME}' não encontrada na tabela '{TABLE_NAME}'.")

# 6) Remover coluna pelo índice
del_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{TABLE_NAME}/columns/{target_index}"
del_resp = requests.delete(del_url, headers=excel_headers)
if del_resp.status_code in (200, 204):
    print(f"[OK] Coluna '{COLUMN_NAME}' removida com sucesso da tabela '{TABLE_NAME}'.")
else:
    raise Exception(f"Erro ao remover coluna: {del_resp.status_code} {del_resp.text}")


requests.post(
    f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession",
    headers=excel_headers
)
