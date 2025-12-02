
import os
import requests
import msal

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")  # braveperspective.sharepoint.com
SITE_PATH = os.getenv("SITE_PATH")          # /sites/equipa.comite
FILE_PATH = "/Documentos Partilhados/General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"

# 1) Obter token
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
if "access_token" not in token_result:
    raise Exception("Erro ao obter token:", token_result)
token = token_result["access_token"]

headers = {"Authorization": f"Bearer {token}"}

# 2) Resolver site
site_url = f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}"
site_resp = requests.get(site_url, headers=headers).json()
site_id = site_resp.get("id")
if not site_id:
    raise Exception("Não consegui resolver o site:", site_resp)

# 3) Obter drive principal
drive_resp = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=headers).json()
drive_id = drive_resp.get("id")

# 4) Download do ficheiro
download_url = f"{GRAPH_BASE}/drives/{drive_id}/root:{FILE_PATH}:/content"
file_resp = requests.get(download_url, headers=headers)
if file_resp.status_code != 200:
    raise Exception(f"Erro ao fazer download: {file_resp.status_code} {file_resp.text}")

# Guardar localmente
with open("Historico_Sell_In_Mensal.xlsx", "wb") as f:
    f.write(file_resp.content)

print("[OK] Ficheiro descarregado com sucesso!")
