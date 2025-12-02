
import os
import sys
import requests
import msal

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

def get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[ERRO] Variável de ambiente '{name}' não definida.", file=sys.stderr)
        sys.exit(1)
    return v

TENANT_ID     = get_env("TENANT_ID")
CLIENT_ID     = get_env("CLIENT_ID")
CLIENT_SECRET = get_env("CLIENT_SECRET")
SITE_HOSTNAME = get_env("SITE_HOSTNAME")  # ex.: braveperspective.sharepoint.com
SITE_PATH     = get_env("SITE_PATH")      # ex.: /sites/equipa.comite

# Caminho relativo **dentro da biblioteca** (sem hostname/site):
# ATENÇÃO: vamos tentar com 'Shared Documents' e com 'Documentos Partilhados'
RELATIVE_AFTER_LIBRARY = "General/Teste - Daniel PowerAutomate/Historico Sell In Mensal.xlsx"

# === Token ===
authority = f"https://login.microsoftonline.com/{TENANT_ID}"
app = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
)
result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
if "access_token" not in result:
    print("[ERRO] Falha ao obter token:", result, file=sys.stderr)
    sys.exit(1)
headers = {"Authorization": f"Bearer {result['access_token']}", "Accept": "application/json"}

# === 1) Resolver o site por hostname + path correto (com ':') ===
site_url = f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}"
# Se SITE_PATH já inclui '/sites/equipa.comite', então fica '/sites/equipa.comite' ao concatenar.
# Exemplo final: /sites/braveperspective.sharepoint.com:/sites/equipa.comite
resp = requests.get(site_url, headers=headers)
if resp.status_code != 200:
    print("[ERRO] Não consegui resolver o site:", resp.status_code, resp.text, file=sys.stderr)
    sys.exit(2)
site = resp.json()
site_id = site.get("id")
print(f"[OK] Site id: {site_id} webUrl={site.get('webUrl')}")

# === 2) Enumerar drives (bibliotecas) e escolher a principal ===
drives_resp = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drives", headers=headers)
if drives_resp.status_code != 200:
    print("[ERRO] Não consegui obter drives:", drives_resp.status_code, drives_resp.text, file=sys.stderr)
    sys.exit(3)
drives = drives_resp.json().get("value", [])
if not drives:
    print("[ERRO] Nenhuma biblioteca encontrada no site.", file=sys.stderr)
    sys.exit(3)

# Preferir o drive padrão (documentLibrary) chamado "Documents" / "Shared Documents"
preferred = None
for d in drives:
    name = (d.get("name") or "").lower()
    if d.get("driveType") == "documentLibrary" and ("document" in name or "shared" in name):
        preferred = d
        break
# fallback: primeiro drive documentLibrary
if preferred is None:
    for d in drives:
        if d.get("driveType") == "documentLibrary":
            preferred = d
            break

drive_id = preferred.get("id")
drive_name = preferred.get("name")
print(f"[OK] Drive selecionado: id={drive_id}, name='{drive_name}', webUrl={preferred.get('webUrl')}")

# === 3) Tentar download com caminho 'Shared Documents' (padrão interno)
def try_download(base_library_name: str) -> bool:
    path = f"/{base_library_name}/{RELATIVE_AFTER_LIBRARY}"
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{path}:/content"
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        out = "Historico_Sell_In_Mensal.xlsx"
        with open(out, "wb") as f:
            f.write(r.content)
        print(f"[OK] Download concluído via '{base_library_name}'. Guardado em: {out}")
        return True
    else:
        print(f"[INFO] Falhou via '{base_library_name}': {r.status_code} {r.text}")
        return False

# 3a) Tenta com 'Shared Documents'
if try_download("Shared Documents"):
    sys.exit(0)

# 3b) Se falhar, tenta com 'Documentos Partilhados' (UI PT)
if try_download("Documentos Partilhados"):
    sys.exit(0)

# === 4) Diagnóstico extra: listar pasta 'General' para ver nomes reais
diag_url = f"{GRAPH_BASE}/drives/{drive_id}/root:/Shared Documents/General:/children"
diag = requests.get(diag_url, headers=headers)
print("\n[DIAGNÓSTICO] Listagem de 'Shared Documents/General':")
if diag.status_code == 200:
    for item in diag.json().get("value", []):
        print(f" - {item.get('name')} (id={item.get('id')})")
else:
    print(f"Não foi possível listar 'Shared Documents/General': {diag.status_code} {diag.text}")

print("\n[ERRO] Ficheiro não encontrado. Verifica se o caminho no drive corresponde exatamente à estrutura.")
sys.exit(4)
