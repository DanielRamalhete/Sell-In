
import os
import sys
import json
import requests
import msal

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

def get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[ERRO] Variável de ambiente '{name}' não definida.", file=sys.stderr)
        sys.exit(1)
    return v

def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Autentica via OAuth2 client credentials e devolve o access token para Microsoft Graph.
    Scope: https://graph.microsoft.com/.default
    """
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        print("[ERRO] Falha ao obter token:", json.dumps(result, indent=2), file=sys.stderr)
        sys.exit(1)
    return result["access_token"]

def main():
    # ====== Ler ambiente (GitHub Secrets / Actions) ======
    TENANT_ID     = get_env("TENANT_ID")
    CLIENT_ID     = get_env("CLIENT_ID")
    CLIENT_SECRET = get_env("CLIENT_SECRET")
    SITE_HOSTNAME = "braveperspective.sharepoint.com"    # ex.: contoso.sharepoint.com
    SITE_PATH     = "/sites/equipa.comite" # ex.: /sites/Finance

    # ====== Token ======
    token = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    # ====== 1) Resolver o Site (hostname + path) ======
    # GET /sites/{hostname}:/sites/{sitePath}
    site_url = "https://braveperspective.sharepoint.com/sites/equipa.comite/"   # f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/sites{SITE_PATH}"
    site_resp = requests.get(site_url, headers=headers)
    if site_resp.status_code != 200:
        print("[ERRO] Não consegui resolver o site:",
              site_resp.status_code, site_resp.text, file=sys.stderr)
        sys.exit(2)

    site = site_resp.json()
    site_id = site.get("id")
    print(f"[OK] Site resolvido: id={site_id}")

    # ====== 2) Obter a Document Library principal (drive) ======
    # GET /sites/{siteId}/drive
    drive_resp = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=headers)
    if drive_resp.status_code != 200:
        print("[ERRO] Não consegui obter o drive:",
              drive_resp.status_code, drive_resp.text, file=sys.stderr)
        sys.exit(3)

    drive = drive_resp.json()
    drive_id = drive.get("id")
    drive_name = drive.get("name")
    print(f"[OK] Drive obtido: id={drive_id}, name={drive_name}")

    # ====== 3) Listar alguns itens na raiz do drive ======
    # GET /drives/{driveId}/root/children?$top=5
    list_resp = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/root/children?$top=5",
        headers=headers
    )
    if list_resp.status_code != 200:
        print("[ERRO] Não consegui listar itens:",
              list_resp.status_code, list_resp.text, file=sys.stderr)
        sys.exit(4)

    items = list_resp.json().get("value", [])
    print(f"[OK] Conexão ao SharePoint/Graph confirmada. Itens na raiz (top 5):")
    for i, it in enumerate(items, start=1):
        print(f"  {i:02d}. {it.get('name')}  [{it.get('id')}]")

    print("[SUCESSO] Teste concluído.")

if __name__ == "__main__":
    main()
