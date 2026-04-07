import requests
import base64
import time
import os

# --- Config ---
TENANT_ID       = os.environ["TENANT_ID_POWERBI"]
CLIENT_ID       = os.environ["CLIENT_ID_POWERBI"]
CLIENT_SECRET   = os.environ["CLIENT_SECRET_POWERBI"]
WORKSPACE_ID    = os.environ["WORKSPACE_ID_POWERBI"]
REPORT_ID       = os.environ["REPORT_ID_POWERBI"]
SENDER_EMAIL    = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]
PAGE_NAME       = os.environ["PAGE_NAME"]

OUTPUT_FILE     = "market_share.xlsx"

# --- 1. Autenticação ---
def get_token(scope):
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         scope,
        }
    )
    r.raise_for_status()
    return r.json()["access_token"]

# --- 2. Listar páginas (para debug) ---
def get_pages():
    token   = get_token("https://analysis.windows.net/powerbi/api/.default")
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/reports/{REPORT_ID}/pages",
        headers=headers
    )
    r.raise_for_status()
    pages = r.json().get("value", [])
    print("=== Páginas disponíveis ===")
    for p in pages:
        print(f"  name: {p['name']} | displayName: {p['displayName']}")
    return pages

# --- 3. Export Power BI ---
def export_report():
    token   = get_token("https://analysis.windows.net/powerbi/api/.default")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base    = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/reports/{REPORT_ID}"

    # Iniciar export
    r = requests.post(f"{base}/ExportTo", headers=headers, json={
        "format": "XLSX",
        "powerBIReportConfiguration": {
            "pages": [{"pageName": PAGE_NAME}]
        }
    })
    r.raise_for_status()
    export_id = r.json()["id"]
    print(f"Export iniciado: {export_id}")

    # Polling
    for _ in range(24):  # max 2 minutos
        time.sleep(5)
        status = requests.get(f"{base}/exports/{export_id}", headers=headers).json()
        print(f"Status: {status['status']}")
        if status["status"] == "Succeeded":
            break
        if status["status"] == "Failed":
            raise Exception("Export falhou no Power BI")
    else:
        raise Exception("Timeout no export")

    # Download
    file_r = requests.get(f"{base}/exports/{export_id}/file", headers=headers)
    file_r.raise_for_status()
    with open(OUTPUT_FILE, "wb") as f:
        f.write(file_r.content)
    print(f"Ficheiro guardado: {OUTPUT_FILE}")

# --- 4. Enviar email ---
def send_email():
    token   = get_token("https://graph.microsoft.com/.default")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    with open(OUTPUT_FILE, "rb") as f:
        attachment_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "message": {
            "subject": "Market Share Mensal - Danone",
            "body": {
                "contentType": "Text",
                "content": "Olá,\n\nEm anexo o ficheiro de Market Share mensal.\n\nCumprimentos"
            },
            "toRecipients": [
                {"emailAddress": {"address": RECIPIENT_EMAIL}}
            ],
            "attachments": [{
                "@odata.type":  "#microsoft.graph.fileAttachment",
                "name":         OUTPUT_FILE,
                "contentBytes": attachment_b64
            }]
        }
    }

    r = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers=headers,
        json=payload
    )
    r.raise_for_status()
    print("Email enviado com sucesso")

# --- Main ---
if __name__ == "__main__":
    get_pages()      # imprime todas as páginas para confirmar o PAGE_NAME correto
    export_report()
    send_email()
