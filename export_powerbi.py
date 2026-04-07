import requests
import base64
import os
import pandas as pd

# --- Config ---
TENANT_ID       = os.environ["TENANT_ID_POWERBI"]
CLIENT_ID       = os.environ["CLIENT_ID_POWERBI"]
CLIENT_SECRET   = os.environ["CLIENT_SECRET_POWERBI"]
WORKSPACE_ID    = os.environ["WORKSPACE_ID_POWERBI"]
DATASET_ID      = os.environ["DATASET_ID_POWERBI"]
SENDER_EMAIL    = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]
PBI_USERNAME    = os.environ["PBI_USERNAME"]
PBI_PASSWORD    = os.environ["PBI_PASSWORD"]

OUTPUT_FILE     = "market_share.xlsx"

# --- 1. Autenticação Delegada (ROPC) ---
def get_token_delegated(scope):
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "password",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "username":      PBI_USERNAME,
            "password":      PBI_PASSWORD,
            "scope":         scope,
        }
    )
    print(f"Auth status: {r.status_code}")
    if r.status_code != 200:
        print(f"Auth error: {r.text}")
    r.raise_for_status()
    return r.json()["access_token"]

# --- 2. Autenticação App (para Graph API / email) ---
def get_token_app(scope):
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

# --- 3. Executar DAX query e exportar para Excel ---
def export_data_to_excel():
    token   = get_token_delegated("https://analysis.windows.net/powerbi/api/.default")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    dax_query = """
        EVALUATE
        CALCULATETABLE(
            SUMMARIZECOLUMNS(
                Farmácias[Nome Farmácia],
                Calendário[Mês],
                Factos[Marca],
                "SOS Corrigido", [SOS Corrigido]
            ),
            Calendário[Ano] = 2026,
            Factos[Parceiro] = "DANONE",
            Factos[Situação] IN {"Validado", "Valido"},
            NOT Factos[Marca] = "Bledina",
            NOT ISBLANK(Farmácias[Nome Farmácia]),
            NOT Factos[Secção] = "Raio-X"
        )
    """

    r = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries",
        headers=headers,
        json={
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }
    )

    print(f"DAX query status: {r.status_code}")
    print(f"DAX query response: {r.text[:500]}")
    r.raise_for_status()

    # Converter para DataFrame
    rows = r.json()["results"][0]["tables"][0]["rows"]
    df = pd.DataFrame(rows)

    # Limpar nomes de colunas
    df.columns = [col.split("[")[-1].rstrip("]") for col in df.columns]

    print(f"Colunas: {list(df.columns)}")
    print(f"Linhas: {len(df)}")
    print(df.head())

    # Pivot para ter os meses como colunas
    df_pivot = df.pivot_table(
        index="Nome Farmácia",
        columns="Mês",
        values="SOS Corrigido",
        aggfunc="first"
    ).reset_index()

    df_pivot.to_excel(OUTPUT_FILE, index=False)
    print(f"Excel criado: {OUTPUT_FILE}")

# --- 4. Enviar email ---
def send_email():
    token   = get_token_app("https://graph.microsoft.com/.default")
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

    print(f"Email status: {r.status_code}")
    print(f"Email response: {r.text}")
    r.raise_for_status()
    print("Email enviado com sucesso")

# --- Main ---
if __name__ == "__main__":
    export_data_to_excel()
    send_email()
