
import os, json, requests, msal
from datetime import datetime, timedelta

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


# ---- Helpers específicos do Excel ----
def workbook_headers(session_id: str) -> dict[str, str]:
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_table_header_and_rows(drive_id: str, item_id: str, table_name: str, session_id: str) -> dict[str, Any]:
    """
    Retorna:
      {
        "headers": ["Col1", "Col2", ...],
        "rows": [ [v11, v12, ...], [v21, v22, ...], ... ]
      }
    Lê via /workbook/tables/{name}/range
    """
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    r.raise_for_status()
    rng = r.json()  # contém address, values, text, etc.
    values = rng.get("values", [])
    if not values:
        return {"headers": [], "rows": []}
    headers = values[0]
    rows = values[1:] if len(values) > 1 else []
    return {"headers": headers, "rows": rows}

def delete_table_row(drive_id: str, item_id: str, table_name: str, session_id: str, row_index: int) -> None:
    """
    Apaga a linha pelo índice 0-based dentro da tabela (exclui header).
    Endpoint: /workbook/tables/{name}/rows/{index}
    """
    h = workbook_headers(session_id)
    r = requests.delete(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{row_index}",
        headers=h
    )
    r.raise_for_status()

# ---- Utilidades de data ----
def months_ago(dt: datetime, months: int) -> datetime:
    """
    Subtrai 'months' meses de dt preservando dia quando possível.
    Ex.: 2026-01-05 - 24 meses = 2024-01-05
    """
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    # Ajuste do dia para evitar finais de mês inválidos (p.ex. 31 em fevereiro)
    day = dt.day
    # Número de dias do novo mês:
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    if day > max_day:
        day = max_day
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)

def parse_date_any(value) -> datetime | None:
    """
    Tenta interpretar células de data vindas do Excel: string, número serial ou ISO.
    Retorna timezone-aware (UTC) ou None se não conseguir.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None

    # Excel serial date (dias desde 1899-12-30); cuidado com o leap bug de 1900
    if isinstance(value, (int, float)):
        try:
            excel_epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
            return excel_epoch + timedelta(days=float(value))
        except Exception:
            pass

    if isinstance(value, str):
        s = value.strip()
        # ISO comum
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
        # Formatos PT comuns
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None

# ---- Lógica principal: manter apenas últimos 24 meses ----
def keep_last_24_months():
    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id  = get_item_id(drive_id, DST_FILE_PATH)

    session_id = create_session(drive_id, item_id)
    try:
        data = get_table_header_and_rows(drive_id, item_id, DST_TABLE, session_id)
        headers = data["headers"]
        rows    = data["rows"]

        if not headers:
            print("Tabela vazia ou sem headers.")
            return

        # Índice da coluna de data
        try:
            date_col_idx = headers.index(DATE_COLUMN)
        except ValueError:
            raise RuntimeError(f"A coluna '{DATE_COLUMN}' não foi encontrada na tabela '{DST_TABLE}'.")

        # Cutoff = hoje (UTC) - 24 meses
        now_utc = datetime.now(timezone.utc)
        cutoff  = months_ago(now_utc, 24)

        # Encontrar quais índices devem ser removidos (0-based relativo às linhas de dados, não incluindo header)
        indices_to_delete: List[int] = []
        for i, row in enumerate(rows):
            # Garantir comprimento
            val = row[date_col_idx] if date_col_idx < len(row) else None
            dt  = parse_date_any(val)
            if dt is None:
                # Se não consegue interpretar, considera como muito antigo? Melhor: manter.
                # Podes mudar para remover se preferires:
                # indices_to_delete.append(i)
                continue
            if dt < cutoff:
                indices_to_delete.append(i)

        if not indices_to_delete:
            print("Nenhuma linha antiga encontrada. Nada a apagar.")
            return

        # Apagar de trás para a frente para não deslocar índices
        indices_to_delete.sort(reverse=True)
        for idx in indices_to_delete:
            delete_table_row(drive_id, item_id, DST_TABLE, session_id, idx)

        print(f"Removidas {len(indices_to_delete)} linhas anteriores a {cutoff.date()} (últimos 24 meses mantidos).")

    finally:
        close_session(drive_id, item_id, session_id)

if __name__ == "__main__":
    keep_last_24_months()
