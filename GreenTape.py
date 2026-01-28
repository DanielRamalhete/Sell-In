
import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar


GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
DST_TABLE      = "Historico"
DATE_COLUMN    = "Data Entrega"

# Estratégia: "block" (sort + delete bloco) ou "batch" (deletes em lotes descendentes)
MODE           = os.getenv("MODE", "block")   # "block" | "batch"
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "20"))

# "rolling" = últimos 24 meses a partir de hoje; "fullmonth" = desde 1º dia do mês corrente - 24 meses
CUTOFF_MODE    = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" | "fullmonth"
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
def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_table_header_and_rows(drive_id, item_id, table_name, session_id):
    """
    Lê a tabela completa via /workbook/tables/{name}/range.
    Retorna dict: {"headers": [...], "rows": [[...], ...]}
    """
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    r.raise_for_status()
    rng = r.json()
    values = rng.get("values", [])
    if not values:
        return {"headers": [], "rows": []}
    headers = values[0]
    rows = values[1:] if len(values) > 1 else []
    return {"headers": headers, "rows": rows}

def get_table_databody_range(drive_id, item_id, table_name, session_id):
    """
    Retorna o DataBodyRange (sem header) com address e values já na ordem atual da folha.
    """
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange",
        headers=h
    )
    r.raise_for_status()
    return r.json()  # address, values, rowCount, columnCount, etc.

def table_sort_by_column(drive_id, item_id, table_name, session_id, column_index_zero_based, ascending=True):
    """
    Ordena a tabela pelo índice de coluna (0-based dentro da tabela).
    Endpoint: /workbook/tables/{name}/sort/apply
    """
    h = workbook_headers(session_id)
    body = {
        "fields": [
            {
                "key": column_index_zero_based,
                "ascending": ascending
            }
        ],
        "matchCase": False
    }
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/sort/apply",
        headers=h, data=json.dumps(body)
    )
    r.raise_for_status()

def delete_range_on_sheet(drive_id, item_id, sheet_name, addr_a1, session_id):
    """
    Apaga um range A1-style numa folha, com shift Up (para "subir" as linhas seguintes).
    Endpoint: /workbook/worksheets/{sheet}/range(address='{A1}')/delete
    """
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{addr_a1}')/delete"
    r = requests.post(url, headers=h, data=json.dumps({"shift": "Up"}))
    r.raise_for_status()

def delete_table_row(drive_id, item_id, table_name, session_id, row_index):
    """
    Apaga uma linha pelo índice 0-based dentro da tabela (exclui header).
    Endpoint: /workbook/tables/{name}/rows/{index}
    """
    h = workbook_headers(session_id)
    r = requests.delete(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{row_index}",
        headers=h
    )
    r.raise_for_status()


# ---- Utilidades de endereço A1 ----
def _parse_a1_address(addr):
    # Ex.: "Folha1!A2:Z100" -> ("Folha1", "A2", "Z100")
    if "!" in addr:
        sheet, rng = addr.split("!", 1)
    else:
        sheet, rng = None, addr
    if ":" in rng:
        start, end = rng.split(":", 1)
    else:
        start, end = rng, rng
    return sheet, start, end

def _split_col_row(a1):
    # "AB123" -> ("AB", 123)
    i = 0
    while i < len(a1) and a1[i].isalpha():
        i += 1
    return a1[:i], int(a1[i:]) if i < len(a1) else 1


# ---- Utilidades de data ----
def months_ago(dt, months):
    """
    Subtrai 'months' meses de dt preservando o dia quando possível.
    """
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = dt.day
    max_day = calendar.monthrange(year, month)[1]
    if day > max_day:
        day = max_day
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)

def cutoff_datetime(mode="rolling"):
    now_utc = datetime.now(timezone.utc) - timedelta(days=1)
    if mode == "fullmonth":
        start_this_month = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return months_ago(start_this_month, 24)
    return months_ago(now_utc, 24)

def parse_date_any(value):
    """
    Tenta interpretar células de data: string, número serial Excel ou ISO.
    Retorna timezone-aware (UTC) ou None se não conseguir.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None

    # Excel serial date (dias desde 1899-12-30; cuidado com leap bug de 1900)
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


# ---- Batch helpers (apagar por lotes descendentes) ----
def chunked_desc(indices, size):
    """Divide a lista em chunks e ordena cada chunk descendentemente."""
    indices_sorted = sorted(indices, reverse=True)
    for i in range(0, len(indices_sorted), size):
        yield indices_sorted[i:i+size]

def batch_delete_rows(drive_id, item_id, table_name, session_id, indices_chunk):
    """
    Envia um POST /$batch com até 20 deletes (limite típico).
    Coloca 'workbook-session-id' em cada sub-request para garantir persistência na sessão.
    """
    batch_url = f"{GRAPH_BASE}/$batch"
    requests_body = []
    for j, idx in enumerate(indices_chunk, start=1):
        sub = {
            "id": str(j),
            "method": "DELETE",
            "url": f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{idx}",
            "headers": {
                "workbook-session-id": session_id
            }
        }
        requests_body.append(sub)

    body = {"requests": requests_body}
    h = dict(base_headers)
    r = requests.post(batch_url, headers=h, data=json.dumps(body))
    r.raise_for_status()

    resp = r.json()
    errors = []
    for sub in resp.get("responses", []):
        status = sub.get("status", 0)
        if status >= 400:
            errors.append({"id": sub.get("id"), "status": status, "body": sub.get("body")})
    if errors:
        raise RuntimeError(f"Falhas no batch: {errors}")

def delete_rows_in_batches(drive_id, item_id, table_name, session_id, indices_to_delete, batch_size=20):
    """Apaga índices fornecidos em batches descendentes."""
    total = len(indices_to_delete)
    if total == 0:
        return 0
    deleted = 0
    for chunk in chunked_desc(indices_to_delete, batch_size):
        batch_delete_rows(drive_id, item_id, table_name, session_id, chunk)
        deleted += len(chunk)
    return deleted


# ---- Função principal ----
def keep_last_24_months(mode="block"):
    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id  = get_item_id(drive_id, DST_FILE_PATH)

    session_id = create_session(drive_id, item_id)
    try:
        # Ler headers para descobrir o índice da coluna de data
        data_all = get_table_header_and_rows(drive_id, item_id, DST_TABLE, session_id)
        headers = data_all["headers"]
        if not headers:
            print("Tabela vazia ou sem headers.")
            return

        try:
            date_col_idx = headers.index(DATE_COLUMN)
        except ValueError:
            raise RuntimeError(f"A coluna '{DATE_COLUMN}' não foi encontrada na tabela '{DST_TABLE}'.")

        cutoff = cutoff_datetime(CUTOFF_MODE)

        if mode == "block":
            # 1) Ordenar ascendente pela coluna de data (0-based)
            table_sort_by_column(drive_id, item_id, DST_TABLE, session_id, date_col_idx, ascending=True)

            # 2) Obter DataBodyRange (sem header), já ordenado
            body = get_table_databody_range(drive_id, item_id, DST_TABLE, session_id)
            values = body.get("values", [])
            if not values:
                print("Sem linhas no corpo da tabela.")
                return

            # 3) Contar quantas linhas iniciais estão < cutoff (contíguas no topo)
            delete_count = 0
            for row in values:
                val = row[date_col_idx] if date_col_idx < len(row) else None
                dt  = parse_date_any(val)
                # Se a data não é parsável, paramos para não apagar indevidamente
                if dt is None or dt >= cutoff:
                    break
                delete_count += 1

            if delete_count == 0:
                print("Nenhuma linha para remover (já só tens últimos 24 meses).")
                return

            # 4) Construir address A1 do bloco a apagar
            address = body.get("address")  # ex.: "Folha1!A2:Z100"
            sheet_name, start_a1, end_a1 = _parse_a1_address(address)
            if not sheet_name:
                raise RuntimeError(f"Endereço inesperado: {address}")

            start_col, start_row = _split_col_row(start_a1)
            end_col,   _end_row  = _split_col_row(end_a1)

            del_start = start_row
            del_end   = start_row + delete_count - 1
            del_addr  = f"{start_col}{del_start}:{end_col}{del_end}"

            print(f"del_start: {del_start} - del_end: {del_end} - del_addr: {del_addr}")

            # 5) Apagar o bloco de uma só vez (shift Up)
            delete_range_on_sheet(drive_id, item_id, sheet_name, del_addr, session_id)

            print(f"[BLOCK] Removidas {delete_count} linhas anteriores a {cutoff.date()} (1 operação).")

        elif mode == "batch":
            # 1) Vamos ler todas as linhas (sem ordenar) e calcular índices a apagar
            rows = data_all["rows"]
            if not rows:
                print("Sem linhas de dados.")
                return

            indices_to_delete = []
            for i, row in enumerate(rows):
                val = row[date_col_idx] if date_col_idx < len(row) else None
                dt  = parse_date_any(val)
                if dt is None:
                    # Mantém (podes alterar para remover)
                    continue
                if dt < cutoff:
                    indices_to_delete.append(i)

            if not indices_to_delete:
                print("Nenhuma linha antiga encontrada. Nada a apagar.")
                return

            deleted = delete_rows_in_batches(drive_id, item_id, DST_TABLE, session_id, indices_to_delete, batch_size=BATCH_SIZE)
            print(f"[BATCH] Removidas {deleted} linhas anteriores a {cutoff.date()} em lotes descendentes (até {BATCH_SIZE} por batch).")

        else:
            raise ValueError(f"MODE inválido: {mode}")

    finally:
        close_session(drive_id, item_id, session_id)


if __name__ == "__main__":
    keep_last_24_months(mode=MODE)
