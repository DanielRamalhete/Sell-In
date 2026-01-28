import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")

DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
DST_TABLE = "Historico"
DATE_COLUMN = "Data Entrega"

# Estratégia: "block" (sort + delete bloco) ou "batch" (deletes em lotes descendentes)
MODE = os.getenv("MODE", "block")  # "block" | "batch"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

# "rolling" = últimos 24 meses a partir de hoje; "fullmonth" = desde 1º dia do mês corrente - 24 meses
CUTOFF_MODE = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" | "fullmonth"

# Paginação (para leitura de rows via /rows?$top&$skip)
DEFAULT_TOP = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
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
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers, data=json.dumps({"persistChanges": True})
    )
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

# ---- Helpers Excel comuns ----
def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

# ===== MESMA ABORDAGEM DO Implementacoes.py (headers + rows paginadas) =====
def get_table_headers(drive_id, item_id, table_name, session_id):
    """Tenta obter os headers via /headerRowRange."""
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][headerRowRange] STATUS:", r.status_code)
        try: print("[DEBUG][headerRowRange] JSON:", r.json())
        except Exception: print("[DEBUG][headerRowRange] TEXT:", r.text)
        r.raise_for_status()
    rng = r.json()
    values = rng.get("values", [[]])
    headers = [str(x) for x in (values[0] if values and values[0] else [])]
    return headers

def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    """Obtém headers com fallback para /columns e para a primeira linha de /range."""
    # 1) oficial
    try:
        headers = get_table_headers(drive_id, item_id, table_name, session_id)
        if headers:
            print(f"[DEBUG] headerRowRange → {headers}")
            return headers
    except requests.HTTPError:
        print("[DEBUG] headerRowRange falhou; a tentar fallback por /columns...")

    h = workbook_headers(session_id)

    # 2) /columns -> names
    url_cols = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns"
    rc = requests.get(url_cols, headers=h)
    if rc.ok:
        cols = rc.json().get("value", [])
        names = [c.get("name") for c in cols if c.get("name") is not None]
        if names:
            print("[DEBUG] Fallback /columns →", names)
            return names
        else:
            print("[DEBUG] /columns devolveu lista vazia ou sem 'name'.")
    else:
        print("[DEBUG] /columns falhou. STATUS:", rc.status_code)
        try: print("[DEBUG] /columns JSON:", rc.json())
        except Exception: print("[DEBUG] /columns TEXT:", rc.text)

    # 3) /range -> primeira linha
    url_rng = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    rr = requests.get(url_rng, headers=h)
    if rr.ok:
        rng = rr.json()
        vals = rng.get("values", [[]])
        if vals and vals[0]:
            headers = [str(x) for x in vals[0]]
            print("[DEBUG] Fallback /range primeira linha →", headers)
            return headers
        else:
            print("[DEBUG] /range não devolveu valores (ou primeira linha vazia).")
    else:
        print("[DEBUG] /range falhou. STATUS:", rr.status_code)
        try: print("[DEBUG] /range JSON:", rr.json())
        except Exception: print("[DEBUG] /range TEXT:", rr.text)
    rr.raise_for_status()  # força erro p/ ver detalhe

def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=None, max_pages=100000):
    """
    Itera páginas usando $top/$skip para evitar ResponsePayloadSizeLimitExceeded.
    Cada item tem 'index' (0-based na Tabela) e 'values'.
    """
    if top is None:
        top = DEFAULT_TOP
    h = workbook_headers(session_id)
    base_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    skip = 0; page = 0
    while page < max_pages:
        page += 1
        url = f"{base_url}?$top={top}&$skip={skip}"
        r = requests.get(url, headers=h)
        if not r.ok:
            print("[DEBUG][list_table_rows_paged] URL:", url)
            print("[DEBUG][list_table_rows_paged] STATUS:", r.status_code)
            try: print("[DEBUG][list_table_rows_paged] JSON:", r.json())
            except Exception: print("[DEBUG][list_table_rows_paged] TEXT:", r.text)
            r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            print(f"[DEBUG][list_table_rows_paged] Fim paginação. pages={page-1}")
            break
        print(f"[DEBUG][list_table_rows_paged] page={page} top={top} skip={skip} count={len(batch)}")
        for row in batch:
            yield row
        skip += top

# ===== Outras helpers específicas do Excel (mantidas) =====
def get_table_databody_range(drive_id, item_id, table_name, session_id):
    """
    Tenta obter o DataBodyRange (sem header). 
    Fallback: usa /range, remove a 1ª linha (headers) e ajusta o address para começar uma linha abaixo.
    Retorna um dicionário com chaves: address, values, rowCount, columnCount.
    """
    h = workbook_headers(session_id)

    # 1) tentativa oficial: dataBodyRange
    url_body = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange"
    r = requests.get(url_body, headers=h)
    if r.ok:
        return r.json()

    # --- Fallback se dataBodyRange falhar ---
    print("[DEBUG][dataBodyRange] Falhou com STATUS:", r.status_code)
    try:
        print("[DEBUG][dataBodyRange] JSON:", r.json())
    except Exception:
        print("[DEBUG][dataBodyRange] TEXT:", r.text)

    # 2) /range (inclui headers na 1ª linha)
    url_rng = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    rr = requests.get(url_rng, headers=h)
    if not rr.ok:
        print("[DEBUG][range] Falhou também. STATUS:", rr.status_code)
        try: print("[DEBUG][range] JSON:", rr.json())
        except Exception: print("[DEBUG][range] TEXT:", rr.text)
        rr.raise_for_status()

    rng = rr.json()
    values_all = rng.get("values", [])
    address_all = rng.get("address")  # ex.: "Folha1!A1:Z100"

    # Se não há valores, devolve corpo vazio coerente
    if not values_all or not isinstance(values_all, list):
        return {"address": address_all, "values": [], "rowCount": 0, "columnCount": 0}

    # Remover a 1ª linha (headers) para obter o corpo
    values_body = values_all[1:] if len(values_all) > 1 else []

    # Ajustar o address A1 para começar 1 linha abaixo
    # address_all = "Folha!A1:Z100" -> corpo = "Folha!A2:Z100" (se existir corpo)
    if address_all and "!" in address_all and ":" in address_all and values_body:
        sheet, cells = address_all.split("!", 1)
        start, end = cells.split(":", 1)

        def split_col_row(a1):
            i = 0
            while i < len(a1) and a1[i].isalpha():
                i += 1
            col = a1[:i]
            row = int(a1[i:]) if i < len(a1) else 1
            return col, row

        s_col, s_row = split_col_row(start)
        e_col, e_row = split_col_row(end)

        # Sobe o início em +1 (pula headers)
        s_row_adj = s_row + 1
        # Se a tabela tinha só headers, values_body=[] e não entramos aqui

        address_body = f"{sheet}!{s_col}{s_row_adj}:{e_col}{e_row}"
    else:
        # Sem address interpretável, devolve o original
        address_body = address_all

    # columnCount = nº de colunas do header (se existir), senão do 1º row do corpo
    col_count = 0
    if values_all and isinstance(values_all[0], list):
        col_count = len(values_all[0])
    elif values_body and isinstance(values_body[0], list):
        col_count = len(values_body[0])

    return {
        "address": address_body,
        "values": values_body,
        "rowCount": len(values_body),
        "columnCount": col_count
    }

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

# ---- Utilidades de data (repôr) ----
def months_ago(dt: datetime, months: int) -> datetime:
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

def cutoff_datetime(mode: str = "rolling") -> datetime:
    """
    Devolve o instante de corte para 'últimos 24 meses'.
    - 'rolling': desde agora-24m (usando now UTC - 1 dia para evitar zona cinzenta do dia corrente)
    - 'fullmonth': desde o 1º dia do mês corrente - 24 meses (00:00 UTC)
    """
    now_utc = datetime.now(timezone.utc) - timedelta(days=1)
    if mode == "fullmonth":
        start_this_month = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return months_ago(start_this_month, 24)
    return months_ago(now_utc, 24)

def parse_date_any(value):
    """
    Interpreta células de data: string, número serial Excel ou ISO.
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

# ===== Função principal =====
def keep_last_24_months(mode="block"):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, DST_FILE_PATH)
    session_id = create_session(drive_id, item_id)
    try:
        # Obter headers seguros (como no Implementacoes.py)
        headers = get_table_headers_safe(drive_id, item_id, DST_TABLE, session_id)
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
                dt = parse_date_any(val)
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
            end_col, _end_row = _split_col_row(end_a1)
            del_start = start_row
            del_end = start_row + delete_count - 1
            del_addr = f"{start_col}{del_start}:{end_col}{del_end}"
            print(f"del_start: {del_start} - del_end: {del_end} - del_addr: {del_addr}")
            # 5) Apagar o bloco de uma só vez (shift Up)
            delete_range_on_sheet(drive_id, item_id, sheet_name, del_addr, session_id)
            print(f"[BLOCK] Removidas {delete_count} linhas anteriores a {cutoff.date()} (1 operação).")

        elif mode == "batch":
            # 1) Ler todas as linhas (sem ordenar) e calcular índices a apagar
            indices_to_delete = []
            for r in list_table_rows_paged(drive_id, item_id, DST_TABLE, session_id, top=DEFAULT_TOP):
                idx = r.get("index")
                vals = (r.get("values", [[]])[0] or [])
                if idx is None or len(vals) <= date_col_idx:
                    continue
                dt = parse_date_any(vals[date_col_idx])
                if dt is None:
                    # mantém (podes alterar para remover)
                    continue
                if dt < cutoff:
                    indices_to_delete.append(int(idx))
            if not indices_to_delete:
                print("Nenhuma linha antiga encontrada. Nada a apagar.")
                return
            deleted = delete_rows_in_batches(drive_id, item_id, DST_TABLE, session_id, indices_to_delete, batch_size=BATCH_SIZE)
            print(f"[BATCH] Removidas {deleted} linhas anteriores a {cutoff.date()} em lotes descendentes (até {BATCH_SIZE} por batch).")
        else:
            raise ValueError(f"MODE inválido: {mode}")
    finally:
        close_session(drive_id, item_id, session_id)

# Entrada do script
if __name__ == "__main__":
    keep_last_24_months(mode=MODE)
