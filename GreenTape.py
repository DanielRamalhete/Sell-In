# --- topo do ficheiro mantém-se (imports/config/auth/etc.) ---
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

MODE = os.getenv("MODE", "block")  # "block" | "batch"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

# NOVO: paginação para leitura de rows (igual ao Implementacoes.py)
DEFAULT_TOP = int(os.getenv("GRAPH_ROWS_TOP") or "5000")

CUTOFF_MODE = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" | "fullmonth"
# ==========================

# ---- Autenticação (mantém-se) ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ---- Helpers base Graph (mantêm-se) ----
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

# ===== [NOVO] => MESMA ABORDAGEM DO Implementacoes.py =====
def get_table_headers(drive_id, item_id, table_name, session_id):
    """Tenta obter os headers via /headerRowRange."""
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r = requests.get(url, headers=h)
    if not r.ok:
        # debug opcional
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
    Itera pára páginas usando $top/$skip.
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

# ===== [mantido] outras helpers do teu script =====
def get_table_databody_range(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange",
        headers=h
    )
    r.raise_for_status()
    return r.json()

def table_sort_by_column(drive_id, item_id, table_name, session_id, column_index_zero_based, ascending=True):
    h = workbook_headers(session_id)
    body = {
        "fields": [{"key": column_index_zero_based, "ascending": ascending}],
        "matchCase": False
    }
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/sort/apply",
        headers=h, data=json.dumps(body)
    )
    r.raise_for_status()

def delete_range_on_sheet(drive_id, item_id, sheet_name, addr_a1, session_id):
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{addr_a1}')/delete"
    r = requests.post(url, headers=h, data=json.dumps({"shift": "Up"}))
    r.raise_for_status()

def delete_table_row(drive_id, item_id, table_name, session_id, row_index):
    h = workbook_headers(session_id)
    r = requests.delete(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/{row_index}",
        headers=h
    )
    r.raise_for_status()

# ... (mantém utilidades A1 e datas, chunked_desc, batch_delete_rows, delete_rows_in_batches, etc.)

# ====== ajuste no fluxo principal ======
def keep_last_24_months(mode="block"):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, DST_FILE_PATH)
    session_id = create_session(drive_id, item_id)
    try:
        # Em vez de get_table_header_and_rows -> usar headers seguros
        headers = get_table_headers_safe(drive_id, item_id, DST_TABLE, session_id)
        if not headers:
            print("Tabela vazia ou sem headers.")
            return
        try:
            date_col_idx = headers.index(DATE_COLUMN)
        except ValueError:
            raise RuntimeError(f"A coluna '{DATE_COLUMN}' não foi encontrada na tabela '{DST_TABLE}'.")

        # cutoff igual ao teu original
        cutoff = cutoff_datetime(CUTOFF_MODE)

        if mode == "block":
            # 1) Ordenar pela coluna de data
            table_sort_by_column(drive_id, item_id, DST_TABLE, session_id, date_col_idx, ascending=True)
            # 2) Obter corpo (sem headers), já ordenado
            body = get_table_databody_range(drive_id, item_id, DST_TABLE, session_id)
            values = body.get("values", [])
            if not values:
                print("Sem linhas no corpo da tabela.")
                return
            # 3) Contar quantas linhas iniciais estão < cutoff
            delete_count = 0
            for row in values:
                val = row[date_col_idx] if date_col_idx < len(row) else None
                dt = parse_date_any(val)
                if dt is None or dt >= cutoff:
                    break
                delete_count += 1
            if delete_count == 0:
                print("Nenhuma linha para remover (já só tens últimos 24 meses).")
                return
            # 4) Construir o endereço A1 do bloco a apagar
            address = body.get("address")  # e.g. "Folha1!A2:Z100"
            sheet_name, start_a1, end_a1 = _parse_a1_address(address)
            if not sheet_name:
                raise RuntimeError(f"Endereço inesperado: {address}")
            start_col, start_row = _split_col_row(start_a1)
            end_col, _end_row = _split_col_row(end_a1)
            del_start = start_row
            del_end = start_row + delete_count - 1
            del_addr = f"{start_col}{del_start}:{end_col}{del_end}"
            print(f"del_start: {del_start} - del_end: {del_end} - del_addr: {del_addr}")
            # 5) Apagar o bloco de uma só vez
            delete_range_on_sheet(drive_id, item_id, sheet_name, del_addr, session_id)
            print(f"[BLOCK] Removidas {delete_count} linhas anteriores a {cutoff.date()} (1 operação).")

        elif mode == "batch":
            # Ler todas as linhas por paginação e calcular índices a apagar
            indices_to_delete = []
            for r in list_table_rows_paged(drive_id, item_id, DST_TABLE, session_id, top=DEFAULT_TOP):
                idx = r.get("index")
                vals = (r.get("values", [[]])[0] or [])
                if idx is None or len(vals) <= date_col_idx:
                    continue
                dt = parse_date_any(vals[date_col_idx])
                if dt is None:
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

# entrada do script
if __name__ == "__main__":
    keep_last_24_months(mode=MODE)
