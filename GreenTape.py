import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID    = os.getenv("TENANT_ID")
CLIENT_ID    = os.getenv("CLIENT_ID")
CLIENT_SECRET= os.getenv("CLIENT_SECRET")
SITE_HOSTNAME= os.getenv("SITE_HOSTNAME")
SITE_PATH    = os.getenv("SITE_PATH")

# Ficheiro e tabela de ORIGEM (de onde lemos tudo)
SRC_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
SRC_TABLE     = "Historico"
SRC_SHEET     = "LstPrd"               # apenas informativo

# Destino: NOVA folha + NOVA tabela (com apenas os últimos 24 meses)
DST_SHEET     = "Historico24M"         # será criada se não existir
DST_TABLE     = "Historico24M"         # nome fixo final (renomeado após criação)

DATE_COLUMN   = "Data Entrega"

DEFAULT_TOP        = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
IMPORT_CHUNK_SIZE  = int(os.getenv("IMPORT_CHUNK_SIZE") or "2000")

# ========================== AUTH ==========================
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def workbook_headers(session_id: str) -> dict:
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h


# ========================== GRAPH BASICS ==========================
def get_site_id():
    r = requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def get_drive_id(site_id: str):
    r = requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def get_item_id(drive_id: str, path: str):
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{path}", headers=base_headers)
    r.raise_for_status()
    return r.json()["id"]

def create_session(drive_id: str, item_id: str):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers,
        data=json.dumps({"persistChanges": True}),
    )
    r.raise_for_status()
    return r.json()["id"]

def close_session(drive_id: str, item_id: str, session_id: str):
    h = workbook_headers(session_id)
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)


# ========================== HEADERS ORIGEM ==========================
def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)

    # 1) headerRowRange
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange",
        headers=h
    )
    if r.ok:
        vals = r.json().get("values", [[]])
        if vals and vals[0]:
            print("[DEBUG] headers =", vals[0])
            return [str(x) for x in vals[0]]

    # 2) columns
    r2 = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns",
        headers=h
    )
    if r2.ok:
        cols = r2.json().get("value", [])
        names = [c.get("name") for c in cols if c.get("name") is not None]
        if names:
            print("[DEBUG] headers via columns =", names)
            return names

    # 3) range (primeira linha)
    r3 = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    if r3.ok:
        vals = r3.json().get("values", [[]])
        if vals and vals[0]:
            print("[DEBUG] headers via range =", vals[0])
            return [str(x) for x in vals[0]]

    # Se chegámos aqui, falhou
    try: print("[DEBUG] headerRowRange:", r.json())
    except: pass
    try: print("[DEBUG] columns:", r2.json())
    except: pass
    try: print("[DEBUG] range:", r3.json())
    except: pass
    raise RuntimeError("Não consegui obter headers da tabela de origem.")


# ========================== LEITURA PAGINADA ORIGEM ==========================
def list_rows_paged(drive_id, item_id, table_name, session_id, top=DEFAULT_TOP):
    h = workbook_headers(session_id)
    base = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    skip = 0
    while True:
        url = f"{base}?$top={top}&$skip={skip}"
        r = requests.get(url, headers=h)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        yield from batch
        skip += top


# ========================== DATAS ==========================
def months_ago(dt, months):
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)

def cutoff_datetime():
    now = datetime.now(timezone.utc) - timedelta(days=1)
    # últimos 24 meses rolling
    return months_ago(now, 24)

def parse_date_any(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
        return epoch + timedelta(days=float(v))
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except:
                pass
    return None


# ========================== SHEETS (DESTINO) ==========================
def get_or_create_sheet(drive_id, item_id, session_id, sheet_name):
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets",
        headers=h
    )
    r.raise_for_status()
    sheets = r.json().get("value", [])
    for s in sheets:
        if s["name"].strip().lower() == sheet_name.lower():
            return s["name"]
    # criar
    r2 = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/add",
        headers=h, data=json.dumps({"name": sheet_name})
    )
    r2.raise_for_status()
    return sheet_name

def write_headers_to_sheet(drive_id, item_id, session_id, sheet_name, headers):
    """
    Escreve a linha de cabeçalhos na linha 1 (A1:...1).
    Isto 'materializa' a folha e garante que tables/add funciona.
    """
    h = workbook_headers(session_id)
    col_end = chr(ord("A") + len(headers) - 1)  # A..Z (assumindo <= 26 colunas; se precisares, posso fazer AA, AB, ...)
    addr = f"'{sheet_name}'!A1:{col_end}1"
    body = {"values": [headers]}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{addr}')"
    r = requests.patch(url, headers=h, data=json.dumps(body))
    if not r.ok:
        print("[DEBUG][WRITE HEADERS] status:", r.status_code)
        try: print("[DEBUG][WRITE HEADERS] json:", r.json())
        except: print("[DEBUG][WRITE HEADERS] text:", r.text)
        r.raise_for_status()


# ========================== TABELAS (DESTINO) ==========================
def create_table_and_get_name(drive_id, item_id, session_id, sheet_name, col_count):
    """
    Cria uma nova tabela baseada na linha 1 (headers já escritos) e devolve o NOME real atribuído pelo Graph (ex.: 'Tabela3').
    """
    h = workbook_headers(session_id)
    col_end = chr(ord("A") + col_count - 1)
    address = f"'{sheet_name}'!A1:{col_end}1"
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables/add"
    body = {"address": address, "hasHeaders": True}
    r = requests.post(url, headers=h, data=json.dumps(body))
    if not r.ok:
        print("[DEBUG][TABLE ADD] status:", r.status_code)
        try: print("[DEBUG][TABLE ADD] json:", r.json())
        except: print("[DEBUG][TABLE ADD] text:", r.text)
        r.raise_for_status()

    # Consulta as tabelas desta sheet e assume a última como a recém-criada
    r2 = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/tables",
        headers=h
    )
    r2.raise_for_status()
    tables = r2.json().get("value", [])
    if not tables:
        raise RuntimeError("Nenhuma tabela encontrada após tables/add.")
    real_name = tables[-1]["name"]
    return real_name

def list_workbook_tables(drive_id, item_id, session_id):
    h = workbook_headers(session_id)
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables", headers=h)
    r.raise_for_status()
    return r.json().get("value", [])

def delete_table_if_exists(drive_id, item_id, session_id, table_name):
    """
    Remove uma tabela pelo nome (se existir), para evitar conflito quando formos renomear.
    """
    h = workbook_headers(session_id)
    tables = list_workbook_tables(drive_id, item_id, session_id)
    for t in tables:
        if t.get("name") == table_name:
            url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}"
            rr = requests.delete(url, headers=h)
            if rr.status_code not in (200, 204, 404):
                try: print("[DEBUG][DEL TABLE] json:", rr.json())
                except: print("[DEBUG][DEL TABLE] text:", rr.text)
                rr.raise_for_status()
            print(f"[INFO] Tabela antiga '{table_name}' removida.")
            return True
    return False

def rename_table(drive_id, item_id, session_id, current_name, new_name):
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{current_name}"
    body = {"name": new_name}
    r = requests.patch(url, headers=h, data=json.dumps(body))
    if not r.ok:
        print("[DEBUG][RENAME] status:", r.status_code)
        try: print("[DEBUG][RENAME] json:", r.json())
        except: print("[DEBUG][RENAME] text:", r.text)
        r.raise_for_status()
    print(f"[INFO] Tabela '{current_name}' → '{new_name}'")
    return new_name


# ========================== INSERÇÃO EM CHUNKS ==========================
def add_rows_chunked(drive_id, item_id, table_name, session_id, rows, chunk=IMPORT_CHUNK_SIZE):
    if not rows:
        return 0
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"
    total = 0
    start = 0
    n = len(rows)
    while start < n:
        end = min(start + chunk, n)
        body = {"index": None, "values": rows[start:end]}
        r = requests.post(url, headers=h, data=json.dumps(body))
        if r.status_code == 429:
            import time
            wait = int(r.headers.get("Retry-After", "5"))
            print(f"[WARN][ADD] 429 → aguardar {wait}s")
            time.sleep(wait)
            continue
        if not r.ok:
            try: print("[DEBUG][ADD] json:", r.json())
            except: print("[DEBUG][ADD] text:", r.text)
            r.raise_for_status()
        total += (end - start)
        start = end
    return total


# ========================== MAIN ==========================
def keep_last_24_months():
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, SRC_FILE_PATH)
    session_id = create_session(drive_id, item_id)

    try:
        # 1) Headers + índice da coluna de data (origem)
        headers = get_table_headers_safe(drive_id, item_id, SRC_TABLE, session_id)
        if DATE_COLUMN not in headers:
            raise RuntimeError(f"Coluna '{DATE_COLUMN}' não encontrada nos headers: {headers}")
        date_idx = headers.index(DATE_COLUMN)

        # 2) Cutoff
        cutoff = cutoff_datetime()
        print("[INFO] Cutoff:", cutoff.date())

        # 3) Ler e filtrar (últimos 24 meses)
        rows_filtered = []
        for row in list_rows_paged(drive_id, item_id, SRC_TABLE, session_id, top=DEFAULT_TOP):
            vals = (row.get("values", [[]])[0] or [])
            if len(vals) <= date_idx:
                continue
            dt = parse_date_any(vals[date_idx])
            if dt and dt >= cutoff:
                rows_filtered.append(vals)
        print("[INFO] Linhas a manter:", len(rows_filtered))

        # 4) Garantir a folha de destino e escrever headers na A1
        real_sheet = get_or_create_sheet(drive_id, item_id, session_id, DST_SHEET)
        write_headers_to_sheet(drive_id, item_id, session_id, real_sheet, headers)

        # 5) Criar nova tabela (Graph devolve nome auto, ex.: 'Tabela3')
        created_name = create_table_and_get_name(drive_id, item_id, session_id, real_sheet, len(headers))
        print("[INFO] Tabela criada (nome automático):", created_name)

        # 6) Normalizar nome final da tabela (apaga se já existir 'DST_TABLE', depois renomeia a recém-criada)
        if created_name != DST_TABLE:
            delete_table_if_exists(drive_id, item_id, session_id, DST_TABLE)
            real_table = rename_table(drive_id, item_id, session_id, created_name, DST_TABLE)
        else:
            real_table = created_name

        # 7) Inserir dados filtrados
        inserted = add_rows_chunked(drive_id, item_id, real_table, session_id, rows_filtered)
        print(f"[INFO] Inseridas: {inserted} linhas em '{real_table}' (sheet '{real_sheet}').")

    finally:
        close_session(drive_id, item_id, session_id)


if __name__ == "__main__":
    keep_last_24_months()
