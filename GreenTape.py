import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH     = os.getenv("SITE_PATH")

# ORIGEM (de onde lemos e filtramos)
SRC_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
SRC_TABLE     = "Historico"

# DESTINO (onde vamos ADICIONAR as linhas filtradas)
DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
DST_TABLE     = "Meses"

# Nome da coluna de data (tem de existir na tabela de origem)
DATE_COLUMN   = "Data Entrega"

# Tamanho da paginação de leitura (origem) e chunks de inserção (destino)
DEFAULT_TOP        = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
IMPORT_CHUNK_SIZE  = int(os.getenv("IMPORT_CHUNK_SIZE") or "2000")
IMPORT_MAX_RETRIES = int(os.getenv("IMPORT_MAX_RETRIES") or "3")

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


# ========================== HEADERS ==========================
def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    """Obtém headers por headerRowRange; se falhar, tenta /columns e /range (1ª linha)."""
    h = workbook_headers(session_id)

    # 1) headerRowRange
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange",
        headers=h
    )
    if r.ok:
        vals = r.json().get("values", [[]])
        if vals and vals[0]:
            return [str(x) for x in vals[0]]

    # 2) columns
    rc = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns",
        headers=h
    )
    if rc.ok:
        cols = rc.json().get("value", [])
        names = [c.get("name") for c in cols if c.get("name") is not None]
        if names:
            return names

    # 3) range (primeira linha)
    rr = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    if rr.ok:
        vals = rr.json().get("values", [[]])
        if vals and vals[0]:
            return [str(x) for x in vals[0]]

    # Erro detalhado
    try: print("[DEBUG] headerRowRange:", r.json())
    except: pass
    try: print("[DEBUG] columns:", rc.json())
    except: pass
    try: print("[DEBUG] range:", rr.json())
    except: pass
    raise RuntimeError(f"Não consegui obter headers da tabela '{table_name}'.")


# ========================== LEITURA PAGINADA (ORIGEM) ==========================
def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=DEFAULT_TOP):
    """Itera as linhas da tabela por $top/$skip para evitar payloads grandes."""
    h = workbook_headers(session_id)
    base = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    skip = 0
    while True:
        url = f"{base}?$top={top}&$skip={skip}"
        r = requests.get(url, headers=h)
        if not r.ok:
            print("[DEBUG][paged] status:", r.status_code)
            try: print("[DEBUG][paged] json:", r.json())
            except: print("[DEBUG][paged] text:", r.text)
            r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        for row in batch:
            yield row
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
    # últimos 24 meses (rolling)
    return months_ago(now, 24)

def parse_date_any(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
        return epoch + timedelta(days=float(v))
    if isinstance(v, str):
        s = v.strip()
        # formatos comuns (ISO e PT)
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except:
                pass
    return None


# ========================== MAPEAMENTO DE COLUNAS ==========================
def reorder_values_by_headers(src_headers, dst_headers, row_values):
    """
    Reordena uma linha 'row_values' que vem na ordem de 'src_headers'
    para a ordem de 'dst_headers' (match por nome exato do header).
    Se um header de destino não existir na origem, coloca None.
    """
    pos = {name: i for i, name in enumerate(src_headers)}
    out = []
    for name in dst_headers:
        idx = pos.get(name)
        out.append(row_values[idx] if idx is not None and idx < len(row_values) else None)
    return out


# ========================== INSERT EM CHUNKS (DESTINO) ==========================
def add_rows_chunked(drive_id, item_id, table_name, session_id, rows_2d,
                     chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES):
    """Insere linhas no fim da tabela em chunks; trata 429 e payload-grande dividindo o chunk."""
    if not rows_2d:
        return 0

    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"

    total = 0
    start = 0
    n = len(rows_2d)

    def post_chunk(vals, attempt=1, local_chunk_size=None):
        nonlocal total
        body = {"index": None, "values": vals}
        r = requests.post(url, headers=h, data=json.dumps(body))

        # throttling
        if r.status_code == 429:
            ra = int(r.headers.get("Retry-After", "5"))
            print(f"[WARN][ADD] 429 TooManyRequests. Aguardar {ra}s…")
            import time; time.sleep(ra)
            return post_chunk(vals, attempt+1, local_chunk_size)

        if not r.ok:
            try:
                err = r.json()
            except:
                err = {"error": {"message": r.text}}
            # payload grande → dividir
            code = str(err.get("error", {}).get("code", "")).lower()
            inner = str(err.get("error", {}).get("innerError", {}).get("code", "")).lower()
            if "responsepayloadsizelimitexceeded" in (code + inner) or "requestentitytoolarge" in (code + inner):
                if len(vals) <= 1:
                    print("[ERROR][ADD] Payload limit com 1 linha — abortar.")
                    r.raise_for_status()
                mid = len(vals) // 2
                print(f"[WARN][ADD] Payload grande. A dividir: {len(vals)} → {mid} + {len(vals)-mid}")
                ok1 = post_chunk(vals[:mid], attempt+1, mid)
                ok2 = post_chunk(vals[mid:], attempt+1, len(vals)-mid)
                return ok1 and ok2

            if attempt <= max_retries:
                print(f"[WARN][ADD] Falhou (tentativa {attempt}). A tentar de novo…")
                return post_chunk(vals, attempt+1, local_chunk_size)

            print("[DEBUG][ADD] STATUS:", r.status_code, "BODY:", err)
            r.raise_for_status()

        total += len(vals)
        return True

    while start < n:
        end = min(start + chunk_size, n)
        chunk = rows_2d[start:end]
        post_chunk(chunk, attempt=1, local_chunk_size=len(chunk))
        print(f"[DEBUG][ADD] OK ({len(chunk)}) total={total}/{n}")
        start = end

    return total


# ========================== MAIN ==========================
def keep_last_24_months():
    # Ids e sessões
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)

    src_item_id = get_item_id(drive_id, SRC_FILE_PATH)
    dst_item_id = get_item_id(drive_id, DST_FILE_PATH)

    src_sid = create_session(drive_id, src_item_id)
    dst_sid = create_session(drive_id, dst_item_id)

    try:
        # Headers origem/destino
        src_headers = get_table_headers_safe(drive_id, src_item_id, SRC_TABLE, src_sid)
        dst_headers = get_table_headers_safe(drive_id, dst_item_id, DST_TABLE, dst_sid)

        if DATE_COLUMN not in src_headers:
            raise RuntimeError(f"A coluna '{DATE_COLUMN}' não existe na tabela de origem '{SRC_TABLE}'.")
        date_idx = src_headers.index(DATE_COLUMN)

        print("[INFO] src_headers:", src_headers)
        print("[INFO] dst_headers:", dst_headers)

        # Cutoff
        cutoff = cutoff_datetime()
        print("[INFO] Cutoff (24m rolling):", cutoff.date())

        # Ler origem (paginado) + filtrar + reordenar para o destino
        to_import = []
        total_read = 0

        for r in list_table_rows_paged(drive_id, src_item_id, SRC_TABLE, src_sid, top=DEFAULT_TOP):
            vals = (r.get("values", [[]])[0] or [])
            total_read += 1
            if len(vals) <= date_idx:
                continue
            dt = parse_date_any(vals[date_idx])
            if dt and dt >= cutoff:
                to_import.append(reorder_values_by_headers(src_headers, dst_headers, vals))

        print(f"[INFO] Lidas {total_read} linhas de origem; a importar {len(to_import)} linhas para '{DST_TABLE}'.")

        if not to_import:
            print("[OK] Nada para inserir (nenhuma linha >= cutoff).")
            return

        # Inserir no destino (append)
        inserted = add_rows_chunked(drive_id, dst_item_id, DST_TABLE, dst_sid, to_import,
                                    chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES)
        print(f"[OK] Inseridas {inserted} linhas no destino '{DST_TABLE}' ({DST_FILE_PATH}).")

    finally:
        # Fechar sessões
        close_session(drive_id, src_item_id, src_sid)
        close_session(drive_id, dst_item_id, dst_sid)


if __name__ == "__main__":
    keep_last_24_months()
``
