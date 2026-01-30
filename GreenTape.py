import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar
import string

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

DEFAULT_TOP = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
IMPORT_CHUNK_SIZE = 2000
IMPORT_MAX_RETRIES = 3

CUTOFF_MODE = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" ou "fullmonth"
# ==========================


# ---- Autenticação ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ---- Helpers base Graph ----
def get_site_id():
    return requests.get(
        f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}",
        headers=base_headers
    ).json()["id"]


def get_drive_id(site_id):
    return requests.get(
        f"{GRAPH_BASE}/sites/{site_id}/drive",
        headers=base_headers
    ).json()["id"]


def get_item_id(drive_id, path):
    return requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/root:{path}",
        headers=base_headers
    ).json()["id"]


def create_session(drive_id, item_id):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers,
        data=json.dumps({"persistChanges": True}),
    )
    return r.json()["id"]


def close_session(drive_id, item_id, session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession",
        headers=h
    )


def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h


# ---- Headers seguros ----
def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)

    # 1) headerRowRange
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange",
        headers=h
    )
    if r.ok:
        values = r.json().get("values", [[]])
        if values and values[0]:
            print("[DEBUG] headerRowRange →", values[0])
            return [str(x) for x in values[0]]
    else:
        print("[DEBUG] headerRowRange falhou:", r.status_code)

    # 2) columns
    rc = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns",
        headers=h
    )
    if rc.ok:
        cols = rc.json().get("value", [])
        names = [c.get("name") for c in cols if c.get("name") is not None]
        if names:
            print("[DEBUG] fallback columns →", names)
            return names

    # 3) range → primeira linha
    rr = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    if rr.ok:
        vals = rr.json().get("values", [[]])
        if vals and vals[0]:
            print("[DEBUG] fallback range →", vals[0])
            return [str(x) for x in vals[0]]

    raise RuntimeError("Não foi possível obter headers da tabela.")


# ---- Leitura paginada ----
def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=DEFAULT_TOP):
    h = workbook_headers(session_id)
    base_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"

    skip = 0
    while True:
        url = f"{base_url}?$top={top}&$skip={skip}"
        r = requests.get(url, headers=h)
        if not r.ok:
            print("[DEBUG][paged] ERROR:", r.status_code)
            try: print(r.json())
            except: print(r.text)
            r.raise_for_status()

        rows = r.json().get("value", [])
        if not rows:
            break

        print(f"[DEBUG] rows page skip={skip} count={len(rows)}")
        for row in rows:
            yield row

        skip += top


# ---- Datas ----
def months_ago(dt: datetime, months: int) -> datetime:
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)


def cutoff_datetime(mode="rolling"):
    now_utc = datetime.now(timezone.utc) - timedelta(days=1)
    if mode == "fullmonth":
        start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return months_ago(start, 24)
    return months_ago(now_utc, 24)


def parse_date_any(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        try:
            excel_epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
            return excel_epoch + timedelta(days=float(v))
        except:
            return None
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except:
                pass
    return None


# ---- CLEAR ----
def clear_table_body(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange/clear"
    r = requests.post(url, headers=h)
    if not r.ok:
        print("[DEBUG] CLEAR falhou")
        try: print(r.json())
        except: print(r.text)
        r.raise_for_status()


# ---- INSERT em chunks (método do Implementacoes) ----
def add_rows_chunked(drive_id, item_id, table_name, session_id, rows_2d,
                     chunk_size=2000, max_retries=3):

    if not rows_2d:
        return 0

    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"

    total = 0
    start = 0
    n = len(rows_2d)

    while start < n:
        end = min(start + chunk_size, n)
        chunk = rows_2d[start:end]

        body = {"index": None, "values": chunk}

        print(f"[DEBUG][ADD] rows={len(chunk)} start={start}")
        r = requests.post(url, headers=h, data=json.dumps(body))

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            print(f"[DEBUG][ADD] 429 → esperar {wait}s")
            import time; time.sleep(wait)
            continue

        if not r.ok:
            try:
                err = r.json()
            except:
                err = {"error": {"message": r.text}}

            print("[DEBUG][ADD] ERROR:", err)

            if "Payload" in str(err):
                mid = len(chunk) // 2
                print("[DEBUG][ADD] payload grande → dividir chunk")
                add_rows_chunked(drive_id, item_id, table_name, session_id, chunk[:mid])
                add_rows_chunked(drive_id, item_id, table_name, session_id, chunk[mid:])
                return total + len(chunk)

            if max_retries > 0:
                max_retries -= 1
                continue

            r.raise_for_status()

        total += len(chunk)
        start = end

    return total


# ---- RESIZE final para remover linhas vazias ----
def resize_table(drive_id, item_id, table_name, session_id, sheet_name, headers, row_count):
    """
    Redimensiona a tabela para caber exatamente o header + rows inseridas.
    Remove completamente as linhas vazias criadas pelo CLEAR.
    """

    col_start = "A"
    
    # Converte número de colunas para letras (A, B, C ... Z, AA, AB, ...)
    def excel_col(n):
        result = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            result = chr(65 + r) + result
        return result

    col_end = excel_col(len(headers))  # última coluna (ex: T)

    # Header = linha 1; corpo = linhas 2..(1+row_count)
    last_row = 1 + row_count

    range_address = f"'{sheet_name}'!{col_start}1:{col_end}{last_row}"
    print("[INFO] RESIZE →", range_address)

    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/resize"

    r = requests.post(url, headers=h, data=json.dumps({"range": range_address}))
    if not r.ok:
        print("[DEBUG][RESIZE] Falhou")
        try: print(r.json())
        except: print(r.text)
        r.raise_for_status()


# ---- FUNÇÃO PRINCIPAL ----
def keep_last_24_months(mode="block"):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, DST_FILE_PATH)
    session_id = create_session(drive_id, item_id)

    try:
        headers = get_table_headers_safe(drive_id, item_id, DST_TABLE, session_id)
        if DATE_COLUMN not in headers:
            raise RuntimeError(f"Coluna '{DATE_COLUMN}' não encontrada.")
        date_idx = headers.index(DATE_COLUMN)

        cutoff = cutoff_datetime(CUTOFF_MODE)
        print("[INFO] Cutoff:", cutoff.date())

        # 1) ORDENAR a tabela
        print("[INFO] A ordenar tabela…")
        h = workbook_headers(session_id)
        sort_body = {
            "fields": [{"key": date_idx, "ascending": True}],
            "matchCase": False
        }
        r = requests.post(
            f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{DST_TABLE}/sort/apply",
            headers=h, data=json.dumps(sort_body)
        )
        r.raise_for_status()

        # 2) LER TODAS AS LINHAS → filtrar as que queremos manter
        print("[INFO] A ler tabela paginada…")
        rows_to_keep = []

        for r in list_table_rows_paged(drive_id, item_id, DST_TABLE, session_id):
            vals = (r.get("values", [[]])[0] or [])
            if len(vals) <= date_idx:
                continue

            dt = parse_date_any(vals[date_idx])
            if dt is None:
                continue

            if dt >= cutoff:
                rows_to_keep.append(vals)

        print(f"[INFO] Linhas a manter: {len(rows_to_keep)}")

        # 3) CLEAR TOTAL (super rápido)
        print("[INFO] CLEAR total do corpo da tabela…")
        clear_table_body(drive_id, item_id, DST_TABLE, session_id)

        # 4) INSERIR apenas as linhas boas
        print("[INFO] Inserir linhas filtradas…")
        inserted = add_rows_chunked(
            drive_id, item_id, DST_TABLE, session_id,
            rows_to_keep,
            chunk_size=IMPORT_CHUNK_SIZE
        )

        print(f"[INFO] Inseridas {inserted} linhas.")

        # 5) RESIZE final para remover linhas vazias restantes
        print("[INFO] Redimensionar tabela…")
        sheet_name = DST_TABLE  # Por norma a tabela tem o mesmo nome da sheet; se não tiver diz-me!
        resize_table(drive_id, item_id, DST_TABLE, session_id, sheet_name, headers, inserted)

    finally:
        close_session(drive_id, item_id, session_id)


# ---- ENTRYPOINT ----
if __name__ == "__main__":
    keep_last_24_months()
