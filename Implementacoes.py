import os, json, requests, msal, time
from datetime import datetime, timedelta

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

SRC_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Implementacoes e Materiais Mensal.xlsx"
SRC_TABLE      = "TabelaAutomatica"

DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/Implementacoes e Materiais.xlsx"
DST_TABLE      = "Historico"

DATE_COLUMN    = "Data da visita"

# Paginação e sweep (podes alterar via ENV)
DEFAULT_TOP           = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
DEFAULT_SWEEP_GROUP   = int(os.getenv("SWEEP_GROUP_SIZE") or "500")

# Importação em chunks (ENV)
IMPORT_CHUNK_SIZE     = int(os.getenv("IMPORT_CHUNK_SIZE") or "2000")
IMPORT_MAX_RETRIES    = int(os.getenv("IMPORT_MAX_RETRIES") or "3")
IMPORT_USE_BATCH      = (os.getenv("IMPORT_USE_BATCH") or "false").lower() == "true"
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

def create_session(drive_id, item_id, max_retries=8):
    """
    Opens a workbook session with retry logic.
    504 on createSession means the file is taking too long to load in Excel Online —
    use longer waits between retries rather than short exponential backoff.
    """
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession"
    for attempt in range(1, max_retries + 1):
        r = requests.post(url, headers=base_headers, data=json.dumps({"persistChanges": True}))
        body = {}
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}

        # 429 – throttled: respect Retry-After header
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10"))
            print(f"[create_session] 429 throttled. Waiting {wait}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
            continue

        # Success
        if r.ok and "id" in body:
            print(f"[create_session] OK on attempt {attempt}")
            return body["id"]

        # 504 – file taking too long to load: wait longer between retries
        if r.status_code == 504:
            # Use longer fixed waits: 30s, 60s, 60s, 90s, 90s, 120s, 120s...
            wait_schedule = [30, 60, 60, 90, 90, 120, 120]
            wait = wait_schedule[min(attempt - 1, len(wait_schedule) - 1)]
            print(f"[create_session] 504 Gateway Timeout (file loading too slowly). "
                  f"Waiting {wait}s before retry (attempt {attempt}/{max_retries})")
            print(f"[create_session] item_id: {item_id} | error: {body}")
            if attempt < max_retries:
                time.sleep(wait)
            continue

        # Any other failure
        print(f"[create_session] FAILED. Status: {r.status_code} | item_id: {item_id}")
        print(f"[create_session] Response body: {body}")

        if attempt < max_retries:
            wait = 2 ** attempt
            print(f"[create_session] Retrying in {wait}s… (attempt {attempt}/{max_retries})")
            time.sleep(wait)
        else:
            raise RuntimeError(
                f"createSession failed after {max_retries} attempts for item_id={item_id}. "
                f"Last status: {r.status_code} | Last response: {body}"
            )

    raise RuntimeError(f"createSession exhausted {max_retries} attempts for item_id={item_id}.")

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

# ---- DEBUG helpers ----
def list_tables(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][list_tables] URL:", url)
        print("[DEBUG][list_tables] STATUS:", r.status_code)
        try:
            print("[DEBUG][list_tables] JSON:", r.json())
        except Exception:
            print("[DEBUG][list_tables] TEXT:", r.text)
        r.raise_for_status()
    data = r.json().get("value", [])
    print(f"[DEBUG] Tabelas no ficheiro (item_id={item_id}):")
    for t in data:
        ws_name = (t.get("worksheet") or {}).get("name")
        print(" - id:", t.get("id"),
              "| name:", t.get("name"),
              "| showHeaders:", t.get("showHeaders"),
              "| worksheet:", ws_name)
    return data

def get_table_headers(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][headerRowRange] URL:", url)
        print("[DEBUG][headerRowRange] STATUS:", r.status_code)
        try:
            print("[DEBUG][headerRowRange] JSON:", r.json())
        except Exception:
            print("[DEBUG][headerRowRange] TEXT:", r.text)
        r.raise_for_status()
    rng = r.json()
    values = rng.get("values", [[]])
    headers = [str(x) for x in (values[0] if values and values[0] else [])]
    return headers

def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    # 1) tentativa oficial
    try:
        headers = get_table_headers(drive_id, item_id, table_name, session_id)
        if headers:
            print(f"[DEBUG] headerRowRange → {headers}")
            return headers
    except requests.HTTPError:
        print("[DEBUG] headerRowRange falhou; a tentar fallback por /columns...")

    # 2) /columns -> names
    h = dict(base_headers); h["workbook-session-id"] = session_id
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

    rr.raise_for_status()

# ---- Listar rows com paginação ($top/$skip)
def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=None, max_pages=100000):
    if top is None:
        top = DEFAULT_TOP

    h = dict(base_headers); h["workbook-session-id"] = session_id
    base_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    skip = 0; page = 0; total = 0

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
            print(f"[DEBUG][list_table_rows_paged] Fim paginação. total={total}, páginas={page-1}, top_final={top}")
            break

        print(f"[DEBUG][list_table_rows_paged] page={page} top={top} skip={skip} count={len(batch)}")
        for row in batch:
            total += 1
            yield row
        skip += top

# ---- Inserir rows (importação repartida)
def add_rows_chunked_sequential(drive_id, item_id, table_name, session_id, values_2d,
                                chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES):
    if not values_2d:
        return 0

    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"
    total = 0; start = 0; n = len(values_2d)

    def post_chunk(vals, attempt=1):
        body = {"index": None, "values": vals}
        print(f"[DEBUG][ADD-CHUNK] POST {url} rows={len(vals)} attempt={attempt} chunk_size={chunk_size}")
        r = requests.post(url, headers=h, data=json.dumps(body))
        if r.status_code == 429:
            ra = int(r.headers.get("Retry-After", "5"))
            print(f"[DEBUG][ADD-CHUNK] 429 TooManyRequests. A aguardar {ra}s…")
            time.sleep(ra)
            return post_chunk(vals, attempt+1 if attempt <= max_retries else attempt)
        if not r.ok:
            try:
                err = r.json()
            except Exception:
                err = {"error": {"message": r.text}}
            code = (err.get("error", {}) or {}).get("code", "")
            inner = (err.get("error", {}) or {}).get("innerError", {}) or {}
            inner_code = inner.get("code", "") or ""
            if (str(code).lower() in ("responsepayloadsizelimitexceeded","requestentitytoolarge")
                or str(inner_code).lower() in ("responsepayloadsizelimitexceeded","requestentitytoolarge")):
                new_size = max(100, len(vals) // 2)
                print(f"[DEBUG][ADD-CHUNK] Payload grande. A dividir o chunk {len(vals)}→{new_size}.")
                mid = len(vals) // 2
                c1, c2 = vals[:mid], vals[mid:]
                ok1 = post_chunk(c1, attempt+1 if attempt <= max_retries else attempt)
                ok2 = post_chunk(c2, attempt+1 if attempt <= max_retries else attempt)
                return ok1 and ok2
            if r.status_code == 504 and attempt <= max_retries:
                print("[DEBUG][ADD-CHUNK] 504 Gateway Timeout. A repetir…")
                return post_chunk(vals, attempt+1)
            print("[DEBUG][ADD-CHUNK] STATUS:", r.status_code, "| BODY:", err)
            r.raise_for_status()
        return True

    while start < n:
        end = min(start + chunk_size, n)
        chunk = values_2d[start:end]
        ok = post_chunk(chunk, attempt=1)
        if ok:
            total += len(chunk)
            print(f"[DEBUG][ADD-CHUNK] OK ({len(chunk)}) total={total}/{n}")
            start = end
        else:
            start = end

    print(f"[DEBUG][ADD-CHUNK] Inseridas (sequencial): {total}")
    return total

def add_rows_chunked_batch(drive_id, item_id, table_name, session_id, values_2d,
                           chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES):
    if not values_2d:
        return 0
    batch_endpoint = f"{GRAPH_BASE}/$batch"
    total = 0; start = 0; n = len(values_2d)

    def chunks(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i+size]

    while start < n:
        end = min(start + chunk_size, n)
        big_chunk = values_2d[start:end]
        print(f"[DEBUG][ADD-BATCH] Preparar chunk rows={len(big_chunk)}")
        subchunks = list(chunks(big_chunk, max(len(big_chunk)//20, 100)))
        requests_list = []
        for i, sc in enumerate(subchunks, start=1):
            rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"
            requests_list.append({
                "id": str(i),
                "method": "POST",
                "url": rel_url,
                "headers": {"workbook-session-id": session_id, "Content-Type": "application/json"},
                "body": {"index": None, "values": sc}
            })
        payload = {"requests": requests_list}
        print(f"[DEBUG][ADD-BATCH] POST {batch_endpoint} subpedidos={len(requests_list)}")

        r = requests.post(batch_endpoint, headers=base_headers, data=json.dumps(payload))
        if r.status_code == 429 and max_retries > 0:
            ra = int(r.headers.get("Retry-After", "5"))
            print(f"[DEBUG][ADD-BATCH] 429 no batch. A aguardar {ra}s e repetir…")
            time.sleep(ra)
            r = requests.post(batch_endpoint, headers=base_headers, data=json.dumps(payload))

        if not r.ok:
            print("[DEBUG][ADD-BATCH] STATUS:", r.status_code)
            try: print("[DEBUG][ADD-BATCH] JSON:", r.json())
            except Exception: print("[DEBUG][ADD-BATCH] TEXT:", r.text)
            r.raise_for_status()

        resp = r.json()
        ok_count = sum(1 for e in resp.get("responses", []) if e.get("status") in (200, 204))
        inserted = sum(len(subchunks[int(e.get("id"))-1]) for e in resp.get("responses", []) if e.get("status") in (200, 204))
        total += inserted
        for e in resp.get("responses", []):
            if e.get("status") not in (200, 204):
                print("[DEBUG][ADD-BATCH] Falhou id", e.get("id"), "| status:", e.get("status"), "| body:", e.get("body"))
        print(f"[DEBUG][ADD-BATCH] OK {ok_count}/{len(subchunks)} | rows_inserted={inserted} | total={total}/{n}")
        start = end

    print(f"[DEBUG][ADD-BATCH] Inseridas (batch): {total}")
    return total

# ---- Outras helpers
def get_table_range(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][get_table_range] STATUS:", r.status_code)
        try: print("[DEBUG][get_table_range] JSON:", r.json())
        except Exception: print("[DEBUG][get_table_range] TEXT:", r.text)
        r.raise_for_status()
    return r.json().get("address")

def get_worksheet_id(drive_id, item_id, session_id, sheet_name):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][get_worksheet_id] STATUS:", r.status_code)
        try: print("[DEBUG][get_worksheet_id] JSON:", r.json())
        except Exception: print("[DEBUG][get_worksheet_id] TEXT:", r.text)
        r.raise_for_status()
    sheets = r.json().get("value", [])
    for s in sheets:
        if s.get("name") == sheet_name.strip("'"):
            return s.get("id")
    raise Exception(f"Folha '{sheet_name}' não encontrada.")

# ---- Utilidades Excel ----
def excel_value_to_date(v):
    if isinstance(v, (int, float)):
        return datetime(1899, 12, 30) + timedelta(days=float(v))
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try: return datetime.strptime(v, fmt)
            except: pass
    return None

def reorder_values_by_headers(src_headers, dst_headers, row_values):
    src_pos = {name: i for i, name in enumerate(src_headers)}
    return [row_values[src_pos.get(name)] if src_pos.get(name) is not None else None for name in dst_headers]

def month_bounds(d: datetime):
    first = datetime(d.year, d.month, 1).date()
    if d.month == 12:
        next_first = datetime(d.year + 1, 1, 1).date()
    else:
        next_first = datetime(d.year, d.month + 1, 1).date()
    last = next_first - timedelta(days=1)
    return first, last

def parse_range_address(address: str):
    sheet, cells = address.split("!")
    start, end = cells.split(":")
    import re
    m1 = re.match(r"([A-Z]+)(\d+)", start)
    m2 = re.match(r"([A-Z]+)(\d+)", end)
    return {
        "sheet": sheet.strip("'"),
        "start_col": m1.group(1),
        "start_row": int(m1.group(2)),
        "end_col": m2.group(1),
        "end_row": int(m2.group(2))
    }

# ---- DELETE via $batch (ItemAt) + sweep em grupos
def delete_table_rows_by_index_batch(
    drive_id, item_id, table_name, session_id, row_indices,
    max_batch_size=20, max_retries=3, fallback_sequential=False
):
    if not row_indices:
        print("[DEBUG][BATCH-DEL] Sem índices para apagar.")
        return {"deleted": 0, "failed": []}

    deleted_total = 0
    failed_global = []
    batch_endpoint = f"{GRAPH_BASE}/$batch"

    def chunks(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i+size]

    row_indices = sorted(set(row_indices), reverse=True)
    print(f"[DEBUG][BATCH-DEL] Total de índices para apagar: {len(row_indices)}")

    def delete_single(idx):
        rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
        abs_url = f"{GRAPH_BASE}{rel_url}"
        h = dict(base_headers); h["workbook-session-id"] = session_id
        print(f"[DEBUG][DEL-ONE] DELETE {abs_url}")
        r = requests.delete(abs_url, headers=h)
        if not r.ok:
            print("[DEBUG][DEL-ONE] STATUS:", r.status_code)
            try: print("[DEBUG][DEL-ONE] JSON:", r.json())
            except Exception: print("[DEBUG][DEL-ONE] TEXT:", r.text)
            return False
        return True

    for chunk in chunks(row_indices, max_batch_size):
        requests_list = []
        for i, idx in enumerate(chunk, start=1):
            rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
            requests_list.append({
                "id": str(i),
                "method": "DELETE",
                "url": rel_url,
                "headers": { "workbook-session-id": session_id }
            })

        print("[DEBUG][BATCH-DEL] URLs no lote:", [req["url"] for req in requests_list])

        payload = { "requests": requests_list }
        attempt = 0
        while True:
            attempt += 1
            print(f"[DEBUG][BATCH-DEL] POST {batch_endpoint} (lote {len(chunk)}, tentativa {attempt})")
            r = requests.post(batch_endpoint, headers=base_headers, data=json.dumps(payload))

            if r.status_code == 429 and attempt <= max_retries:
                ra = int(r.headers.get("Retry-After", "5"))
                print(f"[DEBUG][BATCH-DEL] 429 recebido. A aguardar {ra}s…")
                time.sleep(ra)
                continue

            if not r.ok:
                print("[DEBUG][BATCH-DEL] STATUS:", r.status_code)
                try: print("[DEBUG][BATCH-DEL] JSON:", r.json())
                except Exception: print("[DEBUG][BATCH-DEL] TEXT:", r.text)
                if fallback_sequential:
                    print("[DEBUG][BATCH-DEL] Falha no lote. Fallback sequencial.")
                    for idx in chunk:
                        if delete_single(idx):
                            deleted_total += 1
                        else:
                            failed_global.append(idx)
                break

            resp = r.json()
            ok_ids = [e for e in resp.get("responses", []) if e.get("status") in (200, 204)]
            deleted_total += len(ok_ids)

            for e in resp.get("responses", []):
                status = e.get("status")
                if status not in (200, 204):
                    body = e.get("body") or {}
                    print("[DEBUG][BATCH-DEL] Falhou id", e.get("id"),
                          "| status:", status, "| body:", body)
                    try:
                        failed_idx = chunk[int(e.get("id")) - 1]
                        failed_global.append(failed_idx)
                    except Exception:
                        pass
            break

    print(f"[DEBUG][BATCH-DEL] Total rows apagadas (batch): {deleted_total}")
    return {"deleted": deleted_total, "failed": sorted(set(failed_global), reverse=True)}

def find_month_row_indices(drive_id, item_id, table_name, session_id, date_idx, month_start, month_end, top=None):
    if top is None:
        top = DEFAULT_TOP
    indices = []
    for r in list_table_rows_paged(drive_id, item_id, table_name, session_id, top=top):
        idx = r.get("index")
        vals = (r.get("values", [[]])[0] or [])
        if idx is None or len(vals) <= date_idx:
            continue
        d = excel_value_to_date(vals[date_idx])
        if d and month_start <= d.date() <= month_end:
            indices.append(int(idx))
    return indices

def cleanup_month_rows_in_groups(
    drive_id, item_id, table_name, session_id,
    date_idx, month_start, month_end,
    group_size=DEFAULT_SWEEP_GROUP, top=DEFAULT_TOP, max_iters=10000
):
    total_deleted = 0; iters = 0
    while iters < max_iters:
        iters += 1
        indices = find_month_row_indices(drive_id, item_id, table_name, session_id, date_idx, month_start, month_end, top=top)
        if not indices:
            print(f"[DEBUG][SWEEP-GROUP] Nada restante. iters={iters-1} total_deleted={total_deleted}")
            break
        indices = sorted(set(indices), reverse=True)
        group = indices[:group_size]
        print(f"[DEBUG][SWEEP-GROUP] Iter {iters}: apagar {len(group)} de {len(indices)} restantes.")
        res = delete_table_rows_by_index_batch(
            drive_id, item_id, table_name, session_id, group,
            max_batch_size=20, max_retries=3, fallback_sequential=False
        )
        total_deleted += res["deleted"]
        failed = res["failed"]
        if failed:
            print(f"[DEBUG][SWEEP-GROUP] {len(failed)} falharam no grupo. Retry imediato dessas.")
            res2 = delete_table_rows_by_index_batch(
                drive_id, item_id, table_name, session_id, failed,
                max_batch_size=20, max_retries=3, fallback_sequential=False
            )
            total_deleted += res2["deleted"]
    print(f"[DEBUG][SWEEP-GROUP] Total removido no sweep em grupos: {total_deleted}")
    return total_deleted

# ---- Fluxo principal ----
site_id  = get_site_id()
drive_id = get_drive_id(site_id)
src_id   = get_item_id(drive_id, SRC_FILE_PATH)
dst_id   = get_item_id(drive_id, DST_FILE_PATH)

src_sid  = create_session(drive_id, src_id)
dst_sid  = create_session(drive_id, dst_id)

try:
    _ = list_tables(drive_id, src_id, src_sid)
    _ = list_tables(drive_id, dst_id, dst_sid)

    src_headers = get_table_headers_safe(drive_id, src_id, SRC_TABLE, src_sid)
    dst_headers = get_table_headers_safe(drive_id, dst_id, DST_TABLE, dst_sid)
    print("[DEBUG] src_headers:", src_headers)
    print("[DEBUG] dst_headers:", dst_headers)

    if DATE_COLUMN not in src_headers or DATE_COLUMN not in dst_headers:
        raise Exception(f"A coluna '{DATE_COLUMN}' não existe em uma das tabelas.")

    date_idx_src = src_headers.index(DATE_COLUMN)
    date_idx_dst = dst_headers.index(DATE_COLUMN)

    today = datetime.today() - timedelta(days=1)
    month_start, month_end = month_bounds(today)
    print(f"[DEBUG] Mês atual: {month_start} a {month_end}")

    to_import = []
    for r in list_table_rows_paged(drive_id, src_id, SRC_TABLE, src_sid, top=DEFAULT_TOP):
        vals = (r.get("values", [[]])[0] or [])
        if len(vals) <= date_idx_src:
            continue
        d = excel_value_to_date(vals[date_idx_src])
        if d and month_start <= d.date() <= month_end:
            to_import.append(reorder_values_by_headers(src_headers, dst_headers, vals))
    print(f"[DEBUG] Linhas a importar (mês): {len(to_import)}")

    if not to_import:
        print("Nada para importar.")
    else:
        indices_to_delete = []
        for r in list_table_rows_paged(drive_id, dst_id, DST_TABLE, dst_sid, top=DEFAULT_TOP):
            idx = r.get("index")
            vals = (r.get("values", [[]])[0] or [])
            if idx is None or len(vals) <= date_idx_dst:
                continue
            d = excel_value_to_date(vals[date_idx_dst])
            if d and month_start <= d.date() <= month_end:
                indices_to_delete.append(int(idx))

        print(f"[DEBUG] Total índices a apagar: {len(indices_to_delete)}")
        print(f"[DEBUG] Amostra índices: {indices_to_delete[:50]}{' ...' if len(indices_to_delete)>50 else ''}")

        if indices_to_delete:
            res = delete_table_rows_by_index_batch(
                drive_id, dst_id, DST_TABLE, dst_sid, indices_to_delete,
                max_batch_size=20, max_retries=3, fallback_sequential=False
            )
            print(f"[OK] Removi {res['deleted']} linhas via $batch. Falharam {len(res['failed'])} no batch.")
            sweep_deleted = cleanup_month_rows_in_groups(
                drive_id, dst_id, DST_TABLE, dst_sid,
                date_idx_dst, month_start, month_end,
                group_size=DEFAULT_SWEEP_GROUP, top=DEFAULT_TOP
            )
            print(f"[OK] Sweep em grupos removeu {sweep_deleted} linhas remanescentes do mês.")
        else:
            print("[DEBUG] Nenhuma linha do mês encontrada para apagar no destino.")

        if IMPORT_USE_BATCH:
            inserted = add_rows_chunked_batch(
                drive_id, dst_id, DST_TABLE, dst_sid, to_import,
                chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES
            )
        else:
            inserted = add_rows_chunked_sequential(
                drive_id, dst_id, DST_TABLE, dst_sid, to_import,
                chunk_size=IMPORT_CHUNK_SIZE, max_retries=IMPORT_MAX_RETRIES
            )
        print(f"[OK] Inseridas {inserted} linhas do mês no destino (repartido, {'batch' if IMPORT_USE_BATCH else 'sequencial'}).")

finally:
    close_session(drive_id, src_id, src_sid)
    close_session(drive_id, dst_id, dst_sid)
