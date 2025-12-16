
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

    rr.raise_for_status()  # força erro p/ ver detalhe

# ---- Outras helpers ----
def list_table_rows(drive_id, item_id, table_name, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][list_table_rows] STATUS:", r.status_code)
        try: print("[DEBUG][list_table_rows] JSON:", r.json())
        except Exception: print("[DEBUG][list_table_rows] TEXT:", r.text)
        r.raise_for_status()
    return r.json().get("value", [])

def add_rows(drive_id, item_id, table_name, session_id, values_2d):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    body = {"index": None, "values": values_2d}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"
    print(f"[DEBUG][ADD] {url} count={len(values_2d)}")
    r = requests.post(url, headers=h, data=json.dumps(body))
    if not r.ok:
        print("[DEBUG][ADD] STATUS:", r.status_code)
        try: print("[DEBUG][ADD] JSON:", r.json())
        except Exception: print("[DEBUG][ADD] TEXT:", r.text)
        r.raise_for_status()

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
    """Devolve (first_day, last_day) do mês de d (objetos date)."""
    first = datetime(d.year, d.month, 1).date()
    if d.month == 12:
        next_first = datetime(d.year + 1, 1, 1).date()
    else:
        next_first = datetime(d.year, d.month + 1, 1).date()
    last = next_first - timedelta(days=1)
    return first, last

def parse_range_address(address: str):
    """Ex.: 'LstPrd!A1:U535' → sheet, start_col, start_row, end_col, end_row"""
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

# ---- DELETE via $batch (ItemAt) + recolha de falhas + fallback sequencial
def delete_table_rows_by_index_batch(
    drive_id, item_id, table_name, session_id, row_indices,
    max_batch_size=5, max_retries=3, fallback_sequential=True
):
    """
    Apaga rows do corpo da Tabela por índice (0-based) usando JSON $batch (até 20/lote),
    com endereçamento via função: rows/$/ItemAt(index={n}).
    Recolhe as falhas e devolve {"deleted": X, "failed": [indices...]}.

    Se o $batch falhar com ApiNotFound/InvalidArgument, tenta fallback sequencial.
    """
    if not row_indices:
        print("[DEBUG][BATCH-DEL] Sem índices para apagar.")
        return {"deleted": 0, "failed": []}

    deleted_total = 0
    failed_global = []
    batch_endpoint = f"{GRAPH_BASE}/$batch"

    def chunks(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i+size]

    # Apagar do MAIOR para o MENOR para não deslocar os restantes
    row_indices = sorted(set(row_indices), reverse=True)
    print(f"[DEBUG][BATCH-DEL] Total de índices para apagar: {len(row_indices)}")

    # helper p/ DELETE unitário (fora de batch)
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
        # monta lote
        requests_list = []
        for i, idx in enumerate(chunk, start=1):
            rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
            requests_list.append({
                "id": str(i),
                "method": "DELETE",
                "url": rel_url,
                "headers": { "workbook-session-id": session_id }
            })

        print("[DEBUG][BATCH-DEL] URLs no lote:",
              [req["url"] for req in requests_list])

        payload = { "requests": requests_list }

        attempt = 0
        while True:
            attempt += 1
            print(f"[DEBUG][BATCH-DEL] POST {batch_endpoint} (lote {len(chunk)}, tentativa {attempt})")
            r = requests.post(batch_endpoint, headers=base_headers, data=json.dumps(payload))

            if r.status_code == 429 and attempt <= max_retries:
                wait = int(r.headers.get("Retry-After", "5"))
                print(f"[DEBUG][BATCH-DEL] 429 recebido. A aguardar {wait}s…")
                import time; time.sleep(wait)
                continue

            if not r.ok:
                print("[DEBUG][BATCH-DEL] STATUS:", r.status_code)
                try: print("[DEBUG][BATCH-DEL] JSON:", r.json())
                except Exception: print("[DEBUG][BATCH-DEL] TEXT:", r.text)
                # se o lote falhou “hard”, tenta sequencial em todos os índices do lote (se ativado)
                if fallback_sequential:
                    print("[DEBUG][BATCH-DEL] Falha no lote. A executar fallback sequencial para o lote.")
                    for idx in chunk:
                        if delete_single(idx):
                            deleted_total += 1
                        else:
                            failed_global.append(idx)
                break

            # analisar sub-respostas
            resp = r.json()
            ok_ids = [e for e in resp.get("responses", []) if e.get("status") in (200, 204)]
            deleted_total += len(ok_ids)

            for e in resp.get("responses", []):
                status = e.get("status")
                if status not in (200, 204):
                    body = e.get("body") or {}
                    print("[DEBUG][BATCH-DEL] Falhou id", e.get("id"),
                          "| status:", status, "| body:", body)
                    # mapeia id do lote -> o índice correspondente
                    try:
                        failed_idx = chunk[int(e.get("id")) - 1]
                        failed_global.append(failed_idx)
                    except Exception:
                        pass
            break  # fim do while deste lote

    print(f"[DEBUG][BATCH-DEL] Total rows apagadas (batch+seq parcial): {deleted_total}")
    return {"deleted": deleted_total, "failed": sorted(set(failed_global), reverse=True)}

# ---- Helpers para “sweep” final (recalcula índices e apaga 1 a 1)
def find_month_row_indices(drive_id, item_id, table_name, session_id, date_idx, month_start, month_end):
    """Volta a ler a tabela e devolve os índices (0-based) das rows do mês atual."""
    rows = list_table_rows(drive_id, item_id, table_name, session_id)
    indices = []
    for i, r in enumerate(rows):
        vals = (r.get("values", [[]])[0] or [])
        if len(vals) > date_idx:
            d = excel_value_to_date(vals[date_idx])
            if d and month_start <= d.date() <= month_end:
                indices.append(i)
    return indices

def cleanup_month_rows_sequential(drive_id, item_id, table_name, session_id, date_idx, month_start, month_end, max_iters=5000):
    """
    Sweep final: enquanto existirem linhas do mês atual, apaga 1 a 1 (DELETE unitário).
    Útil para remover pendentes que falharam no $batch por drift de índice.
    """
    deleted = 0
    iters = 0
    while iters < max_iters:
        iters += 1
        indices = find_month_row_indices(drive_id, item_id, table_name, session_id, date_idx, month_start, month_end)
        if not indices:
            break
        # apaga a de MAIOR índice (mais seguro)
        idx = max(indices)
        rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
        abs_url = f"{GRAPH_BASE}{rel_url}"
        h = dict(base_headers); h["workbook-session-id"] = session_id
        print(f"[DEBUG][SWEEP] DELETE {abs_url} (restantes: {len(indices)})")
        r = requests.delete(abs_url, headers=h)
        if not r.ok:
            print("[DEBUG][SWEEP] STATUS:", r.status_code)
            try: print("[DEBUG][SWEEP] JSON:", r.json())
            except Exception: print("[DEBUG][SWEEP] TEXT:", r.text)
            # se falhar, tentamos a próxima iteração
            continue
        deleted += 1
    print(f"[DEBUG][SWEEP] Removidas no sweep: {deleted}")
    return deleted

# ---- Fluxo principal ----
site_id  = get_site_id()
drive_id = get_drive_id(site_id)
src_id   = get_item_id(drive_id, SRC_FILE_PATH)
dst_id   = get_item_id(drive_id, DST_FILE_PATH)

src_sid  = create_session(drive_id, src_id)
dst_sid  = create_session(drive_id, dst_id)

try:
    # Listar tabelas p/ debug
    _ = list_tables(drive_id, src_id, src_sid)
    _ = list_tables(drive_id, dst_id, dst_sid)

    # Obter cabeçalhos com fallback
    src_headers = get_table_headers_safe(drive_id, src_id, SRC_TABLE, src_sid)
    dst_headers = get_table_headers_safe(drive_id, dst_id, DST_TABLE, dst_sid)
    print("[DEBUG] src_headers:", src_headers)
    print("[DEBUG] dst_headers:", dst_headers)

    if DATE_COLUMN not in src_headers or DATE_COLUMN not in dst_headers:
        raise Exception(f"A coluna '{DATE_COLUMN}' não existe em uma das tabelas.")

    date_idx_src = src_headers.index(DATE_COLUMN)
    date_idx_dst = dst_headers.index(DATE_COLUMN)

    today = datetime.today()
    month_start, month_end = month_bounds(today)
    print(f"[DEBUG] Mês atual: {month_start} a {month_end}")

    # --- Origens: filtrar mês atual e reordenar p/ o destino ---
    src_rows = list_table_rows(drive_id, src_id, SRC_TABLE, src_sid)
    src_values = [r.get("values", [[]])[0] for r in src_rows]
    to_import = []
    for vals in src_values:
        d = excel_value_to_date(vals[date_idx_src])
        if d and month_start <= d.date() <= month_end:
            to_import.append(reorder_values_by_headers(src_headers, dst_headers, vals))
    print(f"[DEBUG] Linhas a importar (mês): {len(to_import)}")

    if not to_import:
        print("Nada para importar.")
    else:
        # --- Destino: índices a remover (mês atual) ---
        dst_rows = list_table_rows(drive_id, dst_id, DST_TABLE, dst_sid)
        indices_to_delete = []
        for i, r in enumerate(dst_rows):  # i = índice 0-based no corpo da tabela
            vals = (r.get("values", [[]])[0] or [])
            if len(vals) > date_idx_dst:
                d = excel_value_to_date(vals[date_idx_dst])
                if d and month_start <= d.date() <= month_end:
                    indices_to_delete.append(i)

        print(f"[DEBUG] Índices a apagar no destino (mês): {indices_to_delete[:50]}{' ...' if len(indices_to_delete)>50 else ''}")
        print(f"[DEBUG] Total índices a apagar: {len(indices_to_delete)}")

        # --- Apagar via $batch + seq parcial (recolhe falhas) ---
        if indices_to_delete:
            res = delete_table_rows_by_index_batch(
                drive_id, dst_id, DST_TABLE, dst_sid, indices_to_delete,
                max_batch_size=20, max_retries=3, fallback_sequential=True
            )
            print(f"[OK] Removi {res['deleted']} linhas via $batch/seq parcial. Falharam {len(res['failed'])} no batch.")

            # --- Sweep final: recalcula índices e apaga remanescentes do mês ---
            sweep_deleted = cleanup_month_rows_sequential(
                drive_id, dst_id, DST_TABLE, dst_sid, date_idx_dst, month_start, month_end
            )
            print(f"[OK] Sweep final removeu {sweep_deleted} linhas do mês (pendentes).")
        else:
            print("[DEBUG] Nenhuma linha do mês encontrada para apagar no destino.")

        # --- Inserir novas linhas do mês atual ---
        add_rows(drive_id, dst_id, DST_TABLE, dst_sid, to_import)
        print(f"[OK] Inseridas {len(to_import)} linhas do mês atual no destino.")

finally:
    close_session(drive_id, src_id, src_sid)

