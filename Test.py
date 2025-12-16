
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
    url = f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}"
    r = requests.get(url, headers=base_headers)
    if not r.ok:
        print("[DEBUG][get_site_id] STATUS:", r.status_code)
        print("[DEBUG][get_site_id] TEXT:", r.text)
        r.raise_for_status()
    return r.json()["id"]

def get_drive_id(site_id):
    url = f"{GRAPH_BASE}/sites/{site_id}/drive"
    r = requests.get(url, headers=base_headers)
    if not r.ok:
        print("[DEBUG][get_drive_id] STATUS:", r.status_code)
        print("[DEBUG][get_drive_id] TEXT:", r.text)
        r.raise_for_status()
    return r.json()["id"]

def get_item_id(drive_id, path):
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{path}"
    r = requests.get(url, headers=base_headers)
    if not r.ok:
        print("[DEBUG][get_item_id] STATUS:", r.status_code)
        print("[DEBUG][get_item_id] TEXT:", r.text)
        r.raise_for_status()
    return r.json()["id"]

def create_session(drive_id, item_id):
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession"
    r = requests.post(url, headers=base_headers, data=json.dumps({"persistChanges": True}))
    if not r.ok:
        print("[DEBUG][create_session] STATUS:", r.status_code)
        print("[DEBUG][create_session] TEXT:", r.text)
        r.raise_for_status()
    sid = r.json()["id"]
    print("[DEBUG] Sessão criada:", sid)
    return sid

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession"
    r = requests.post(url, headers=h)
    print("[DEBUG] Sessão fechada:", session_id, "| status:", r.status_code)

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
    # 1) tentar "oficial"
    try:
        headers = get_table_headers(drive_id, item_id, table_name, session_id)
        if headers:
            print(f"[DEBUG] headerRowRange → {headers}")
            return headers
    except requests.HTTPError:
        print("[DEBUG] headerRowRange falhou; a tentar fallback por /columns...")

    # 2) columns
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

    # 3) range
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

    rr.raise_for_status()  # força erro para ver detalhe

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
    """Ex.: 'Historico!A1:AD100' → sheet, start_col, start_row, end_col, end_row"""
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

# ---- DELETE por blocos contíguos (address relativo à worksheet)
def delete_row_blocks(drive_id, item_id, session_id, worksheet_id, sheet_name,
                      start_col, end_col, header_row, indices_0based):
    """
    Mapeia índices (0-based) do corpo da tabela p/ números de linha da folha
    e apaga blocos contíguos com range/delete (shift Up).
    """
    ws_rows = sorted({ header_row + 1 + i for i in indices_0based })
    if not ws_rows:
        print("[DEBUG] Nenhuma linha a apagar.")
        return 0

    # Agrupar linhas contíguas
    blocks = []
    s = prev = ws_rows[0]
    for r in ws_rows[1:]:
        if r == prev + 1:
            prev = r
        else:
            blocks.append((s, prev))
            s = prev = r
    blocks.append((s, prev))

    h = dict(base_headers); h["workbook-session-id"] = session_id
    total_deleted = 0
    for ini, fim in blocks:
        # address relativo à worksheet (NÃO incluir 'Sheet'!)
        addr = f"{start_col}{ini}:{end_col}{fim}"   # ex.: "A2:U10"
        url = (f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
               f"/workbook/worksheets/{worksheet_id}/range(address='{addr}')/delete")
        body = {"shift": "Up"}
        print(f"[DEBUG][DELETE] {url} body={body}")
        r = requests.post(url, headers=h, data=json.dumps(body))
        if not r.ok:
            print("[DEBUG][DELETE] STATUS:", r.status_code)
            try: print("[DEBUG][DELETE] JSON:", r.json())
            except Exception: print("[DEBUG][DELETE] TEXT:", r.text)
            r.raise_for_status()
        total_deleted += (fim - ini + 1)

    print(f"[DEBUG] Total de linhas apagadas: {total_deleted}")
    return total_deleted

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
        print(f"[DEBUG] Índices a apagar no destino (mês): {indices_to_delete}")

        # --- Apagar fisicamente as rows do mês atual (range/delete) ---
        if indices_to_delete:
            table_addr = get_table_range(drive_id, dst_id, DST_TABLE, dst_sid)  # ex.: "Historico!A1:AD100"
            meta = parse_range_address(table_addr)
            sheet_name   = meta["sheet"]
            header_row   = meta["start_row"]
            start_col    = meta["start_col"]
            end_col      = meta["end_col"]
            worksheet_id = get_worksheet_id(drive_id, dst_id, dst_sid, sheet_name)
            print(f"[DEBUG] worksheet_id={worksheet_id} sheet_name='{sheet_name}' table_range={table_addr}")

            deleted = delete_row_blocks(
                drive_id, dst_id, dst_sid, worksheet_id,
                sheet_name, start_col, end_col, header_row,
                indices_to_delete
            )
            print(f"[OK] Removi {deleted} linhas do mês atual no destino (eliminação física).")
        else:
            print("[DEBUG] Nenhuma linha do mês encontrada para apagar no destino.")

        # --- Inserir novas linhas do mês atual ---
        add_rows(drive_id, dst_id, DST_TABLE, dst_sid, to_import)
        print(f"[OK] Inseridas {len(to_import)} linhas do mês atual no destino.")

finally:
    close_session(drive_id, src_id, src_sid)
    close_session(drive_id, dst_id, dst_sid)
