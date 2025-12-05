
import os
import json
import urllib.parse
import requests
import msal

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG por variáveis de ambiente =========
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SITE_HOSTNAME = os.getenv("SITE_HOSTNAME", "").strip()
SITE_PATH     = os.getenv("SITE_PATH_W", "").strip()

FOLDERS_ENV   = os.getenv("DRIVE_RELATIVE_FOLDERS", "")
DRIVE_FOLDERS = [p.strip() for p in FOLDERS_ENV.split(";") if p.strip()]

# Ficheiro consolidado (na mesma drive "Documentos Partilhados")
CONSOLIDATE_FILE_PATH  = os.getenv("CONSOLIDATE_FILE_PATH", "").strip()

# Nome da folha de consolidação (env) — default para "Planos" como pediste
# (tolerância opcional a typo CONSOLIDADE_SHEET_NAME)
_sheet_name_env = os.getenv("CONSOLIDATE_SHEET_NAME") or os.getenv("CONSOLIDADE_SHEET_NAME")
CONSOLIDATE_SHEET_NAME = (_sheet_name_env or "Planos").strip()

# Folhas e colunas esperadas
SOURCE_SHEET = "PowerBI Nao Mexer"

COL_MARCAS    = "Marcas"
VAL_COLS      = ["4Q2025", "1Q2026", "2Q2026", "3Q2026", "FY 2026"]
PCT_COLS      = [f"{c}%" for c in VAL_COLS]
EXTRA_COLS    = ["Farmácias", "GSI"]

ALL_COLS      = [COL_MARCAS] + VAL_COLS + PCT_COLS + EXTRA_COLS  # 13 colunas
COL_COUNT     = len(ALL_COLS)  # 13

# ========= AUTH (MSAL) =========
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
if "access_token" not in token_result:
    raise RuntimeError(f"Falha a obter token: {token_result.get('error')} - {token_result.get('error_description')}")
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ========= HELPERS Graph =========
def get_site_id():
    return requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=base_headers).json()["id"]

def get_drive_id(site_id):
    return requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=base_headers).json()["id"]

def get_item_id_by_path(token: str, drive_id: str, drive_relative_path: str) -> str:
    """ devolve item_id dado caminho relativo (SEM %20) """
    h = {"Authorization": f"Bearer {token}"}
    enc = urllib.parse.quote(drive_relative_path.strip("/"))
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{enc}"
    r = requests.get(url, headers=h); r.raise_for_status()
    return r.json()["id"]

def list_children_recursive(token: str, drive_id: str, drive_relative_folder: str) -> list[dict]:
    """ devolve todos os ficheiros (.xlsx/.xlsm) dentro da pasta (e subpastas) """
    h = {"Authorization": f"Bearer {token}"}
    enc = urllib.parse.quote(drive_relative_folder.strip("/"))
    url_item = f"{GRAPH_BASE}/drives/{drive_id}/root:/{enc}"
    r = requests.get(url_item, headers=h); r.raise_for_status()
    folder_id = r.json()["id"]

    files = []

    def list_children(item_id: str):
        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"
        next_url = url
        while next_url:
            resp = requests.get(next_url, headers=h); resp.raise_for_status()
            data = resp.json()
            for it in data.get("value", []):
                name = it.get("name", "")
                if "file" in it:
                    if name.lower().endswith((".xlsx", ".xlsm")) and not name.startswith("~$"):
                        files.append(it)
                elif "folder" in it:
                    list_children(it["id"])
            next_url = data.get("@odata.nextLink")
    list_children(folder_id)
    return files

# ========= Workbook APIs =========
def create_session(token: str, drive_id: str, item_id: str, persist=True) -> str:
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession"
    body = {"persistChanges": bool(persist)}
    r = requests.post(url, headers=h, data=json.dumps(body)); r.raise_for_status()
    sid = r.json()["id"]
    print(f"[DEBUG] Session criada: {sid}")
    return sid

def close_session(token: str, drive_id: str, item_id: str, session_id: str):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    r = requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)
    print(f"[DEBUG] Session fechada (status {r.status_code})")

def get_worksheets(token: str, drive_id: str, item_id: str, session_id: str) -> list[dict]:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets"
    r = requests.get(url, headers=h); r.raise_for_status()
    return r.json().get("value", [])

def get_worksheet_id_by_name(token: str, drive_id: str, item_id: str, session_id: str, sheet_name: str) -> str | None:
    for s in get_worksheets(token, drive_id, item_id, session_id):
        if s.get("name") == sheet_name:
            return s.get("id")
    return None

def add_worksheet(token: str, drive_id: str, item_id: str, session_id: str, sheet_name: str) -> str:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id, "Content-Type":"application/json"}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/add"
    r = requests.post(url, headers=h, data=json.dumps({"name": sheet_name})); r.raise_for_status()
    return r.json()["id"]

def delete_worksheet(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}"
    r = requests.delete(url, headers=h)  # 204 esperado; ignoramos falhas leves
    print(f"[DEBUG] DELETE worksheet id={worksheet_id} (status {r.status_code})")

def get_range_values(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str, address: str) -> list[list]:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/range(address='{address}')"
    print(f"[DEBUG] GET range {address} …")
    r = requests.get(url, headers=h)
    if not r.ok:
        raise RuntimeError(f"GET range {address} falhou: {r.status_code} {r.text}")
    vals = r.json().get("values", [])
    print(f"[DEBUG] Range {address}: {len(vals)} linhas")
    return vals

def get_used_range(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str) -> dict:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/usedRange(valuesOnly=true)"
    r = requests.get(url, headers=h)
    if not r.ok:
        raise RuntimeError(f"GET usedRange falhou: {r.status_code} {r.text}")
    return r.json()  # contém 'address' e 'values'

def patch_range_values(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str, address: str, values_2d: list[list]):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id, "Content-Type":"application/json"}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/range(address='{address}')"
    rows = len(values_2d)
    cols = len(values_2d[0]) if rows > 0 else 0
    print(f"[DEBUG] PATCH range {address} com {rows}x{cols} …")
    body = {"values": values_2d}
    r = requests.patch(url, headers=h, data=json.dumps(body))
    if not r.ok:
        raise RuntimeError(f"PATCH {address} falhou: {r.status_code} {r.text}")

def pad_row(r, width=COL_COUNT):
    rr = list(r)
    if len(rr) < width:
        rr.extend([None] * (width - len(rr)))
    elif len(rr) > width:
        rr = rr[:width]
    return rr

def chunk_rows(rows, size=4000):
    for i in range(0, len(rows), size):
        yield rows[i:i+size]

# ========= MAIN =========
def main():
    # Validação mínima
    if not DRIVE_FOLDERS:
        raise RuntimeError("DRIVE_RELATIVE_FOLDERS vazio.")
    if not CONSOLIDATE_FILE_PATH:
        raise RuntimeError("CONSOLIDATE_FILE_PATH vazio. Define o caminho relativo do ficheiro consolidado.")

    print(f"[DEBUG] SITE_HOSTNAME={SITE_HOSTNAME}")
    print(f"[DEBUG] SITE_PATH={SITE_PATH}")
    print(f"[DEBUG] Pastas: {len(DRIVE_FOLDERS)} → {DRIVE_FOLDERS}")
    print(f"[DEBUG] CONSOLIDATE_FILE_PATH={CONSOLIDATE_FILE_PATH!r}")
    print(f"[DEBUG] CONSOLIDATE_SHEET_NAME efetivo: {CONSOLIDATE_SHEET_NAME!r}")

    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)
    print(f"[DEBUG] site_id={site_id}")
    print(f"[DEBUG] drive_id={drive_id}")

    # Resolver item do consolidado e criar sessão única
    cons_item_id = get_item_id_by_path(token, drive_id, CONSOLIDATE_FILE_PATH)
    cons_sess_id = create_session(token, drive_id, cons_item_id, persist=True)

    try:
        # >>> Overwrite total: Delete + Add da folha
        cons_ws_id = get_worksheet_id_by_name(token, drive_id, cons_item_id, cons_sess_id, CONSOLIDATE_SHEET_NAME)
        if cons_ws_id:
            print(f"[DEBUG] Overwrite: a eliminar folha '{CONSOLIDATE_SHEET_NAME}' (id={cons_ws_id})")
            delete_worksheet(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id)

        cons_ws_id = add_worksheet(token, drive_id, cons_item_id, cons_sess_id, CONSOLIDATE_SHEET_NAME)
        print(f"[DEBUG] Folha recriada: '{CONSOLIDATE_SHEET_NAME}' (id={cons_ws_id})")

        # Escrever cabeçalho (A1:M1) e preparar próxima linha
        header_out = [pad_row(ALL_COLS, COL_COUNT)]
        patch_range_values(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id, "A1:M1", header_out)
        next_row = 2
        # <<< Fim overwrite total

        total_appended = 0

        # Percorrer todas as pastas e ficheiros fonte
        for folder in DRIVE_FOLDERS:
            print(f"\n[Pasta] {folder}")
            try:
                items = list_children_recursive(token, drive_id, folder)
                print(f"[DEBUG] {len(items)} ficheiros encontrados.")
            except Exception as e:
                print(f"  [ERRO] A aceder à pasta: {e}")
                continue

            for it in items:
                name    = it.get("name", "")
                item_id = it.get("id")
                print(f"  [Ler] {name}")

                src_sess_id = create_session(token, drive_id, item_id, persist=False)
                try:
                    # Verificar folha 'PowerBI'
                    src_ws_id = get_worksheet_id_by_name(token, drive_id, item_id, src_sess_id, SOURCE_SHEET)
                    if not src_ws_id:
                        print("     [INFO] Folha 'PowerBI' não existe — a ignorar.")
                        continue

                    # Ler usedRange da fonte
                    used_src = get_used_range(token, drive_id, item_id, src_sess_id, src_ws_id)
                    print(f"     [DEBUG] usedRange address: {used_src.get('address')!r}")
                    src_vals = used_src.get("values", [])
                    if not src_vals or len(src_vals) <= 1:
                        print("     [INFO] Sem dados (apenas cabeçalho ou vazio).")
                        continue

                    # Validar cabeçalho da fonte (com debug)
                    def norm_cell(x):
                        return str(x).replace("\xa0", " ").strip()

                    header_src = [norm_cell(x) for x in src_vals[0]]
                    expected   = [norm_cell(x) for x in ALL_COLS]

                    set_src = set(header_src)
                    set_exp = set(expected)
                    missing = [c for c in expected if c not in set_src]
                    extra   = [c for c in header_src if c not in set_exp]
                    order_diff = (header_src != expected)

                    if missing or extra or order_diff:
                        print("     [WARN] Cabeçalho 'PowerBI' divergente.")
                        if missing:
                            print(f"     [WARN]  - Faltam: {missing}")
                        if extra:
                            print(f"     [WARN]  - Extras: {extra}")
                        if order_diff and not (missing or extra):
                            print(f"     [INFO]  - Ordem diferente, mas mesmas colunas.")

                    # Se faltarem colunas obrigatórias, ignorar o ficheiro
                    if missing:
                        print("     [ERRO] Há colunas obrigatórias em falta — a ignorar este ficheiro.")
                        continue

                    # Remapear dados para a ordem esperada (robusto contra ordem diferente)
                    idx_map = {col: header_src.index(col) for col in expected}
                    data_rows_src = [row for row in src_vals[1:] if any(c not in (None, "",) for c in row)]
                    remapped_rows = []
                    for r in data_rows_src:
                        new_r = [ r[idx_map[col]] if idx_map[col] < len(r) else None for col in expected ]
                        remapped_rows.append(new_r)

                    # Pad/trunc para 13 colunas
                    data_rows = [pad_row(r, COL_COUNT) for r in remapped_rows]
                    if not data_rows:
                        print("     [INFO] Sem linhas válidas após limpeza.")
                        continue

                    # Escrever em blocos
                    for chunk in chunk_rows(data_rows, size=4000):
                        end_row = next_row + len(chunk) - 1
                        addr_out = f"A{next_row}:M{end_row}"
                        print(f"     [DEBUG] Append {len(chunk)} linhas → {addr_out}")
                        patch_range_values(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id, addr_out, chunk)
                        next_row = end_row + 1
                        total_appended += len(chunk)

                    print(f"     [OK] {len(data_rows)} linhas anexadas do ficheiro.")

                except Exception as e:
                    print(f"     [ERRO] {e}")
                finally:
                    close_session(token, drive_id, item_id, src_sess_id)

        print(f"\n[Resumo] Linhas totais anexadas: {total_appended}")

    finally:
        close_session(token, drive_id, cons_item_id, cons_sess_id)

if __name__ == "__main__":
    main()
