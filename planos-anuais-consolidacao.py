
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
SITE_PATH     = os.getenv("SITE_PATH", "").strip()

FOLDERS_ENV   = os.getenv("DRIVE_RELATIVE_FOLDERS", "")
DRIVE_FOLDERS = [p.strip() for p in FOLDERS_ENV.split(";") if p.strip()]

# Ficheiro consolidado (na mesma drive "Documentos Partilhados")
CONSOLIDATE_FILE_PATH  = os.getenv("CONSOLIDATE_FILE_PATH", "").strip()
CONSOLIDATE_SHEET_NAME = os.getenv("CONSOLIDATE_SHEET_NAME", "Consolidado").strip()

# Folhas e colunas esperadas
SOURCE_SHEET = "PowerBI"

COL_MARCAS    = "Marcas"
VAL_COLS      = ["4Q2025", "1Q2026", "2Q2026", "3Q2026", "FY 2026"]
PCT_COLS      = [f"{c}%" for c in VAL_COLS]
EXTRA_COLS    = ["Valor B3", "Pasta"]

ALL_COLS      = [COL_MARCAS] + VAL_COLS + PCT_COLS + EXTRA_COLS  # 13 colunas
COL_COUNT     = len(ALL_COLS)  # 13

# ========= AUTH (MSAL) =========
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
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
    print(f"[DEBUG] Consolidado: {CONSOLIDATE_FILE_PATH} | folha='{CONSOLIDATE_SHEET_NAME}'")

    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)
    print(f"[DEBUG] site_id={site_id}")
    print(f"[DEBUG] drive_id={drive_id}")

    # Resolver item do consolidado e criar sessão única
    cons_item_id = get_item_id_by_path(token, drive_id, CONSOLIDATE_FILE_PATH)
    cons_sess_id = create_session(token, drive_id, cons_item_id, persist=True)

    try:
        # Garantir folha de destino
        cons_ws_id = get_worksheet_id_by_name(token, drive_id, cons_item_id, cons_sess_id, CONSOLIDATE_SHEET_NAME)
        if not cons_ws_id:
            cons_ws_id = add_worksheet(token, drive_id, cons_item_id, cons_sess_id, CONSOLIDATE_SHEET_NAME)
            print(f"[DEBUG] Folha consolidada criada: id={cons_ws_id}")

        # Cabeçalho: se vazio ou diferente, escrever
        used = get_used_range(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id)
        cons_vals = used.get("values", [])
        existing_rows = len(cons_vals)
        print(f"[DEBUG] Consolidado usedRange: {existing_rows} linhas")

        header_out = [pad_row(ALL_COLS, COL_COUNT)]
        if existing_rows == 0:
            patch_range_values(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id, "A1:M1", header_out)
            next_row = 2
        else:
            # validar cabeçalho existente
            header_existing = [str(x).replace("\xa0"," ").strip() for x in (cons_vals[0] if cons_vals else [])]
            header_expected = [str(x) for x in ALL_COLS]
            print(f"[DEBUG] Header consolidado atual: {header_existing}")
            print(f"[DEBUG] Header esperado: {header_expected}")
            if header_existing != header_expected:
                print("[WARN] Cabeçalho diferente — a substituir pelo esperado.")
                patch_range_values(token, drive_id, cons_item_id, cons_sess_id, cons_ws_id, "A1:M1", header_out)
            # próxima linha após dados existentes
            data_rows = max(0, existing_rows - 1)
            next_row = 2 + data_rows
        print(f"[DEBUG] Próxima linha livre no consolidado: {next_row}")

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
                    src_vals = used_src.get("values", [])
                    if not src_vals or len(src_vals) <= 1:
                        print("     [INFO] Sem dados (apenas cabeçalho ou vazio).")
                        continue

                    # Validar cabeçalho da fonte
                    header_src = [str(x).replace("\xa0"," ").strip() for x in src_vals[0]]
                    expected   = [str(x) for x in ALL_COLS]
                    if header_src != expected:
                        print("     [WARN] Cabeçalho 'PowerBI' diferente do esperado — a tentar mesmo assim.")
                        # Podemos optar por normalizar ou falhar. Aqui seguimos.

                    # Filtrar linhas reais (da 2 em diante) e remover linhas totalmente vazias
                    data_rows = [row for row in src_vals[1:] if any(c not in (None, "",) for c in row)]
                    # Pad/truncate para 13 colunas
                    data_rows = [pad_row(r, COL_COUNT) for r in data_rows]
                    if not data_rows:
                        print("     [INFO] Sem linhas válidas após limpeza.")
                        continue

                    # Escrever em blocos para evitar limites
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
