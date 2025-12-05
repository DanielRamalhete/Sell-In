
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

# Ex.: SITE_HOSTNAME="braveperspective.sharepoint.com"
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME", "").strip()

# Ex.: SITE_PATH="equipa.comite"  (ATENÇÃO: sem 'sites/' e sem URL completo)
SITE_PATH     = os.getenv("SITE_PATH", "").strip()

# Pastas relativas ao drive "Documentos Partilhados" (SEM %20), separadas por ';'
# Ex.: "General/Teste - Daniel PowerAutomate/5. Planos Anuais/FMENEZES;General/.../GMALAFAYA;..."
FOLDERS_ENV   = os.getenv("DRIVE_RELATIVE_FOLDERS", "")
DRIVE_FOLDERS = [p.strip() for p in FOLDERS_ENV.split(";") if p.strip()]

# Lê até N linhas do corpo (B6:G...) — podes ajustar via env
MAX_ROWS_READ = int(os.getenv("MAX_ROWS_READ", "2000"))

# Folhas e colunas
SHEET_SOURCE  = "Resumo Plano anual"
SHEET_TARGET  = "PowerBI"

COL_MARCAS    = "Marcas"
VAL_COLS      = ["4Q2025", "1Q2026", "2Q2026", "3Q2026", "FY 2026"]
PCT_COLS      = [f"{c}%" for c in VAL_COLS]

# ========= AUTH (MSAL) =========
# ---- Autenticação ----
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

def list_children_recursive(token: str, drive_id: str, drive_relative_folder: str) -> list[dict]:
    """
    Devolve todos os ficheiros (.xlsx/.xlsm) dentro da pasta (e subpastas).
    drive_relative_folder: ex. "General/Teste - Daniel PowerAutomate/5. Planos Anuais/FMENEZES"
    """
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
    return r.json()["id"]

def close_session(token: str, drive_id: str, item_id: str, session_id: str):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

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
    requests.delete(url, headers=h)  # 204 esperado; ignoramos falhas leves

def get_range_values(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str, address: str) -> list[list]:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/range(address='{address}')"
    r = requests.get(url, headers=h); r.raise_for_status()
    return r.json().get("values", [])

def patch_range_values(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str, address: str, values_2d: list[list]):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id, "Content-Type":"application/json"}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/range(address='{address}')"
    body = {"values": values_2d}
    r = requests.patch(url, headers=h, data=json.dumps(body)); r.raise_for_status()

# ========= Transformação =========
def normalize_percent(v):
    if v is None: return None
    if isinstance(v, str):
        s = v.strip()
        if s.endswith("%"):
            return s.replace(" %", "%").replace("% ", "%")
        try:
            num = float(s.replace(",", "."))
            return f"{int(round(num*100))}%" if 0 <= num <= 1 else f"{int(round(num))}%"
        except:
            return s
    try:
        num = float(v)
        return f"{int(round(num*100))}%" if 0 <= num <= 1 else f"{int(round(num))}%"
    except:
        return str(v)

def build_output_from_values(values_rows: list[list]) -> list[list]:
    """
    values_rows: linhas [Marcas, 4Q2025, 1Q2026, 2Q2026, 3Q2026, FY 2026]
    Estrutura: pares consecutivos
      - linha i = % (Marcas blank)
      - linha i+1 = valores (Marcas preenchida)
    Output: [Marcas, 4Q2025, 1Q2026, 2Q2026, 3Q2026, FY 2026, 4Q2025%, 1Q2026%, 2Q2026%, 3Q2026%, FY 2026%]
    """
    out = []
    i = 0
    n = len(values_rows)
    while i < n - 1:
        row_pct = values_rows[i] or []
        row_val = values_rows[i+1] or []
        marcas_pct = (row_pct[0] if len(row_pct) > 0 else None)
        marcas_val = (row_val[0] if len(row_val) > 0 else None)

        if (marcas_pct is None or str(marcas_pct).strip() == "") and (marcas_val is not None and str(marcas_val).strip() != ""):
            rec = [str(marcas_val).strip()]
            # Valores (da linha i+1)
            for k in range(1, 6):
                rec.append(row_val[k] if len(row_val) > k else None)
            # Percentagens (da linha i) normalizadas
            for k in range(1, 6):
                rec.append(normalize_percent(row_pct[k] if len(row_pct) > k else None))
            out.append(rec)
            i += 2
        else:
            i += 1
    return out

# ========= MAIN =========
def main():
    # Validação mínima de config
    if not DRIVE_FOLDERS:
        raise RuntimeError("DRIVE_RELATIVE_FOLDERS vazio. Define as pastas no segredo/variável.")

    print(f"[DEBUG] SITE_HOSTNAME={SITE_HOSTNAME}")
    print(f"[DEBUG] SITE_PATH={SITE_PATH}")
    print(f"[DEBUG] MAX_ROWS_READ={MAX_ROWS_READ}")

    token = token_result["access_token"]
    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)

    total_files = 0
    ok_files    = 0
    errors      = []

    for folder in DRIVE_FOLDERS:
        print(f"\n[Pasta] {folder}")
        try:
            items = list_children_recursive(token, drive_id, folder)
        except Exception as e:
            print(f"  [ERRO] A aceder à pasta: {e}")
            continue

        for it in items:
            name    = it.get("name", "")
            item_id = it.get("id")
            total_files += 1
            print(f"  [Processar] {name}")

            sess_id = create_session(token, drive_id, item_id, persist=True)
            try:
                # 1) Worksheet origem
                ws_src_id = get_worksheet_id_by_name(token, drive_id, item_id, sess_id, SHEET_SOURCE)
                if not ws_src_id:
                    raise RuntimeError(f"Folha '{SHEET_SOURCE}' não encontrada.")

                # 2) Ler cabeçalho B5:G5
                header_vals = get_range_values(token, drive_id, item_id, sess_id, ws_src_id, "B5:G5")
                header = [str(x) for x in (header_vals[0] if header_vals else [])]
                expected = [COL_MARCAS] + VAL_COLS
                if header != expected:
                    raise RuntimeError(f"Header inesperado.\nEsperado: {expected}\nEncontrado: {header}")

                # 3) Ler corpo B6:G{fim}
                end_row = 6 + MAX_ROWS_READ - 1
                body_addr = f"B6:G{end_row}"
                body_vals = get_range_values(token, drive_id, item_id, sess_id, ws_src_id, body_addr)

                # Limpar cauda vazia
                clean_rows = [row for row in body_vals if any(c not in (None, "",) for c in row)]
                out_rows = build_output_from_values(clean_rows)

                # 4) Preparar destino: recriar folha para evitar resíduos
                ws_dst_id = get_worksheet_id_by_name(token, drive_id, item_id, sess_id, SHEET_TARGET)
                if ws_dst_id:
                    delete_worksheet(token, drive_id, item_id, sess_id, ws_dst_id)
                ws_dst_id = add_worksheet(token, drive_id, item_id, sess_id, SHEET_TARGET)

                # 5) Escrever cabeçalho + dados (A1:K...)
                header_out = [COL_MARCAS] + VAL_COLS + PCT_COLS
                patch_range_values(token, drive_id, item_id, sess_id, ws_dst_id, "A1:K1", [header_out])

                if out_rows:
                    end_out = 1 + len(out_rows) + 1  # A partir da linha 2
                    addr_out = f"A2:K{end_out}"
                    patch_range_values(token, drive_id, item_id, sess_id, ws_dst_id, addr_out, out_rows)

                print(f"     [OK] {len(out_rows)} marcas → folha '{SHEET_TARGET}' escrita.")
                ok_files += 1

            except Exception as e:
                print(f"     [ERRO] {e}")
                errors.append((name, str(e)))
            finally:
                close_session(token, drive_id, item_id, sess_id)

    print("\nResumo:")
    print(f"  Ficheiros encontrados: {total_files}")
    print(f"  Processados com sucesso: {ok_files}")
    if errors:
        print("  Erros:")
        for fname, err in errors:
            print(f"    - {fname}: {err}")

if __name__ == "__main__":
    main()
