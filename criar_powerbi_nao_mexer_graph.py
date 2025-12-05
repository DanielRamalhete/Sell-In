
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
# Agora aceitamos duas alternativas para a folha de origem:
SHEET_SOURCE_ALTS = ["Resumo Plano anual", "Folha1"]
SHEET_TARGET  = "PowerBI"

COL_MARCAS    = "Marcas"
VAL_COLS      = ["4Q2025", "1Q2026", "2Q2026", "3Q2026", "FY 2026"]
PCT_COLS      = [f"{c}%" for c in VAL_COLS]

# Novas colunas
EXTRA_COLS    = ["Farmácias", "GSI"]  # nomes das duas novas colunas

# ========= AUTH (MSAL) =========
# ---- Autenticação (mantida como tinhas) ----
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ========= HELPERS Graph =========
# (mantidas como tinhas)
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
    v = r.json().get("value", [])
    print(f"[DEBUG] Worksheets: {len(v)}")
    return v

def get_worksheet_id_by_name(token: str, drive_id: str, item_id: str, session_id: str, sheet_name: str) -> str | None:
    for s in get_worksheets(token, drive_id, item_id, session_id):
        if s.get("name") == sheet_name:
            return s.get("id")
    return None

def add_worksheet(token: str, drive_id: str, item_id: str, session_id: str, sheet_name: str) -> str:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id, "Content-Type":"application/json"}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/add"
    r = requests.post(url, headers=h, data=json.dumps({"name": sheet_name})); r.raise_for_status()
    wsid = r.json()["id"]
    print(f"[DEBUG] Worksheet adicionada: {sheet_name} (id={wsid})")
    return wsid

def delete_worksheet(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str):
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}"
    r = requests.delete(url, headers=h)  # 204 esperado; ignoramos falhas leves
    print(f"[DEBUG] DELETE worksheet id={worksheet_id} (status {r.status_code})")

def get_range_values(token: str, drive_id: str, item_id: str, session_id: str, worksheet_id: str, address: str) -> list[list]:
    h = {"Authorization": f"Bearer {token}", "workbook-session-id": session_id}
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets/{worksheet_id}/range(address='{address}')"
    print(f"[DEBUG] GET range {address} …")
    r = requests.get(url, headers=h); 
    if not r.ok:
        raise RuntimeError(f"GET range {address} falhou: {r.status_code} {r.text}")
    vals = r.json().get("values", [])
    print(f"[DEBUG] Range {address}: {len(vals)} linhas")
    return vals

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

# ========= Transformação =========
def normalize_percent(v):
    if v is None: 
        return None
    if isinstance(v, str):
        s = v.replace("\xa0", " ").strip()
        if s in {"-", "–", "—"}:
            return None
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
    Estrutura: pares consecutivos — aceita [% , valores] OU [valores , %]
    Output base (antes das novas colunas): 
      [Marcas, 4Q2025, 1Q2026, 2Q2026, 3Q2026, FY 2026, 4Q2025%, 1Q2026%, 2Q2026%, 3Q2026%, FY 2026%]
    """
    out = []
    i = 0
    n = len(values_rows)

    def brand(cell):
        return None if cell is None else str(cell).replace("\xa0"," ").strip()

    while i < n - 1:
        r1 = values_rows[i] or []
        r2 = values_rows[i+1] or []
        b1 = brand(r1[0] if len(r1) > 0 else None)
        b2 = brand(r2[0] if len(r2) > 0 else None)

        # Caso 1: [% , valores]
        if (not b1) and b2:
            row_pct, row_val, brand_name = r1, r2, b2
        # Caso 2: [valores , %]
        elif b1 and (not b2):
            row_pct, row_val, brand_name = r2, r1, b1
        else:
            i += 1
            continue

        rec = [brand_name]
        # valores (colunas 1..5)
        for k in range(1, 6):
            rec.append(row_val[k] if len(row_val) > k else None)
        # percentagens normalizadas (colunas 1..5)
        for k in range(1, 6):
            rec.append(normalize_percent(row_pct[k] if len(row_pct) > k else None))
        out.append(rec)
        i += 2

    return out

def pad_row(r, width=13):
    rr = list(r)
    if len(rr) < width:
        rr.extend([None] * (width - len(rr)))
    elif len(rr) > width:
        rr = rr[:width]
    return rr

# ========= MAIN =========
def main():
    # Validação mínima de config
    if not DRIVE_FOLDERS:
        raise RuntimeError("DRIVE_RELATIVE_FOLDERS vazio. Define as pastas no segredo/variável.")

    print(f"[DEBUG] SITE_HOSTNAME={SITE_HOSTNAME}")
    print(f"[DEBUG] SITE_PATH={SITE_PATH}")
    print(f"[DEBUG] MAX_ROWS_READ={MAX_ROWS_READ}")
    print(f"[DEBUG] Pastas: {len(DRIVE_FOLDERS)} → {DRIVE_FOLDERS}")

    token = token_result["access_token"]
    site_id  = get_site_id()
    drive_id = get_drive_id(site_id)

    print(f"[DEBUG] site_id={site_id}")
    print(f"[DEBUG] drive_id={drive_id}")

    total_files = 0
    ok_files    = 0
    errors      = []

    for folder in DRIVE_FOLDERS:
        print(f"\n[Pasta] {folder}")
        try:
            items = list_children_recursive(token, drive_id, folder)
            print(f"[DEBUG] {len(items)} ficheiros Excel encontrados na pasta.")
        except Exception as e:
            print(f"  [ERRO] A aceder à pasta: {e}")
            continue

        # Nome simples da pasta (último segmento)
        folder_name_simple = folder.rsplit("/", 1)[-1] if "/" in folder else folder

        for it in items:
            name    = it.get("name", "")
            item_id = it.get("id")
            total_files += 1
            print(f"  [Processar] {name}")

            sess_id = create_session(token, drive_id, item_id, persist=True)
            try:
                # 1) Worksheet origem: tenta alternativas
                ws_src_id = None
                sheet_used = None
                for candidate in SHEET_SOURCE_ALTS:
                    ws_src_id = get_worksheet_id_by_name(token, drive_id, item_id, sess_id, candidate)
                    if ws_src_id:
                        sheet_used = candidate
                        break
                if not ws_src_id:
                    raise RuntimeError(f"Folha de origem não encontrada (tentadas: {SHEET_SOURCE_ALTS}).")
                print(f"[DEBUG] Folha de origem usada: '{sheet_used}' (id={ws_src_id})")

                # 2) Ler B3 (valor a replicar em todas as linhas)
                b3_vals = get_range_values(token, drive_id, item_id, sess_id, ws_src_id, "B3:B3")
                b3_value = None
                if b3_vals and b3_vals[0]:
                    b3_value = b3_vals[0][0]
                print(f"[DEBUG] Valor B3 lido: {b3_value!r}")
                print(f"[DEBUG] Nome da pasta (extra coluna): {folder_name_simple!r}")

                # 3) Ler cabeçalho B5:G5
                header_vals = get_range_values(token, drive_id, item_id, sess_id, ws_src_id, "B5:G5")
                header = [str(x).replace("\xa0"," ").strip() for x in (header_vals[0] if header_vals else [])]
                expected = [COL_MARCAS] + VAL_COLS
                expected_norm = [str(x).replace("\xa0"," ").strip() for x in expected]
                print(f"[DEBUG] Header lido: {header}")
                print(f"[DEBUG] Header esperado: {expected_norm}")
                if header != expected_norm:
                    raise RuntimeError(f"Header inesperado.\nEsperado: {expected_norm}\nEncontrado: {header}")

                # 4) Ler corpo B6:G{fim}
                end_row = 6 + MAX_ROWS_READ - 1
                body_addr = f"B6:G{end_row}"
                body_vals = get_range_values(token, drive_id, item_id, sess_id, ws_src_id, body_addr)

                # Limpar cauda vazia
                clean_rows = [row for row in body_vals if any(c not in (None, "",) for c in row)]
                print(f"[DEBUG] Linhas lidas do corpo: {len(body_vals)} | após limpeza: {len(clean_rows)}")

                out_rows = build_output_from_values(clean_rows)
                print(f"[DEBUG] Registos de marcas calculados (antes dos extras): {len(out_rows)}")
                if out_rows:
                    print(f"[DEBUG] Primeiro registo base (preview): {out_rows[0]}")

                # 5) Adicionar colunas extra (B3 e Pasta) no fim de cada linha
                # Base tem 11 colunas; com 2 extras → 13 colunas
                out_rows = [list(r) + [b3_value, folder_name_simple] for r in out_rows]
                if out_rows:
                    print(f"[DEBUG] Primeiro registo com extras (preview): {out_rows[0]}")

                # 6) Preparar destino: recriar folha para evitar resíduos
                ws_dst_id = get_worksheet_id_by_name(token, drive_id, item_id, sess_id, SHEET_TARGET)
                if ws_dst_id:
                    delete_worksheet(token, drive_id, item_id, sess_id, ws_dst_id)
                ws_dst_id = add_worksheet(token, drive_id, item_id, sess_id, SHEET_TARGET)

                # 7) Escrever cabeçalho + dados (A1:M...)
                header_out = [COL_MARCAS] + VAL_COLS + PCT_COLS + EXTRA_COLS
                patch_range_values(token, drive_id, item_id, sess_id, ws_dst_id, "A1:M1", [pad_row(header_out, 13)])

                if out_rows:
                    # garantir 13 colunas (A..M)
                    out_rows = [pad_row(r, 13) for r in out_rows]
                    # corrigido off-by-one: última linha = 1 + len(out_rows)
                    end_out = 1 + len(out_rows)
                    addr_out = f"A2:M{end_out}"
                    print(f"[DEBUG] Vou escrever {len(out_rows)} linhas x 13 colunas em {addr_out}")
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
