# ========================== IMPORTS (podes alterar imports) ==========================
import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

# PATCH: CSV exports
import csv
import io

# Acrescentos seguros
import pandas as pd
import unicodedata
import re
import time

# ========================== CONSTANTES BASE GRAPH (intacto) ==========================
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========================== CONFIG ==================================================
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

# ---- Fontes (AST/BST/CST) ----
AST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
AST_TABLE      = "Meses"

BST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
BST_TABLE      = "Dados"

CST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/PAINEL_WBRANDS_26.xlsx"
CST_TABLE      = "Painel"

# ---- Destino (DST) ----
DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.xlsx"
DST_TABLE      = "Historico"

# Colunas da tabela destino (ordem exata)
DST_COLUMNS = [
    "ref_visita","estado","data_registo","data_enc","data_entrega","gsi","empresa",
    "apresentacao","ref_farmacia","nome_farmacia","anf","segmentacao_otc","morada",
    "cp","cp_ext","distrito","concelho","freguesia","localidade","grupos","armazem",
    "armazenista","cod_produto","cod_sap_produto","biu_hmr","email","nome_facturar",
    "nif","telefone","fax","qt_caixas","bonus_caixa","qt_caixas_confirmadas",
    "bonus_caixa_confirmado","desconto_percentagem","net","gross"
]

# ========================== AUTENTICAÇÃO (intacto) ==================================
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ========================== HELPERS BASE GRAPH (intacto) ============================
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

# ========================== ACRESCENTOS: UTILIDADES SEGUROS =========================
def _session_headers(session_id: str):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_ids_for_path(site_id: str, file_path: str):
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, file_path)
    return drive_id, item_id

def open_session(drive_id: str, item_id: str):
    return create_session(drive_id, item_id)

def close_session_safe(drive_id: str, item_id: str, session_id: str):
    try:
        close_session(drive_id, item_id, session_id)
    except Exception:
        pass

def get_table_header_and_data(drive_id: str, item_id: str, session_id: str, table_name: str):
    """Lê header + body de uma tabela Excel e devolve (headers, rows)."""
    h = _session_headers(session_id)

    # Header
    url_hdr = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    rh = requests.get(url_hdr, headers=h); rh.raise_for_status()
    header_values = rh.json().get("values", [])
    headers = header_values[0] if header_values else []

    # Data body
    url_body = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange"
    rb = requests.get(url_body, headers=h); rb.raise_for_status()
    body_values = rb.json().get("values", [])

    return headers, body_values

def table_to_dataframe(drive_id: str, item_id: str, session_id: str, table_name: str) -> pd.DataFrame:
    headers, rows = get_table_header_and_data(drive_id, item_id, session_id, table_name)
    if not headers:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame(columns=headers)
    return pd.DataFrame(rows, columns=headers)

# ========================== ACRESCENTOS: LEFT JOINS ================================
#  - AST["Refª Visita"]   ⟵ LEFT ⟶  BST["Refª"]
#  - (AST+BST)["Ref. Farmácia"] ⟵ LEFT ⟶  CST["Ref"]
def build_merged_dataframe():
    """Lê AST/BST/CST e faz os LEFT JOINs, devolvendo o DataFrame final (pré-mapeamento)."""
    site_id = get_site_id()

    # AST e BST no mesmo workbook (mas abrimos sessão separada só se necessário)
    ast_drive, ast_item = get_ids_for_path(site_id, AST_FILE_PATH)
    bst_drive, bst_item = get_ids_for_path(site_id, BST_FILE_PATH)

    # CST noutro workbook
    cst_drive, cst_item = get_ids_for_path(site_id, CST_FILE_PATH)

    ast_sess = bst_sess = cst_sess = None
    try:
        ast_sess = open_session(ast_drive, ast_item)
        bst_sess = ast_sess if (bst_drive, bst_item) == (ast_drive, ast_item) else open_session(bst_drive, bst_item)
        cst_sess = ast_sess if (cst_drive, cst_item) == (ast_drive, ast_item) else open_session(cst_drive, cst_item)

        df_ast = table_to_dataframe(ast_drive, ast_item, ast_sess, AST_TABLE)
        df_bst = table_to_dataframe(bst_drive, bst_item, bst_sess, BST_TABLE)
        df_cst = table_to_dataframe(cst_drive, cst_item, cst_sess, CST_TABLE)

        print("\n=== DEBUG READ TABLES ===")
        print(f"AST: {df_ast.shape} | cols: {list(df_ast.columns)}")
        print(f"BST: {df_bst.shape} | cols: {list(df_bst.columns)}")
        print(f"CST: {df_cst.shape} | cols: {list(df_cst.columns)}")

        # LEFT JOIN 1: AST ⟵ BST
        merged1 = df_ast.merge(
            df_bst, how="left",
            left_on="Refª Visita", right_on="Refª",
            suffixes=("", "_BST")
        )

        # LEFT JOIN 2: (AST+BST) ⟵ CST
        final_df = merged1.merge(
            df_cst, how="left",
            left_on="Ref. Farmácia", right_on="Ref",
            suffixes=("", "_CST")
        )

        print(f"MERGED: {final_df.shape}")
        return final_df

    finally:
        if cst_sess and cst_sess is not ast_sess:
            close_session_safe(cst_drive, cst_item, cst_sess)
        if bst_sess and bst_sess is not ast_sess:
            close_session_safe(bst_drive, bst_item, bst_sess)
        if ast_sess:
            close_session_safe(ast_drive, ast_item, ast_sess)

# ========================== ACRESCENTOS: NORMALIZAÇÃO / MAPEAMENTO =================
def _normalize_name(s: str) -> str:
    """Normaliza nomes: minúsculas, remove acentos, 'refª/ref.'→'ref', pontuação→underscore; remove sufixos _bst/_cst."""
    if s is None:
        return ""
    s0 = str(s).strip().lower()
    s0 = s0.replace("refª", "ref")
    s0 = s0.replace("ref.", "ref")
    s1 = unicodedata.normalize("NFD", s0)
    s1 = "".join(ch for ch in s1 if not unicodedata.combining(ch))
    s1 = re.sub(r"[^\w]+", "_", s1)
    s1 = re.sub(r"_+", "_", s1).strip("_")
    s1 = re.sub(r"_(bst|cst)$", "", s1)
    return s1

def build_dataframe_for_dst(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    - Renomeia colunas do merge para corresponder a DST_COLUMNS
    - Garante ordem exata e cria colunas em falta como vazias
    """
    df = df_merged.copy()

    # Mapeamento explícito das chaves que sabemos
    rename_map_explicit = {}
    if "Refª Visita" in df.columns:
        rename_map_explicit["Refª Visita"] = "ref_visita"
    if "Ref. Farmácia" in df.columns:
        rename_map_explicit["Ref. Farmácia"] = "ref_farmacia"

    if rename_map_explicit:
        df = df.rename(columns=rename_map_explicit)

    # Construir mapeamento automático por normalização
    current_cols = list(df.columns)
    used_src_cols = set()
    rename_map_auto = {}

    # Preferir colunas sem sufixos (_BST/_CST)
    def sort_key(c):
        return (c.endswith("_BST") or c.endswith("_CST"), c)

    norm_to_srcs = {}
    for c in sorted(current_cols, key=sort_key):
        norm = _normalize_name(c)
        norm_to_srcs.setdefault(norm, []).append(c)

    for target in DST_COLUMNS:
        norm_t = _normalize_name(target)
        already = [k for k, v in rename_map_explicit.items() if v == target]
        if already:
            used_src_cols.add(already[0])
            continue
        candidates = norm_to_srcs.get(norm_t, [])
        chosen = None
        for c in candidates:
            if c not in used_src_cols and c not in rename_map_explicit:
                chosen = c
                break
        if chosen:
            rename_map_auto[chosen] = target
            used_src_cols.add(chosen)

    if rename_map_auto:
        df = df.rename(columns=rename_map_auto)

    # Limpar chaves auxiliares que não fazem parte do destino
    for aux in ["Refª", "Ref", "Ref_CST", "Ref_BST"]:
        if aux in df.columns and aux not in DST_COLUMNS:
            df = df.drop(columns=[aux])

    # Selecionar e ordenar colunas (as que faltem ficam NaN)
    df_out = df.reindex(columns=DST_COLUMNS)

    # Debug simples de cobertura de colunas
    missing_all_nan = [c for c in DST_COLUMNS if c in df_out.columns and df_out[c].isna().all()]
    extra_cols = [c for c in df.columns if c not in DST_COLUMNS]
    print("\n=== DEBUG DST SCHEMA ===")
    print(f"DST columns: {len(DST_COLUMNS)}")
    print(f"Output shape: {df_out.shape}")
    if missing_all_nan:
        print(f"Cols no destino com valores vazios (todos NaN): {missing_all_nan}")
    if extra_cols:
        print(f"Cols do merge que não entram no destino: {extra_cols[:20]}{'...' if len(extra_cols)>20 else ''}")

    return df_out

# ========================== ACRESCENTOS: ESCRITA NA TABELA DST (sem resize) =========
def _post_with_debug(url, headers, json_payload, label):
    resp = requests.post(url, headers=headers, json=json_payload)
    print(f"\n-- {label} --")
    print("URL:", url)
    print("Status:", resp.status_code)
    txt = resp.text
    print("Response:", txt[:800] + ("..." if len(txt) > 800 else ""))
    resp.raise_for_status()
    return resp

def clear_table_body(drive_id: str, item_id: str, session_id: str, table_name: str):
    """Limpa o corpo da tabela (dataBodyRange.clear)."""
    h = _session_headers(session_id)

    # Obter range do corpo (debug)
    body_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange"
    rb = requests.get(body_url, headers=h)
    print("\n-- GET dataBodyRange --")
    print("Status:", rb.status_code)
    print("Response:", rb.text[:800])
    rb.raise_for_status()

    # Clear (mesmo que esteja vazio é OK)
    clear_url = f"{body_url}/clear"
    _post_with_debug(clear_url, h, {"applyTo": "all"}, "POST clear (dataBodyRange)")

def add_rows_to_table_chunked(
    drive_id: str,
    item_id: str,
    session_id: str,
    table_name: str,
    df: pd.DataFrame,
    chunk_size: int = 1000,
    pause_sec: float = 0.2
):
    """
    Adiciona linhas via /rows/add em blocos.
    Assume header já escrito e corpo limpo.
    """
    h = _session_headers(session_id)
    add_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/add"

    total = len(df)
    if total == 0:
        print("⚠️ DF sem linhas — nada para adicionar.")
        return

    values = df.values.tolist()
    start = 0
    batch = 1
    while start < total:
        end = min(start + chunk_size, total)
        payload = {"values": values[start:end]}
        print(f"\n== Add batch #{batch}: rows {start}..{end-1} (count={end-start}) ==")
        _post_with_debug(add_url, h, payload, f"POST rows/add (batch {batch})")
        start = end
        batch += 1
        if pause_sec:
            time.sleep(pause_sec)

    print(f"✅ Adicionadas {total} linhas à tabela '{table_name}'.")

def write_dataframe_to_table(
    drive_id: str,
    item_id: str,
    session_id: str,
    table_name: str,
    df: pd.DataFrame
):
    """
    Estratégia resiliente sem usar /resize:
      1) Debug do range atual
      2) PATCH headerRowRange com nomes das colunas
      3) Limpar dataBodyRange (clear)
      4) Adicionar linhas via rows/add em blocos
    """
    h = _session_headers(session_id)

    print("\n========== WRITE TABLE (resiliente) ==========")
    print(f"Tabela: {table_name}")
    print(f"Linhas df: {len(df)} | Cols df: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")

    # 1) Range atual (debug)
    rng_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    rr = requests.get(rng_url, headers=h)
    print("\n-- GET table range --")
    print("Status:", rr.status_code)
    print("Response:", rr.text[:800])
    rr.raise_for_status()
    data = rr.json()
    addr_local = data.get("address") or data.get("addressLocal")
    print("Current table address:", addr_local)

    # 2) Header
    header_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r_hdr = requests.patch(header_url, headers=h, json={"values": [list(df.columns)]})
    print("\n-- PATCH header --")
    print("Status:", r_hdr.status_code)
    print("Response:", r_hdr.text[:800])
    r_hdr.raise_for_status()

    # 3) Clear body
    clear_table_body(drive_id, item_id, session_id, table_name)

    # 4) Add dados em blocos
    add_rows_to_table_chunked(drive_id, item_id, session_id, table_name, df, chunk_size=1000, pause_sec=0.2)

    print("✅ WRITE COMPLETED (sem resize)")
    print("=============================================\n")

# ========================== PIPELINE COMPLETA =======================================
def build_and_write_to_dst():
    """
    1) Lê AST/BST/CST e faz merge
    2) Conforma colunas ao schema DST (ordem/nome)
    3) Escreve tudo na tabela DST
    """
    # 1) Merge
    df_merged = build_merged_dataframe()

    # 2) Conformidade com a DST
    df_dst = build_dataframe_for_dst(df_merged)

    # 3) Escrever no ficheiro destino / tabela destino
    site_id = get_site_id()
    dst_drive, dst_item = get_ids_for_path(site_id, DST_FILE_PATH)

    sess = None
    try:
        sess = open_session(dst_drive, dst_item)
        write_dataframe_to_table(dst_drive, dst_item, sess, DST_TABLE, df_dst)
        print(f"✅ Gravado na '{DST_TABLE}' do '{DST_FILE_PATH}' — {df_dst.shape[0]} linhas x {df_dst.shape[1]} colunas.")
    finally:
        if sess:
            close_session_safe(dst_drive, dst_item, sess)

# ========================== ENTRYPOINT =============================================
if __name__ == "__main__":
    build_and_write_to_dst()
