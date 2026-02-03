# ========================== IMPORTS (podes alterar imports) ==========================
import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

# PATCH: CSV exports
import csv
import io

# Acrescentos seguros (permitidos): pandas, unicodedata, re
import pandas as pd
import unicodedata
import re

# ========================== CONSTANTES BASE GRAPH (intacto) ==========================
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========================== CONFIG (apenas corrigi quebras de linha) =================
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

# Colunas da tabela destino (ordem exata que pediste)
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
# Chaves que me deste:
#  - AST["Refª Visita"]  ⟵ LEFT ⟶  BST["Refª"]
#  - (AST+BST)["Ref. Farmácia"]  ⟵ LEFT ⟶  CST["Ref"]

def build_merged_dataframe():
    """Lê AST/BST/CST e faz os LEFT JOINs, devolvendo o DataFrame final (ainda sem renomear para DST)."""
    site_id = get_site_id()

    # AST e BST no mesmo workbook
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
    """Normaliza nomes: minúsculas, remove acentos, pontuação → underscore, normaliza 'refª'/'ref.' → 'ref'."""
    if s is None:
        return ""
    s0 = str(s).strip().lower()

    # Normalizações específicas
    s0 = s0.replace("refª", "ref")
    s0 = s0.replace("ref.", "ref")

    # Remover acentos
    s1 = unicodedata.normalize("NFD", s0)
    s1 = "".join(ch for ch in s1 if not unicodedata.combining(ch))

    # Substituir não alfanumérico por underscore
    s1 = re.sub(r"[^\w]+", "_", s1)
    s1 = re.sub(r"_+", "_", s1).strip("_")

    # Remover sufixos de merge (_bst/_cst)
    s1 = re.sub(r"_(bst|cst)$", "", s1)

    return s1

def build_dataframe_for_dst(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    - Renomeia colunas do merge para corresponder a DST_COLUMNS
    - Garante ordem exata e cria colunas em falta como vazias
    """
    df = df_merged.copy()

    # Mapeamento explícito das 2 chaves que sabemos
    rename_map_explicit = {}
    if "Refª Visita" in df.columns:
        rename_map_explicit["Refª Visita"] = "ref_visita"
    if "Ref. Farmácia" in df.columns:
        rename_map_explicit["Ref. Farmácia"] = "ref_farmacia"

    # Aplicar renomeação explícita primeiro
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
        # Se já foi mapeado explicitamente, salta
        already = [k for k, v in rename_map_explicit.items() if v == target]
        if already:
            used_src_cols.add(already[0])
            continue

        candidates = norm_to_srcs.get(norm_t, [])
        # Escolher a 1ª não utilizada
        chosen = None
        for c in candidates:
            if c not in used_src_cols and c not in rename_map_explicit:
                chosen = c
                break
        if chosen:
            rename_map_auto[chosen] = target
            used_src_cols.add(chosen)

    # Aplicar renomeação automática
    if rename_map_auto:
        df = df.rename(columns=rename_map_auto)

    # Remover chaves auxiliares que não existem no destino (opcional)
    for aux in ["Refª", "Ref", "Ref_CST", "Ref_BST"]:
        if aux in df.columns and aux not in DST_COLUMNS:
            df = df.drop(columns=[aux])

    # Selecionar e ordenar colunas (colunas em falta serão criadas com NaN)
    df_out = df.reindex(columns=DST_COLUMNS)

    return df_out

# ========================== ACRESCENTOS: ESCRITA NA TABELA DST =====================
def _parse_address(address: str):
    """
    Recebe um address estilo: 'Folha1'!A1:Z100  ou  A1:Z100
    Devolve (sheet_name or None, start_col_letter, start_row, end_col_letter, end_row)
    """
    sheet = None
    addr = address
    if "!" in address:
        sheet, addr = address.split("!", 1)
        sheet = sheet.strip("'")

    start, end = addr.split(":")
    def split_cell(cell):
        m = re.match(r"([A-Za-z]+)(\d+)", cell)
        return m.group(1), int(m.group(2))
    sc, sr = split_cell(start)
    ec, er = split_cell(end)
    return sheet, sc, sr, ec, er

def _col_letter(n):
    """1->A, 26->Z, 27->AA"""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _col_index(col_letters):
    """A->1, Z->26, AA->27"""
    n = 0
    for ch in col_letters.upper():
        n = n * 26 + (ord(ch) - 64)
    return n

def write_dataframe_to_table(
    drive_id: str,
    item_id: str,
    session_id: str,
    table_name: str,
    df: pd.DataFrame
):
    """
    Escreve df COMPLETO na tabela (header + body) redimensionando a tabela ao novo tamanho.
    Mantém a sessão e usa endpoints do Workbook.
    """
    h = _session_headers(session_id)

    # 1) Obter o range atual da tabela para descobrir folha e célula inicial
    rng_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    rr = requests.get(rng_url, headers=h); rr.raise_for_status()
    addr_local = rr.json().get("address", rr.json().get("addressLocal"))  # 'Folha'!A1:Z50
    if not addr_local:
        raise RuntimeError(f"Não foi possível obter o address da tabela {table_name}")

    sheet_name, start_col_letters, start_row, _, _ = _parse_address(addr_local)
    if sheet_name is None:
        # Fallback (raro): se não vier o nome da folha
        sheet_name = rr.json().get("worksheet", {}).get("name", None)

    # 2) Calcular novo address (tamanho = header + len(df) linhas, n_colunas = df.shape[1])
    n_rows = (len(df) + 1)  # +1 header
    n_cols = len(df.columns)

    start_col_idx = _col_index(start_col_letters)
    end_col_idx = start_col_idx + n_cols - 1
    end_col_letters = _col_letter(end_col_idx)
    end_row = start_row + n_rows - 1

    # Montar address qualificado com nome da folha
    sheet_escaped = sheet_name.replace("'", "''") if sheet_name else None
    address_new = (
        f"'{sheet_escaped}'!{start_col_letters}{start_row}:{end_col_letters}{end_row}"
        if sheet_escaped else f"{start_col_letters}{start_row}:{end_col_letters}{end_row}"
    )

    # 3) Resize da tabela ao novo address
    resize_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/resize"
    requests.post(resize_url, headers=h, json={"address": address_new}).raise_for_status()

    # 4) Escrever HEADER
    header_range_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    requests.patch(header_range_url, headers=h, json={"values": [list(df.columns)]}).raise_for_status()

    # 5) Escrever DATA BODY (se houver linhas)
    if len(df) > 0:
        body_range_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange"
        values = df.values.tolist()
        requests.patch(body_range_url, headers=h, json={"values": values}).raise_for_status()
    # Se não houver linhas, a tabela ficou apenas com header (após resize), o que é válido.

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
    # Executa tudo de ponta a ponta
    build_and_write_to_dst()
