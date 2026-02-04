# ========================== IMPORTS ==========================
import os, json, requests, msal
import pandas as pd
import unicodedata
import re
import time
import math
from io import BytesIO

# ========================== GRAPH BASE =======================
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========================== CONFIG ===========================
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

# ---- FONTES ----
AST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
AST_TABLE      = "Meses"

BST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
BST_TABLE      = "Dados"

CST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/PAINEL_WBRANDS_26.xlsx"
CST_TABLE      = "Painel"

# ---- DESTINO (tabela) ----
DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.xlsx"
DST_TABLE      = "Historico"

# === CSV EXPORT ===
# Caminho do CSV a criar no SharePoint (podes mudar o nome/pasta à vontade)
CSV_DEST_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.csv"

# Colunas da tabela destino (ordem exata)
DST_COLUMNS = [
    "ref_visita","estado","data_registo","data_enc","data_entrega","gsi","empresa",
    "apresentacao","ref_farmacia","nome_farmacia","anf","segmentacao_otc","morada",
    "cp","cp_ext","distrito","concelho","freguesia","localidade","grupos","armazem",
    "armazenista","cod_produto","cod_sap_produto","biu_hmr","email","nome_facturar",
    "nif","telefone","fax","qt_caixas","bonus_caixa","qt_caixas_confirmadas",
    "bonus_caixa_confirmado","desconto_percentagem","net","gross"
]

# ========================== AUTENTICAÇÃO (INTACTA) ==========
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
token = token_result["access_token"]
base_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ========================== HELPERS BASE GRAPH (INTACTOS) ===
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

# ========================== UTILIDADES ======================
def _session_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_ids_for_path(site_id, path):
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, path)
    return drive_id, item_id

def read_table(drive_id, item_id, session_id, table):
    """Lê uma tabela Excel via Graph (header + body) para DataFrame."""
    h = _session_headers(session_id)

    hdr = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/headerRowRange",
        headers=h
    ).json()["values"][0]

    body = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/dataBodyRange",
        headers=h
    ).json().get("values", [])

    return pd.DataFrame(body, columns=hdr)

# ========================== MERGES ==========================
#  - AST["Refª Visita"]   ⟵ LEFT ⟶  BST["Refª"]
#  - (AST+BST)["Ref. Farmácia"] ⟵ LEFT ⟶  CST["Ref"]
def build_merged_dataframe():
    site_id = get_site_id()

    ast_drive, ast_item = get_ids_for_path(site_id, AST_FILE_PATH)
    cst_drive, cst_item = get_ids_for_path(site_id, CST_FILE_PATH)

    sess_ast = create_session(ast_drive, ast_item)
    sess_cst = create_session(cst_drive, cst_item)

    try:
        df_ast = read_table(ast_drive, ast_item, sess_ast, AST_TABLE)
        df_bst = read_table(ast_drive, ast_item, sess_ast, BST_TABLE)
        df_cst = read_table(cst_drive, cst_item, sess_cst, CST_TABLE)

        df = (
            df_ast
            .merge(df_bst, how="left", left_on="Refª Visita", right_on="Refª")
            .merge(df_cst, how="left", left_on="Ref. Farmácia", right_on="Ref")
        )

        return df

    finally:
        close_session(ast_drive, ast_item, sess_ast)
        close_session(cst_drive, cst_item, sess_cst)

# ========================== NORMALIZAÇÃO -> DST ====================
def _norm(s):
    s = str(s).lower().replace("refª", "ref").replace("ref.", "ref")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^\w]+", "_", s).strip("_")

def build_dataframe_for_dst(df):
    """Mapeia/renomeia colunas do merge para corresponder exatamente a DST_COLUMNS e aplica a mesma ordem."""
    rename = {}
    for c in df.columns:
        for d in DST_COLUMNS:
            if _norm(c) == _norm(d):
                rename[c] = d
                break
    df = df.rename(columns=rename)
    df = df.reindex(columns=DST_COLUMNS)
    return df

# ========================== REGRA DE NEGÓCIO (WBRANDS) ====================
def apply_empresa_wbrands_rule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se empresa == 'WBRANDS', substituir pelo primeiro token da coluna 'apresentacao'.
    Se 'apresentacao' estiver vazia, mantém 'WBRANDS'.
    """
    df = df.copy()
    mask = df["empresa"].astype(str).str.upper() == "WBRANDS"
    first_token = (
        df.loc[mask, "apresentacao"]
          .fillna("")
          .astype(str)
          .str.strip()
          .str.split()
          .str[0]
    )
    non_empty = first_token.ne("")
    df.loc[mask & non_empty, "empresa"] = first_token[non_empty]
    return df

# ========================== JSON-SAFE (por célula) ====================
def json_safe_value(v):
    """Converte NaN/Inf para None (JSON válido)."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v

# ========================== WRITE (CLEAR + ROWS/ADD) ====================
def clear_and_write_table(drive_id, item_id, table, df):
    """Escreve df na tabela: PATCH header, CLEAR body, ROWS/ADD em blocos."""
    sess = create_session(drive_id, item_id)
    h = _session_headers(sess)

    try:
        # 1) Header
        requests.patch(
            f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/headerRowRange",
            headers=h, json={"values": [list(df.columns)]}
        ).raise_for_status()

        # 2) Limpar corpo
        requests.post(
            f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/dataBodyRange/clear",
            headers=h, json={"applyTo": "all"}
        ).raise_for_status()

        # 3) Adicionar linhas (chunked) com JSON-safe
        raw_rows = df.values.tolist()
        rows = [[json_safe_value(v) for v in row] for row in raw_rows]
        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/rows/add"

        for i in range(0, len(rows), 1000):
            requests.post(url, headers=h, json={"values": rows[i:i+1000]}).raise_for_status()
            time.sleep(0.2)

    finally:
        close_session(drive_id, item_id, sess)

# ========================== CSV EXPORT =====================
def upload_csv_to_sharepoint(csv_bytes: bytes, dest_path: str):
    """
    Faz upload (cria/sobrescreve) de um ficheiro CSV no SharePoint via Graph:
    PUT /drives/{drive_id}/root:{dest_path}:/content
    """
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)

    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{dest_path}:/content"
    headers = dict(base_headers)
    headers["Content-Type"] = "text/csv; charset=utf-8"

    r = requests.put(url, headers=headers, data=csv_bytes)
    r.raise_for_status()
    return r.json()

def dataframe_to_csv_bytes(df: pd.DataFrame, sep: str = ",") -> bytes:
    """
    Converte um DataFrame para CSV (UTF-8 BOM) e devolve bytes.
    - Por omissão usa separador vírgula (',').
    - Se preferires ponto-e-vírgula (';'), muda o parâmetro sep.
    """
    # BOM para abrir diretamente no Excel com acentuação correta
    csv_str = df.to_csv(index=False, sep=sep, lineterminator="\n")
    # UTF-8 BOM
    return ("\ufeff" + csv_str).encode("utf-8")

# ========================== PIPELINE FINAL ==================
def build_and_write_to_dst():
    # 1) Merge
    df_merged = build_merged_dataframe()

    # 2) Conformidade com o schema da DST
    df_dst = build_dataframe_for_dst(df_merged)

    # 3) Regra de negócio: empresa WBRANDS -> 1ª palavra da apresentação
    df_dst = apply_empresa_wbrands_rule(df_dst)

    # 4) Escrever no destino (tabela Excel)
    site_id = get_site_id()
    dst_drive, dst_item = get_ids_for_path(site_id, DST_FILE_PATH)
    clear_and_write_table(dst_drive, dst_item, DST_TABLE, df_dst)

    # 5) Exportar também para CSV (no mesmo site/drive)
    #    -> Se preferires ';' como separador (muito comum em PT), usa sep=';'
    csv_bytes = dataframe_to_csv_bytes(df_dst, sep=",")  # ou sep=";"
    upload_csv_to_sharepoint(csv_bytes, CSV_DEST_PATH)

    print(f"✅ Concluído: {len(df_dst)} linhas gravadas em '{DST_TABLE}' e CSV criado em '{CSV_DEST_PATH}'")

# ========================== ENTRYPOINT ======================
if __name__ == "__main__":
    build_and_write_to_dst()
