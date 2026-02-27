# ========================== IMPORTS ==========================
import os, json, requests, msal 
import pandas as pd 
import unicodedata 
import re 
import time 
import math 

# ========================== GRAPH BASE =======================
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========================== CONFIG ===========================
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")

# ---- FONTES ----
AST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
AST_TABLE = "Meses"

BST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape24M.xlsx"
BST_TABLE = "Dados"

CST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/PAINEL_WBRANDS_26.xlsx"
CST_TABLE = "Painel"

# ---- DESTINO (Excel) ----
DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.xlsx"
DST_TABLE = "Historico"

# ---- DESTINO CSV ----
CSV_DEST_PATH = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.csv"

# ---- COLUNAS FINAIS ----
DST_COLUMNS = [
    "ref_visita","estado","data_registo","data_enc","data_entrega","gsi","empresa",
    "apresentacao","ref_farmacia","nome_farmacia","anf","segmentacao_otc","morada",
    "cp","cp_ext","distrito","concelho","freguesia","localidade","grupos","armazem",
    "armazenista","cod_produto","cod_sap_produto","biu_hmr","email","nome_facturar",
    "nif","telefone","fax","qt_caixas","bonus_caixa","qt_caixas_confirmadas",
    "bonus_caixa_confirmado","desconto_percentagem","net","gross"
]

# ========================== AUTENTICAÇÃO =====================
app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)

token = app.acquire_token_for_client(
    scopes=["https://graph.microsoft.com/.default"]
)["access_token"]

base_headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ========================== HELPERS BASE GRAPH ===============
def get_site_id():
    return requests.get(
        f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}",
        headers=base_headers
    ).json()["id"]

def get_drive_id(site_id):
    return requests.get(
        f"{GRAPH_BASE}/sites/{site_id}/drive",
        headers=base_headers
    ).json()["id"]

def get_item_id(drive_id, path):
    return requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/root:{path}",
        headers=base_headers
    ).json()["id"]

def create_session(drive_id, item_id):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers,
        data=json.dumps({"persistChanges": True})
    )
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession",
        headers=h
    )

# ========================== UTILIDADES ========================
def _session_headers(session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    return h

def get_ids_for_path(site_id, path):
    drive = get_drive_id(site_id)
    return drive, get_item_id(drive, path)

def read_table(drive_id, item_id, session_id, table):
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

# ========================== MERGES ============================
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

        return (
            df_ast
               .merge(df_bst, how="left", left_on="Refª Visita", right_on="Refª")
               .merge(df_cst, how="left", left_on="Ref. Farmácia", right_on="Ref")
        )
    finally:
        close_session(ast_drive, ast_item, sess_ast)
        close_session(cst_drive, cst_item, sess_cst)

# ========================== NORMALIZAÇÃO ======================
def _norm(s):
    s = str(s).lower().replace("refª","ref").replace("ref.","ref").replace("dim","gsi")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^\w]+","_",s).strip("_")

def build_dataframe_for_dst(df):
    ren = {}
    for c in df.columns:
        for d in DST_COLUMNS:
            if _norm(c) == _norm(d):
                ren[c] = d
                break
    return df.rename(columns=ren).reindex(columns=DST_COLUMNS)

# ========================== REGRA WBRANDS ======================
def apply_empresa_wbrands_rule(df):
    df = df.copy()
    mask = df["empresa"].astype(str).str.upper() == "WBRANDS"
    tokens = df.loc[mask, "apresentacao"].fillna("").astype(str).str.strip().str.split().str[0]
    df.loc[mask & tokens.ne(""), "empresa"] = tokens[tokens.ne("")]
    return df

# ========================== DATAS ==============================
def convert_excel_serial_dates(df, cols):
    df = df.copy()
    for col in cols:
        if col not in df.columns: continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() == 0: continue
        df[col] = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
    return df

# ========================== JSON SAFE ==========================
def normalize_cell_for_json(v):
    if v is None: return None
    if isinstance(v, pd.NaT.__class__): return None
    if isinstance(v, pd.Timestamp): return v.strftime("%Y-%m-%d")
    if hasattr(v, "isoformat"): return v.isoformat()
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v): return None
        return v
    if isinstance(v, (str,int)): return v
    return str(v)

# ========================== WRITE TABLE ========================
def clear_and_write_table(drive_id, item_id, table, df):
    sess = create_session(drive_id, item_id)
    h = _session_headers(sess)

    try:
        # headers
        requests.patch(
            f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/headerRowRange",
            headers=h, json={"values":[list(df.columns)]}
        ).raise_for_status()

        # clear
        requests.post(
            f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/dataBodyRange/clear",
            headers=h, json={"applyTo":"all"}
        ).raise_for_status()

        # add data
        rows = [
            [normalize_cell_for_json(v) for v in row]
            for row in df.values.tolist()
        ]

        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table}/rows/add"

        for i in range(0, len(rows), 1000):
            requests.post(url, headers=h, json={"values": rows[i:i+1000]}).raise_for_status()
            time.sleep(0.2)

    finally:
        close_session(drive_id, item_id, sess)

# ========================== CSV ================================
def dataframe_to_csv_bytes(df, sep=","):
    csv_str = df.to_csv(index=False, sep=sep, lineterminator="\n")
    return ("\ufeff" + csv_str).encode("utf-8")

def upload_csv_to_sharepoint(csv_bytes, dest_path):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{dest_path}:/content"
    h = dict(base_headers); h["Content-Type"] = "text/csv; charset=utf-8"
    requests.put(url, headers=h, data=csv_bytes).raise_for_status()

# ========================== PIPELINE FINAL =====================
def build_and_write_to_dst():
    df = build_merged_dataframe()
    df = build_dataframe_for_dst(df)
    df = apply_empresa_wbrands_rule(df)
    df = convert_excel_serial_dates(df, ["data_registo","data_enc","data_entrega"])

    # >>> NORMALIZAR PARA APENAS DATA <<<
    for col in ["data_registo","data_enc","data_entrega"]:
        if col in df.columns:
            df[col] = df[col].dt.date

    # ==========================================================
    #         FILTRO FINAL PARA APENAS ESTAS EMPRESAS
    # ==========================================================
    EMPRESAS_WHITELIST = {
        "bbraun","dr. scholl's","infacol","kelo.cell","lifergy",
        "medela","monchique","moskout","pranarom","roche",
        "sidefarma","wab","wbrands"
    }

    def _norm_empresa(x):
        return str(x).strip().lower() if x is not None else ""

    before = len(df)
    df = df[df["empresa"].apply(lambda x: _norm_empresa(x) in EMPRESAS_WHITELIST)]
    after = len(df)

    print(f"🔎 Filtro Empresas: Removidas {before-after} linhas. Total final: {after}")

    # ---- Escrever Excel ----
    site_id = get_site_id()
    dst_drive, dst_item = get_ids_for_path(site_id, DST_FILE_PATH)
    clear_and_write_table(dst_drive, dst_item, DST_TABLE, df)

    # ---- Exportar CSV ----
    upload_csv_to_sharepoint(
        dataframe_to_csv_bytes(df, sep=","),
        CSV_DEST_PATH
    )

    print(f"✅ Concluído: {after} linhas processadas — Excel + CSV atualizados.")

# ========================== ENTRYPOINT =========================
if __name__ == "__main__":
    build_and_write_to_dst()
