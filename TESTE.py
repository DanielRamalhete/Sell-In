import os, io, requests, msal, pandas as pd, openpyxl, unicodedata, re, math
from datetime import datetime, timedelta, timezone
import calendar

# ========================== CONFIG ===========================
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH     = os.getenv("SITE_PATH")

# Fontes
HISTORICO_FILE_PATH      = "/General/Teste - Daniel PowerAutomate/Historico Sell In.xlsx"
HISTORICO_SHEET_LSTPRD   = "LstPrd"      # Sheet 1 — tem Data Entrega e Refª Visita
HISTORICO_SHEET_PHRORD   = "PhrOrdLst"  # Sheet 0 — tem Refª

PAINEL_FILE_PATH         = "/General/Teste - Daniel PowerAutomate/PAINEL_WBRANDS_26.xlsx"
PAINEL_SHEET             = "Painel Wize Brands"

# Destinos
DST_FILE_PATH            = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.xlsx"
DST_SHEET                = "Historico"
CSV_DEST_PATH            = "/General/Teste - Daniel PowerAutomate/GreenTapeFinal.csv"

# Colunas finais
DST_COLUMNS = [
    "ref_visita", "estado", "data_registo", "data_enc", "data_entrega", "gsi", "empresa",
    "apresentacao", "ref_farmacia", "nome_farmacia", "anf", "segmentacao_otc", "morada",
    "cp", "cp_ext", "distrito", "concelho", "freguesia", "localidade", "grupos", "armazem",
    "armazenista", "cod_produto", "cod_sap_produto", "biu_hmr", "email", "nome_facturar",
    "nif", "telefone", "fax", "qt_caixas", "bonus_caixa", "qt_caixas_confirmadas",
    "bonus_caixa_confirmado", "desconto_percentagem", "net", "gross"
]

# Empresas permitidas
EMPRESAS_WHITELIST = {
    "bbraun", "dr. scholl's", "infacol", "kelo.cell", "lifergy",
    "medela", "monchique", "moskout", "movicol", "pranarom", "roche",
    "sidefarma", "wab", "wbrands"
}

# ========================== AUTH =============================
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# ========================== GRAPH HELPERS ====================
def get_site_id():
    return requests.get(f"{GRAPH_BASE}/sites/{SITE_HOSTNAME}:/{SITE_PATH}", headers=headers).json()["id"]

def get_drive_id(site_id):
    return requests.get(f"{GRAPH_BASE}/sites/{site_id}/drive", headers=headers).json()["id"]

def get_item_id(drive_id, path):
    return requests.get(f"{GRAPH_BASE}/drives/{drive_id}/root:{path}", headers=headers).json()["id"]

def download_file(drive_id, item_id) -> bytes:
    r = requests.get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content", headers=headers)
    r.raise_for_status()
    return r.content

def upload_file(drive_id, item_id, content: bytes):
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/createUploadSession",
        headers={**headers, "Content-Type": "application/json"},
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}
    )
    r.raise_for_status()
    upload_url = r.json()["uploadUrl"]

    chunk_size = 10 * 1024 * 1024
    total = len(content)
    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total) - 1
        chunk = content[start:end + 1]
        chunk_headers = {
            "Content-Length": str(len(chunk)),
            "Content-Range": f"bytes {start}-{end}/{total}"
        }
        r = requests.put(upload_url, headers=chunk_headers, data=chunk)
        r.raise_for_status()
        print(f"Uploaded bytes {start}-{end}/{total}")

def upload_csv(csv_bytes: bytes, dest_path: str):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{dest_path}:/content"
    h = {**headers, "Content-Type": "text/csv; charset=utf-8"}
    requests.put(url, headers=h, data=csv_bytes).raise_for_status()

# ========================== 24 MONTH CUTOFF ==================
def months_ago(dt, months):
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, tzinfo=dt.tzinfo)

def cutoff_date():
    now = datetime.now(timezone.utc) - timedelta(days=1)
    return months_ago(now, 21).date()

# ========================== NORMALISATION ====================
def _norm(s):
    s = str(s).lower().replace("refª", "ref").replace("ref.", "ref").replace("gsi_zona", "gsi").replace("desconto %", "desconto_percentagem")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^\w]+", "_", s).strip("_")

def normalize_columns(df):
    # Remove duplicates before rename
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    ren = {}
    for c in df.columns:
        for d in DST_COLUMNS:
            if _norm(c) == _norm(d):
                ren[c] = d
                break
    df = df.rename(columns=ren)

    # Remove duplicates again after rename (rename may create new ones)
    dupes = df.columns[df.columns.duplicated()].tolist()
    if dupes:
        print(f"Duplicate columns after rename (dropping): {dupes}")
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    return df.reindex(columns=DST_COLUMNS)

# ========================== WBRANDS RULE =====================
def apply_empresa_wbrands_rule(df):
    df = df.copy()
    mask = df["empresa"].astype(str).str.upper() == "WBRANDS"
    tokens = df.loc[mask, "apresentacao"].fillna("").astype(str).str.strip().str.split().str[0]
    df.loc[mask & tokens.ne(""), "empresa"] = tokens[tokens.ne("")]
    return df

# ========================== MAIN PIPELINE ====================
site_id  = get_site_id()
drive_id = get_drive_id(site_id)

historico_id = get_item_id(drive_id, HISTORICO_FILE_PATH)
painel_id    = get_item_id(drive_id, PAINEL_FILE_PATH)
dst_id       = get_item_id(drive_id, DST_FILE_PATH)

# Download source files
print("Downloading Historico Sell In...")
historico_bytes = download_file(drive_id, historico_id)

print("Downloading Painel WBRANDS...")
painel_bytes = download_file(drive_id, painel_id)

# Load sheets
print("Loading sheets into pandas...")
df_lstprd = pd.read_excel(io.BytesIO(historico_bytes), sheet_name=HISTORICO_SHEET_LSTPRD, engine="openpyxl")
df_phrord = pd.read_excel(io.BytesIO(historico_bytes), sheet_name=HISTORICO_SHEET_PHRORD, engine="openpyxl")

# Load Painel
wb_painel = openpyxl.load_workbook(io.BytesIO(painel_bytes), read_only=True)
print("Painel sheets:", wb_painel.sheetnames)
wb_painel.close()
df_painel = pd.read_excel(io.BytesIO(painel_bytes), sheet_name=PAINEL_SHEET, engine="openpyxl")

print(f"LstPrd rows: {len(df_lstprd)}")
print(f"PhrOrdLst rows: {len(df_phrord)}")
print(f"Painel rows: {len(df_painel)}")

# Filter LstPrd to last 24 months by Data Entrega
cutoff = cutoff_date()
print(f"24-month cutoff: {cutoff}")

df_lstprd["Data Entrega"] = pd.to_datetime(df_lstprd["Data Entrega"], dayfirst=True, errors="coerce")
mask_24m = df_lstprd["Data Entrega"].dt.date >= cutoff
df_lstprd = df_lstprd[mask_24m]
print(f"LstPrd rows after 24-month filter: {len(df_lstprd)}")

# Merge 1: LstPrd + PhrOrdLst via Refª Visita = Refª
df = df_lstprd.merge(df_phrord, how="left", left_on="Refª Visita", right_on="Refª")

# Drop DIM column if present (comes from PhrOrdLst)
df = df.drop(columns=[c for c in df.columns if c.lower() == "dim"], errors="ignore")

# Remove duplicate columns after first merge
dupes_1 = df.columns[df.columns.duplicated()].tolist()
if dupes_1:
    print(f"Duplicate columns after PhrOrdLst merge (dropping): {dupes_1}")
df = df.loc[:, ~df.columns.duplicated(keep="first")]

print(f"After merge with PhrOrdLst: {len(df)} rows")

# Merge 2: result + Painel via Ref. Farmácia = Ref
df = df.merge(df_painel, how="left", left_on="Ref. Farmácia", right_on="Ref")

# Guarantee GSI from Painel
gsi_cols = [c for c in df.columns if c.lower() == "gsi"]
if gsi_cols:
    df["gsi"] = df[gsi_cols[0]]

# Remove duplicate columns after second merge
dupes_2 = df.columns[df.columns.duplicated()].tolist()
if dupes_2:
    print(f"Duplicate columns after Painel merge (dropping): {dupes_2}")
df = df.loc[:, ~df.columns.duplicated(keep="first")]

print(f"After merge with Painel: {len(df)} rows")

# Normalise columns to DST_COLUMNS
df = normalize_columns(df)

# Apply WBRANDS empresa rule
df = apply_empresa_wbrands_rule(df)

# Convert date columns
for col in ["data_registo", "data_enc", "data_entrega"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        df[col] = df[col].dt.date

# Filter empresas whitelist
before = len(df)
df = df[df["empresa"].apply(lambda x: str(x).strip().lower() if x is not None else "").isin(EMPRESAS_WHITELIST)]
after = len(df)
print(f"Empresa filter: removed {before - after} rows. Final total: {after}")

# ========================== WRITE XLSX =======================
print("Writing GreenTapeFinal.xlsx to memory...")

def safe_cell(v):
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if hasattr(v, "isoformat"): return str(v)
    return v

output = io.BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    df.applymap(safe_cell).to_excel(writer, sheet_name=DST_SHEET, index=False)
output.seek(0)
xlsx_bytes = output.read()

print("Uploading GreenTapeFinal.xlsx...")
upload_file(drive_id, dst_id, xlsx_bytes)

# ========================== WRITE CSV ========================
print("Uploading GreenTapeFinal.csv...")
csv_str = df.to_csv(index=False, sep=",", lineterminator="\n")
csv_bytes = ("\ufeff" + csv_str).encode("utf-8")
upload_csv(csv_bytes, CSV_DEST_PATH)

print(f"Done. {after} rows written to Excel and CSV.")
