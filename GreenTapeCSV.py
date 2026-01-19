
import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

# PATCH: CSV exports
import csv
import io


GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME  = os.getenv("SITE_HOSTNAME")
SITE_PATH      = os.getenv("SITE_PATH")

DST_FILE_PATH  = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
DST_TABLE      = "Historico"
DATE_COLUMN    = "Data Entrega"

# Estratégia: "block" (sort + delete bloco) ou "batch" (deletes em lotes descendentes)
MODE           = os.getenv("MODE", "block")   # "block" | "batch"
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "20"))

# "rolling" = últimos 24 meses a partir de hoje; "fullmonth" = desde 1º dia do mês corrente - 24 meses
CUTOFF_MODE    = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" | "fullmonth"
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

# ---------- Excel helpers ----------
def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

def get_table_header_and_rows(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range",
        headers=h
    )
    r.raise_for_status()

    values = r.json().get("values", [])
    if not values:
        return {"headers": [], "rows": []}

    return {
        "headers": values[0],
        "rows": values[1:]
    }

def get_table_databody_range(drive_id, item_id, table_name, session_id):
    h = workbook_headers(session_id)
    r = requests.get(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/dataBodyRange",
        headers=h
    )
    r.raise_for_status()
    return r.json()

def table_sort_by_column(drive_id, item_id, table_name, session_id, column_index, ascending=True):
    h = workbook_headers(session_id)
    body = {
        "fields": [{"key": column_index, "ascending": ascending}],
        "matchCase": False
    }
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/sort/apply",
        headers=h,
        data=json.dumps(body)
    )
    r.raise_for_status()

def delete_range_on_sheet(drive_id, item_id, sheet_name, addr_a1, session_id):
    h = workbook_headers(session_id)
    url = (
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
        f"/workbook/worksheets/{sheet_name}"
        f"/range(address='{addr_a1}')/delete"
    )
    r = requests.post(url, headers=h, data=json.dumps({"shift": "Up"}))
    r.raise_for_status()

def delete_table_row(drive_id, item_id, table_name, session_id, row_index):
    h = workbook_headers(session_id)
    r = requests.delete(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
        f"/workbook/tables/{table_name}/rows/{row_index}",
        headers=h
    )
    r.raise_for_status()


# ---------- A1 utils ----------
def _parse_a1_address(addr):
    sheet, rng = addr.split("!", 1)
    start, end = rng.split(":")
    return sheet, start, end

def _split_col_row(a1):
    i = 0
    while i < len(a1) and a1[i].isalpha():
        i += 1
    return a1[:i], int(a1[i:])


# ---------- Date utils ----------
def months_ago(dt, months):
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1

    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, tzinfo=dt.tzinfo)

def cutoff_datetime():
    now = datetime.now(timezone.utc) - timedelta(days=1)
    if CUTOFF_MODE == "fullmonth":
        now = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return months_ago(now, 24)

def parse_date_any(value):
    if value is None or str(value).strip() == "":
        return None

    if isinstance(value, (int, float)):
        return datetime(1899, 12, 30, tzinfo=timezone.utc) + timedelta(days=float(value))

    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S"
    ):
        try:
            return datetime.strptime(str(value), fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None


# ---------- Batch helpers ----------
def chunked_desc(indices, size):
    indices_sorted = sorted(indices, reverse=True)
    for i in range(0, len(indices_sorted), size):
        yield indices_sorted[i:i + size]

def batch_delete_rows(drive_id, item_id, table_name, session_id, indices_chunk):
    batch_url = f"{GRAPH_BASE}/$batch"
    requests_body = []

    for j, idx in enumerate(indices_chunk, start=1):
        requests_body.append({
            "id": str(j),
            "method": "DELETE",
            "url": f"/drives/{drive_id}/items/{item_id}"
                   f"/workbook/tables/{table_name}/rows/{idx}",
            "headers": {
                "workbook-session-id": session_id
            }
        })

    r = requests.post(
        batch_url,
        headers=base_headers,
        data=json.dumps({"requests": requests_body})
    )
    r.raise_for_status()

def delete_rows_in_batches(drive_id, item_id, table_name, session_id, indices, batch_size):
    deleted = 0
    for chunk in chunked_desc(indices, batch_size):
        batch_delete_rows(drive_id, item_id, table_name, session_id, chunk)
        deleted += len(chunk)
    return deleted


# ---------- CSV helpers (PATCH) ----------
def table_to_csv_bytes(headers, rows, delimiter=";"):
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=delimiter)

    if headers:
        writer.writerow(headers)

    for row in rows:
        writer.writerow(row)

    return buffer.getvalue().encode("utf-8-sig")

def upload_csv_to_sharepoint(drive_id, csv_path, csv_bytes, access_token):
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:{csv_path}:/content"
    r = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "text/csv"
        },
        data=csv_bytes
    )
    r.raise_for_status()

def export_table_to_csv_sharepoint(
    drive_id, item_id, table_name, session_id, excel_path, access_token, delimiter=";"
):
    data = get_table_header_and_rows(
        drive_id, item_id, table_name, session_id
    )

    headers = data.get("headers", [])
    rows = data.get("rows", [])

    if not headers:
        print("Tabela vazia — CSV não gerado.")
        return

    csv_bytes = table_to_csv_bytes(headers, rows, delimiter)
    csv_path = excel_path.replace(".xlsx", ".csv")

    upload_csv_to_sharepoint(
        drive_id,
        csv_path,
        csv_bytes,
        access_token
    )

    print(f"CSV atualizado no SharePoint: {csv_path}")


# ---------- Main ----------
def keep_last_24_months(mode="block"):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, DST_FILE_PATH)
    session_id = create_session(drive_id, item_id)

    try:
        data_all = get_table_header_and_rows(drive_id, item_id, DST_TABLE, session_id)
        headers = data_all["headers"]
        rows = data_all["rows"]

        date_col_idx = headers.index(DATE_COLUMN)
        cutoff = cutoff_datetime()

        if mode == "block":
            table_sort_by_column(
                drive_id, item_id, DST_TABLE,
                session_id, date_col_idx, True
            )

            body = get_table_databody_range(
                drive_id, item_id, DST_TABLE, session_id
            )

            delete_count = 0
            for r in body["values"]:
                dt = parse_date_any(r[date_col_idx])
                if dt is None or dt >= cutoff:
                    break
                delete_count += 1

            if delete_count > 0:
                sheet, start, end = _parse_a1_address(body["address"])
                col, row = _split_col_row(start)
                end_col, _ = _split_col_row(end)

                del_addr = f"{col}{row}:{end_col}{row + delete_count - 1}"
                delete_range_on_sheet(
                    drive_id, item_id, sheet, del_addr, session_id
                )

        elif mode == "batch":
            indices = []
            for i, r in enumerate(rows):
                dt = parse_date_any(r[date_col_idx])
                if dt and dt < cutoff:
                    indices.append(i)

            if indices:
                delete_rows_in_batches(
                    drive_id,
                    item_id,
                    DST_TABLE,
                    session_id,
                    indices,
                    BATCH_SIZE
                )

        # PATCH: CSV export (no changes to original flow)
        export_table_to_csv_sharepoint(
            drive_id=drive_id,
            item_id=item_id,
            table_name=DST_TABLE,
            session_id=session_id,
            excel_path=DST_FILE_PATH,
            access_token=token,
            delimiter=";"
        )

    finally:
        close_session(drive_id, item_id, session_id)


if __name__ == "__main__":
    keep_last_24_months(mode=MODE)
