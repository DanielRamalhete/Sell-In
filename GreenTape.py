import os, json, requests, msal
from datetime import datetime, timedelta, timezone
import calendar

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ========= CONFIG =========
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_HOSTNAME = os.getenv("SITE_HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")

DST_FILE_PATH = "/General/Teste - Daniel PowerAutomate/GreenTape.xlsx"
DST_TABLE = "Historico"
DATE_COLUMN = "Data Entrega"

# Estratégia: "block" (sort + delete contíguo) ou "batch" (deletes em lotes descendentes)
MODE = os.getenv("MODE", "block")  # "block" | "batch"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

# "rolling" = últimos 24 meses a partir de hoje; "fullmonth" = 1º dia do mês corrente - 24 meses
CUTOFF_MODE = os.getenv("CUTOFF_MODE", "rolling")  # "rolling" | "fullmonth"

# Paginação (para leitura de rows via /rows?$top&$skip)
DEFAULT_TOP = int(os.getenv("GRAPH_ROWS_TOP") or "5000")
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
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/createSession",
        headers=base_headers, data=json.dumps({"persistChanges": True})
    )
    return r.json()["id"]

def close_session(drive_id, item_id, session_id):
    h = dict(base_headers); h["workbook-session-id"] = session_id
    requests.post(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/closeSession", headers=h)

# ---- Helpers Excel comuns ----
def workbook_headers(session_id):
    h = dict(base_headers)
    h["workbook-session-id"] = session_id
    return h

# ===== MESMA ABORDAGEM DO Implementacoes.py (headers + rows paginadas) =====
def get_table_headers(drive_id, item_id, table_name, session_id):
    """Tenta obter os headers via /headerRowRange."""
    h = workbook_headers(session_id)
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/headerRowRange"
    r = requests.get(url, headers=h)
    if not r.ok:
        print("[DEBUG][headerRowRange] STATUS:", r.status_code)
        try: print("[DEBUG][headerRowRange] JSON:", r.json())
        except Exception: print("[DEBUG][headerRowRange] TEXT:", r.text)
        r.raise_for_status()
    rng = r.json()
    values = rng.get("values", [[]])
    headers = [str(x) for x in (values[0] if values and values[0] else [])]
    return headers

def get_table_headers_safe(drive_id, item_id, table_name, session_id):
    """Obtém headers com fallback para /columns e para a primeira linha de /range."""
    # 1) oficial
    try:
        headers = get_table_headers(drive_id, item_id, table_name, session_id)
        if headers:
            print(f"[DEBUG] headerRowRange → {headers}")
            return headers
    except requests.HTTPError:
        print("[DEBUG] headerRowRange falhou; a tentar fallback por /columns...")

    h = workbook_headers(session_id)

    # 2) /columns -> names
    url_cols = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/columns"
    rc = requests.get(url_cols, headers=h)
    if rc.ok:
        cols = rc.json().get("value", [])
        names = [c.get("name") for c in cols if c.get("name") is not None]
        if names:
            print("[DEBUG] Fallback /columns →", names)
            return names
        else:
            print("[DEBUG] /columns devolveu lista vazia ou sem 'name'.")
    else:
        print("[DEBUG] /columns falhou. STATUS:", rc.status_code)
        try: print("[DEBUG] /columns JSON:", rc.json())
        except Exception: print("[DEBUG] /columns TEXT:", rc.text)

    # 3) /range -> primeira linha
    url_rng = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/range"
    rr = requests.get(url_rng, headers=h)
    if rr.ok:
        rng = rr.json()
        vals = rng.get("values", [[]])
        if vals and vals[0]:
            headers = [str(x) for x in vals[0]]
            print("[DEBUG] Fallback /range primeira linha →", headers)
            return headers
        else:
            print("[DEBUG] /range não devolveu valores (ou primeira linha vazia).")
    else:
        print("[DEBUG] /range falhou. STATUS:", rr.status_code)
        try: print("[DEBUG] /range JSON:", rr.json())
        except Exception: print("[DEBUG] /range TEXT:", rr.text)
    rr.raise_for_status()  # força erro p/ ver detalhe

def list_table_rows_paged(drive_id, item_id, table_name, session_id, top=None, max_pages=100000):
    """
    Itera páginas usando $top/$skip para evitar ResponsePayloadSizeLimitExceeded.
    Cada item tem 'index' (0-based na Tabela) e 'values'.
    """
    if top is None:
        top = DEFAULT_TOP
    h = workbook_headers(session_id)
    base_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows"
    skip = 0; page = 0
    while page < max_pages:
        page += 1
        url = f"{base_url}?$top={top}&$skip={skip}"
        r = requests.get(url, headers=h)
        if not r.ok:
            print("[DEBUG][list_table_rows_paged] URL:", url)
            print("[DEBUG][list_table_rows_paged] STATUS:", r.status_code)
            try: print("[DEBUG][list_table_rows_paged] JSON:", r.json())
            except Exception: print("[DEBUG][list_table_rows_paged] TEXT:", r.text)
            r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            print(f"[DEBUG][list_table_rows_paged] Fim paginação. pages={page-1}")
            break
        print(f"[DEBUG][list_table_rows_paged] page={page} top={top} skip={skip} count={len(batch)}")
        for row in batch:
            yield row
        skip += top

# ===== Outras helpers específicas do Excel =====
def table_sort_by_column(drive_id, item_id, table_name, session_id, column_index_zero_based, ascending=True):
    """
    Ordena a tabela pelo índice de coluna (0-based dentro da tabela).
    Endpoint: /workbook/tables/{name}/sort/apply
    """
    h = workbook_headers(session_id)
    body = {
        "fields": [{"key": column_index_zero_based, "ascending": ascending}],
        "matchCase": False
    }
    r = requests.post(
        f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/sort/apply",
        headers=h, data=json.dumps(body)
    )
    r.raise_for_status()

# ---- Utilidades de endereço A1 ----
def _parse_a1_address(addr):
    # Ex.: "Folha1!A2:Z100" -> ("Folha1", "A2", "Z100")
    if "!" in addr:
        sheet, rng = addr.split("!", 1)
    else:
        sheet, rng = None, addr
    if ":" in rng:
        start, end = rng.split(":", 1)
    else:
        start, end = rng, rng
    return sheet, start, end

def _split_col_row(a1):
    # "AB123" -> ("AB", 123)
    i = 0
    while i < len(a1) and a1[i].isalpha():
        i += 1
    return a1[:i], int(a1[i:]) if i < len(a1) else 1

# ---- Utilidades de data ----
def months_ago(dt: datetime, months: int) -> datetime:
    """
    Subtrai 'months' meses de dt preservando o dia quando possível.
    """
    year = dt.year
    month = dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = dt.day
    max_day = calendar.monthrange(year, month)[1]
    if day > max_day:
        day = max_day
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)

def cutoff_datetime(mode: str = "rolling") -> datetime:
    """
    Devolve o instante de corte para 'últimos 24 meses'.
    - 'rolling': desde agora-24m (usando now UTC - 1 dia para evitar zona cinzenta do dia corrente)
    - 'fullmonth': desde o 1º dia do mês corrente - 24 meses (00:00 UTC)
    """
    now_utc = datetime.now(timezone.utc) - timedelta(days=1)
    if mode == "fullmonth":
        start_this_month = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return months_ago(start_this_month, 24)
    return months_ago(now_utc, 24)

def parse_date_any(value):
    """
    Interpreta células de data: string, número serial Excel ou ISO.
    Retorna timezone-aware (UTC) ou None se não conseguir.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    # Excel serial date (dias desde 1899-12-30)
    if isinstance(value, (int, float)):
        try:
            excel_epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
            return excel_epoch + timedelta(days=float(value))
        except Exception:
            pass
    if isinstance(value, str):
        s = value.strip()
        # ISO comum
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
        # Formatos PT comuns
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None

# ---- DELETE via $batch (ItemAt) + fallback sequencial ----
def delete_single_row_itemat(drive_id, item_id, table_name, session_id, idx):
    """Apaga 1 linha via ItemAt(index=idx). Útil como fallback sequencial."""
    h = workbook_headers(session_id)
    abs_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
    r = requests.delete(abs_url, headers=h)
    if not r.ok:
        print("[DEBUG][DEL-ONE] STATUS:", r.status_code)
        try: print("[DEBUG][DEL-ONE] JSON:", r.json())
        except Exception: print("[DEBUG][DEL-ONE] TEXT:", r.text)
        return False
    return True

def delete_table_rows_by_index_batch(
    drive_id, item_id, table_name, session_id, row_indices,
    max_batch_size=20, max_retries=3, fallback_sequential=True
):
    """
    Apaga linhas por índices via $batch com ItemAt(index=...).
    Se algum subpedido falhar, opcionalmente tenta em modo sequencial para esse índice.
    """
    if not row_indices:
        print("[DEBUG][BATCH-DEL] Sem índices para apagar.")
        return {"deleted": 0, "failed": []}

    # normaliza e ordena descendente para evitar shift
    row_indices = sorted(set(row_indices), reverse=True)
    print(f"[DEBUG][BATCH-DEL] Total de índices para apagar: {len(row_indices)}")

    deleted_total = 0
    failed_global = []

    def chunks(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i+size]

    batch_endpoint = f"{GRAPH_BASE}/$batch"

    for chunk in chunks(row_indices, max_batch_size):
        requests_list = []
        for i, idx in enumerate(chunk, start=1):
            rel_url = f"/drives/{drive_id}/items/{item_id}/workbook/tables/{table_name}/rows/$/ItemAt(index={idx})"
            requests_list.append({
                "id": str(i),
                "method": "DELETE",
                "url": rel_url,
                "headers": {"workbook-session-id": session_id}
            })
        print("[DEBUG][BATCH-DEL] URLs no lote:", [req["url"] for req in requests_list])

        payload = {"requests": requests_list}
        attempt = 0

        while True:
            attempt += 1
            print(f"[DEBUG][BATCH-DEL] POST {batch_endpoint} (lote {len(chunk)}, tentativa {attempt})")
            r = requests.post(batch_endpoint, headers=base_headers, data=json.dumps(payload))

            # Throttling
            if r.status_code == 429 and attempt <= max_retries:
                ra = int(r.headers.get("Retry-After", "5"))
                print(f"[DEBUG][BATCH-DEL] 429 recebido. A aguardar {ra}s…")
                import time; time.sleep(ra)
                continue

            if not r.ok:
                print("[DEBUG][BATCH-DEL] STATUS:", r.status_code)
                try: print("[DEBUG][BATCH-DEL] JSON:", r.json())
                except Exception: print("[DEBUG][BATCH-DEL] TEXT:", r.text)
                # fallback sequencial para todo o chunk
                if fallback_sequential:
                    print("[DEBUG][BATCH-DEL] Falha no lote. Fallback sequencial de todo o chunk.")
                    for idx in chunk:
                        if delete_single_row_itemat(drive_id, item_id, table_name, session_id, idx):
                            deleted_total += 1
                        else:
                            failed_global.append(idx)
                    break  # sai do while True e passa ao próximo chunk
                else:
                    # sem fallback → marca todos como falhados
                    failed_global.extend(chunk)
                    break

            # OK: analisar respostas
            resp = r.json()
            ok_ids = [e for e in resp.get("responses", []) if e.get("status") in (200, 204)]
            deleted_total += len(ok_ids)

            for e in resp.get("responses", []):
                status = e.get("status")
                if status not in (200, 204):
                    body = e.get("body") or {}
                    print("[DEBUG][BATCH-DEL] Falhou id", e.get("id"), "status:", status, "body:", body)
                    # mapear o id ao índice no chunk
                    try:
                        failed_idx = chunk[int(e.get("id")) - 1]
                        failed_global.append(failed_idx)
                    except Exception:
                        pass
            break  # lote processado; avançar para o próximo

    # Se houver falhados e fallback_sequencial=True, tenta novamente individualmente
    if failed_global and fallback_sequential:
        print(f"[DEBUG][BATCH-DEL] {len(failed_global)} falharam no batch. Retry imediato sequencial dessas.")
        retry_failed = sorted(set(failed_global), reverse=True)
        still_failed = []
        for idx in retry_failed:
            if delete_single_row_itemat(drive_id, item_id, table_name, session_id, idx):
                deleted_total += 1
            else:
                still_failed.append(idx)
        failed_global = still_failed

    print(f"[DEBUG][BATCH-DEL] Total rows apagadas (batch+fallback): {deleted_total}")
    return {"deleted": deleted_total, "failed": sorted(set(failed_global), reverse=True)}

# ===== Função principal =====
def keep_last_24_months(mode="block"):
    site_id = get_site_id()
    drive_id = get_drive_id(site_id)
    item_id = get_item_id(drive_id, DST_FILE_PATH)
    session_id = create_session(drive_id, item_id)
    try:
        # Obter headers seguros (como no Implementacoes.py)
        headers = get_table_headers_safe(drive_id, item_id, DST_TABLE, session_id)
        if not headers:
            print("Tabela vazia ou sem headers.")
            return
        try:
            date_col_idx = headers.index(DATE_COLUMN)
        except ValueError:
            raise RuntimeError(f"A coluna '{DATE_COLUMN}' não foi encontrada na tabela '{DST_TABLE}'.")

        cutoff = cutoff_datetime(CUTOFF_MODE)

        if mode == "block":
            # 1) Ordenar ascendente pela coluna de data (0-based)
            table_sort_by_column(drive_id, item_id, DST_TABLE, session_id, date_col_idx, ascending=True)

            # 2) Varre paginado até ao primeiro >= cutoff; acumula índices contíguos do topo
            indices_to_delete = []
            for r in list_table_rows_paged(drive_id, item_id, DST_TABLE, session_id, top=DEFAULT_TOP):
                idx = r.get("index")
                vals = (r.get("values", [[]])[0] or [])
                if idx is None or len(vals) <= date_col_idx:
                    # Linha mal formada; interrompe para segurança
                    break
                dt = parse_date_any(vals[date_col_idx])
                # Parar ao primeiro não parsável ou >= cutoff para manter contiguidade
                if dt is None or dt >= cutoff:
                    break
                indices_to_delete.append(int(idx))

            if not indices_to_delete:
                print("Nenhuma linha para remover (já só tens últimos 24 meses).")
                return

            # 3) Apagar índices contíguos do topo via $batch (ItemAt) + fallback sequencial
            res = delete_table_rows_by_index_batch(
                drive_id, item_id, DST_TABLE, session_id, indices_to_delete,
                max_batch_size=BATCH_SIZE, max_retries=3, fallback_sequential=True
            )
            print(f"[BLOCK→BATCH] Removidas {res['deleted']} linhas anteriores a {cutoff.date()} (falharam {len(res['failed'])}).")

        elif mode == "batch":
            # 1) Ler todas as linhas (sem ordenar) e calcular índices a apagar
            indices_to_delete = []
            for r in list_table_rows_paged(drive_id, item_id, DST_TABLE, session_id, top=DEFAULT_TOP):
                idx = r.get("index")
                vals = (r.get("values", [[]])[0] or [])
                if idx is None or len(vals) <= date_col_idx:
                    continue
                dt = parse_date_any(vals[date_col_idx])
                if dt is None:
                    # mantém (podes alterar para remover)
                    continue
                if dt < cutoff:
                    indices_to_delete.append(int(idx))
            if not indices_to_delete:
                print("Nenhuma linha antiga encontrada. Nada a apagar.")
                return
            res = delete_table_rows_by_index_batch(
                drive_id, item_id, DST_TABLE, session_id, indices_to_delete,
                max_batch_size=BATCH_SIZE, max_retries=3, fallback_sequential=True
            )
            print(f"[BATCH] Removidas {res['deleted']} linhas anteriores a {cutoff.date()} (falharam {len(res['failed'])}).")
        else:
            raise ValueError(f"MODE inválido: {mode}")
    finally:
        close_session(drive_id, item_id, session_id)

# Entrada do script
if __name__ == "__main__":
    keep_last_24_months(mode=MODE)
