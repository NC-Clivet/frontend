import re

from datetime import date, datetime

import pandas as pd
import streamlit as st
import requests
import json
import streamlit.components.v1 as components
import altair as alt

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import unicodedata

import os

try:
    import google.generativeai as genai
except ImportError:
    genai = None

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or (st.secrets.get("GEMINI_API_KEY") if hasattr(st, "secrets") else None) or (st.secrets.get("gemini", {}).get("api_key") if hasattr(st, "secrets") else None) or ""
GEMINI_MODEL = "gemini-2.5-flash"
# ----------------------------
# DOMAIN CONSTANTS (UI)
# ----------------------------
NC_STATUS_SELECTABLE = ["New", "Managed", "Close"]            # selezionabili in inserimento/modifica
NC_STATUS_ALL = NC_STATUS_SELECTABLE + ["Cancelled"]          # 'Cancelled' esiste ma NON selezionabile in inserimento
RESPONSIBILITY_OPTIONS = ["R&D", "Operation", "Supplier", "MKT", "Other", "Third party"]
MAKEBUY_OPTIONS = ["manufactured", "traded"]




# ============================================================
# CONFIG
# ============================================================

DB_PATH = ""  # (non usato: backend Google Sheets)
TREND_PATH = r"P:\QA\007 Validazione Prodotti\11 Non conformità\Trend _NC Quality_.xlsx"

# Email / SMTP - DA PERSONALIZZARE
SMTP_SERVER = os.getenv("SMTP_SERVER") or (st.secrets.get("mail", {}).get("smtp_server") if hasattr(st, "secrets") else None) or ""
SMTP_PORT = int(os.getenv("SMTP_PORT") or (st.secrets.get("mail", {}).get("smtp_port") if hasattr(st, "secrets") else 0) or 0)
SMTP_USER = os.getenv("SMTP_USER") or (st.secrets.get("mail", {}).get("username") if hasattr(st, "secrets") else None) or ""
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD") or (st.secrets.get("mail", {}).get("password") if hasattr(st, "secrets") else None) or ""


# ============================================================
# DB HELPERS
# ============================================================

def _script_url_data() -> str:
    """URL WebApp Apps Script per operazioni dati (list/create/update).

    Chiavi supportate in secrets.toml:
      [google] data_script_url = "..."   (consigliato)
      [google] script_url = "..."        (fallback)
      SCRIPT_URL = "..."                 (fallback)
    """
    if "google" in st.secrets and "data_script_url" in st.secrets["google"]:
        return str(st.secrets["google"]["data_script_url"]).split("?")[0]
    if "google" in st.secrets and "script_url" in st.secrets["google"]:
        return str(st.secrets["google"]["script_url"]).split("?")[0]
    if "SCRIPT_URL" in st.secrets:
        return str(st.secrets["SCRIPT_URL"]).split("?")[0]
    raise RuntimeError("URL dati non configurato in .streamlit/secrets.toml")


def _script_url_mail() -> str:
    """URL WebApp Apps Script per invio mail come utente Workspace (iframe/browser).

    Chiavi supportate in secrets.toml:
      [google] mail_script_url = "..."   (consigliato)
      [google] script_url = "..."        (fallback)
      (se non presente, usa l'URL dati)
    """
    if "google" in st.secrets and "mail_script_url" in st.secrets["google"]:
        return str(st.secrets["google"]["mail_script_url"]).split("?")[0]
    if "google" in st.secrets and "script_url" in st.secrets["google"]:
        return str(st.secrets["google"]["script_url"]).split("?")[0]
    # fallback: usa quello dati
    return _script_url_data().split("?")[0]

def send_mail_via_hidden_iframe(script_url: str, payload: dict, key: str = "sendmail"):
    payload_json = json.dumps(payload, ensure_ascii=False)

    payload_js = json.dumps(payload_json).replace("</", "<\\/")

    # Base64 UTF-8 (btoa non gestisce unicode -> uso encodeURIComponent trick)
    html = f"""
<div id="{key}_wrap"></div>
<iframe name="{key}_frame" style="display:none;"></iframe>

<form id="{key}_form" action="{script_url}" method="POST" target="{key}_frame">
  <input type="hidden" name="op" value="send_mail" />
  <input type="hidden" name="mode" value="iframe" />
  <input type="hidden" name="payload_b64" id="{key}_payload_b64" value="" />
</form>

<script>
(function() {{
  const payload = {payload_js}; // string JSON
  function toB64Unicode(str) {{
    return btoa(unescape(encodeURIComponent(str)));
  }}

  // UI feedback (solo dentro questo component)
  const wrap = document.getElementById("{key}_wrap");
  wrap.innerHTML = '<div style="padding:6px 0;">📨 Invio email in corso...</div>';

  // Listener per risposta Apps Script
  function onMsg(ev) {{
    try {{
      const d = ev.data;
      if (!d || (typeof d !== 'object')) return;
      if (d.ok) {{
        wrap.innerHTML = '<div style="padding:6px 0; color:green;">✅ Email inviata.</div>';
      }} else {{
        wrap.innerHTML = '<div style="padding:6px 0; color:red;">❌ Errore invio email: ' + (d.error || 'sconosciuto') + '</div>';
      }}
    }} catch(e) {{}}
    window.removeEventListener("message", onMsg);
  }}
  window.addEventListener("message", onMsg);

  // Set payload e submit
  document.getElementById("{key}_payload_b64").value = toB64Unicode(payload);
  document.getElementById("{key}_form").submit();
}})();
</script>
"""
    components.html(html, height=60)

def _json_safe(v):
    """Rende serializzabili in JSON date/datetime e NaN."""
    try:
        import pandas as _pd
        if _pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v



def _dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Coalesce duplicate column names safely (no .loc with duplicate labels).

    For each duplicated name, keeps ONE column with that name and fills its values
    by taking the first non-empty/non-null value from the duplicates (left->right).
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    cols_list = list(out.columns)

    # Fast exit
    if len(set(cols_list)) == len(cols_list):
        return out

    # Find duplicates by name
    seen = {}
    for i, name in enumerate(cols_list):
        seen.setdefault(name, []).append(i)

    # Only names with >1 occurrence
    dup_names = [n for n, idxs in seen.items() if len(idxs) > 1]

    for name in dup_names:
        idxs = seen[name]
        # Safety guard: if something is horribly wrong, don't explode memory
        if len(idxs) > 50:
            # keep first occurrence only
            keep = idxs[0]
            drop = idxs[1:]
            out = out.drop(out.columns[drop], axis=1)
            cols_list = list(out.columns)
            continue

        block = out.iloc[:, idxs]  # safe: integer positions

        # Start with first column
        coalesced = block.iloc[:, 0].copy()

        for j in range(1, block.shape[1]):
            s = block.iloc[:, j]
            # fill where coalesced is null or empty-string after strip
            m = coalesced.isna()
            try:
                m = m | (coalesced.astype(str).str.strip() == "")
            except Exception:
                pass
            coalesced = coalesced.where(~m, s)

        # Drop all duplicated columns, then insert merged one at the first position
        first_pos = idxs[0]
        out = out.drop(out.columns[idxs], axis=1)
        out.insert(first_pos, name, coalesced)

        # Refresh for next iteration
        cols_list = list(out.columns)
        seen = {}
        for i, nm in enumerate(cols_list):
            seen.setdefault(nm, []).append(i)

    return out


def _api_get(op: str, **params):
    url = _script_url_data()
    r = requests.get(url, params={"op": op, **params}, timeout=(5, 30), headers={"Accept":"application/json"})
    r.raise_for_status()
    try:
        j = r.json()
    except Exception:
        snippet = (r.text or "").strip()[:800]
        raise RuntimeError(
            "Risposta non-JSON dalla WebApp. Probabile redirect/login o deploy non corretto. "
            "Apri l'URL della WebApp nel browser con account clivet.it e verifica che l'app sia pubblicata "
            "come 'Web app' con accesso 'Only users in your domain'."
            f"Snippet risposta: {snippet}"
        )
    if not j.get("ok"):
        raise RuntimeError(j.get("error", "API error"))
    return j.get("data")


def _api_post(op: str, **body):
    url = _script_url_data()
    payload = {"op": op}
    payload.update(body)
    r = requests.post(url, json=payload, timeout=(5, 60), headers={"Accept":"application/json"})
    r.raise_for_status()
    try:
        j = r.json()
    except Exception:
        snippet = (r.text or "").strip()[:800]
        raise RuntimeError(
            "Risposta non-JSON dalla WebApp (POST). Probabile redirect/login o deploy non corretto. "
            "Apri l'URL della WebApp nel browser con account clivet.it e verifica che l'app sia pubblicata "
            "come 'Web app' con accesso 'Only users in your domain'."
            f"Snippet risposta: {snippet}"
        )
    if not j.get("ok"):
        raise RuntimeError(j.get("error", "API error"))
    return j.get("data")


@st.cache_data(show_spinner=False)
def load_nc_data() -> pd.DataFrame:
    """Carica tutte le NC dal Google Sheet (tab NC) via Apps Script."""
    data = _api_get("list_nc") or []
    df = pd.DataFrame(data)

    if df.empty:
        return df

    # --- Pulizia nomi colonna (export Oracle spesso contiene spazi/NBSP e duplicati) ---
    def _clean_col(c: str) -> str:
        c = str(c or "")
        c = c.replace("\u00a0", " ").replace("\xa0", " ")
        c = c.strip()
        c = re.sub(r"\s+", " ", c)
        return c

    df.columns = [_clean_col(c) for c in df.columns]
    df = _dedup_columns(df)

    # Se il backend restituisce header Oracle con piccole differenze (case/spazi), normalizza in modo robusto
    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    def _col(*names):
        for n in names:
            k = str(n).strip().upper()
            if k in cols_upper:
                return cols_upper[k]
        return None

    # alias robusti per i campi principali
    _ncnum_col = _col('NONCONFORMANCE_NUMBER', 'NC_NUMBER', 'NONCONFORMANCE NO', 'NONCONFORMANCE NUM')
    _ncstatus_col = _col('NONCONFORMANCE_STATUS')
    _opened_col = _col('DATE_OPENED')
    _closed_col = _col('DATE_CLOSED')

    # se esistono con nome diverso, rinomina prima del mapping
    pre_ren = {}
    if _ncnum_col and _ncnum_col != 'NONCONFORMANCE_NUMBER':
        pre_ren[_ncnum_col] = 'NONCONFORMANCE_NUMBER'
    if _ncstatus_col and _ncstatus_col != 'NONCONFORMANCE_STATUS':
        pre_ren[_ncstatus_col] = 'NONCONFORMANCE_STATUS'
    if _opened_col and _opened_col != 'DATE_OPENED':
        pre_ren[_opened_col] = 'DATE_OPENED'
    if _closed_col and _closed_col != 'DATE_CLOSED':
        pre_ren[_closed_col] = 'DATE_CLOSED'
    if pre_ren:
        df = df.rename(columns=pre_ren)

    # Normalizzazione header: se nel foglio arrivano colonne Oracle (es. NONCONFORMANCE_NUMBER, AC_...)
    # le riportiamo allo schema interno usato dall'app (snake_case).
    oracle_nc_map = {
        "NONCONFORMANCE_NUMBER": "nonconformance_number",
        "NONCONFORMANCE_STATUS": "nonconformance_status",
        "DATE_OPENED": "date_opened",
        "DATE_CLOSED": "date_closed",
        "OWNER": "owner",
        "RESPONSIBILITY": "responsibility",
        "PIATTAFORMA": "piattaforma",
        "PIATT.": "piattaforma",
        "SHORT_DESCRIPTION": "short_description",
        "DETAILED_DESCRIPTION": "detailed_description",
        "NC_PARENT_REF": "nc_parent_ref",
    }
    oracle_ac_map = {
        "AC_CORRECTIVE_ACTION_NUM": "ac_corrective_action_num",
        "AC_REQUEST_STATUS": "ac_request_status",
        "AC_REQUEST_PRIORITY": "ac_request_priority",
        "AC_DATE_OPENED": "ac_date_opened",
        "AC_DATE_REQUIRED": "ac_date_required",
        "AC_END_DATE": "ac_end_date",
        "AC_FOLLOW_UP_DATE": "ac_follow_up_date",
        "AC_OWNER": "ac_owner",
        "AC_EMAIL_ADDRESS": "ac_email_address",
        "AC_SHORT_DESCRIPTION": "ac_short_description",
        "AC_DETAILED_DESCRIPTION": "ac_detailed_description",
    }

    # rename solo le colonne presenti
    ren = {k: v for k, v in {**oracle_nc_map, **oracle_ac_map}.items() if k in df.columns}
    if ren:
        df = df.rename(columns=ren)

    # Se non c'è 'id' ma c'è nonconformance_number, usalo come id (alfanumerico)
    if "id" not in df.columns and "nonconformance_number" in df.columns:
        df["id"] = df["nonconformance_number"].astype(str).str.strip()

    # Coalesce eventuali colonne duplicate (tipico export Oracle con campi ripetuti)
    df = _dedup_columns(df)

    # conversione date (in oggetti date)
    for col in ["date_opened", "date_closed", "created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    # id come stringa (alfanumerico da Oracle)
    if "id" in df.columns:
        df["id"] = df["id"].astype(str).str.strip()

    # Se id è vuoto o costante (capita con export wide), forza id = nonconformance_number
    if "nonconformance_number" in df.columns:
        nn = df["nonconformance_number"].astype(str).str.strip()
        if "id" not in df.columns or df["id"].nunique(dropna=False) <= 1 or (df["id"].astype(str).str.strip() == '').all():
            df["id"] = nn

    return df


@st.cache_data(show_spinner=False)
def load_ac_data() -> pd.DataFrame:
    """Carica tutte le AC dal Google Sheet (tab AC) via Apps Script."""
    data = _api_get("list_ac") or []
    df = pd.DataFrame(data)

    if df.empty:
        # garantisce colonne minime per evitare KeyError nelle view
        return pd.DataFrame(columns=["id", "nc_id"])

    # --- Pulizia nomi colonna (spazi/NBSP ecc.) ---
    def _clean_col(c: str) -> str:
        c = str(c or "")
        c = c.replace("\u00a0", " ").replace("\xa0", " ")
        c = c.strip()
        c = re.sub(r"\s+", " ", c)
        return c

    df.columns = [_clean_col(c) for c in df.columns]
    df = _dedup_columns(df)

    # Normalizzazione header Oracle (se tab AC contiene colonne in MAIUSCOLO)
    oracle_ac_map = {
        "ID": "id",
        "NC_ID": "nc_id",
        "AC_CORRECTIVE_ACTION_NUM": "ac_corrective_action_num",
        "AC_REQUEST_STATUS": "ac_request_status",
        "AC_REQUEST_PRIORITY": "ac_request_priority",
        "AC_DATE_OPENED": "ac_date_opened",
        "AC_DATE_REQUIRED": "ac_date_required",
        "AC_END_DATE": "ac_end_date",
        "AC_FOLLOW_UP_DATE": "ac_follow_up_date",
        "AC_OWNER": "ac_owner",
        "AC_EMAIL_ADDRESS": "ac_email_address",
        "AC_SHORT_DESCRIPTION": "ac_short_description",
        "AC_DETAILED_DESCRIPTION": "ac_detailed_description",
    }
    ren = {k: v for k, v in oracle_ac_map.items() if k in df.columns}
    if ren:
        df = df.rename(columns=ren)

    for col in ["ac_date_opened", "ac_date_required", "ac_end_date", "ac_follow_up_date", "created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    if "id" in df.columns:
        df["id"] = df["id"].astype(str).str.strip()
    if "nc_id" in df.columns:
        df["nc_id"] = df["nc_id"].astype(str).str.strip()

    return df



def _split_combined_nc_ac(df_nc: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Export Oracle "wide": NC + campi AC nella stessa tabella (NC ripetuta su più righe).
    Se la tab AC è vuota, qui separiamo:
      - df_nc_unique: 1 riga per NC
      - df_ac: 1 riga per AC, collegata con nc_id = nonconformance_number (oppure id)
    NOTE: questa funzione è "memory safe" e non fa merge.
    """
    if df_nc is None or df_nc.empty:
        return df_nc, pd.DataFrame()

    # chiave NC robusta (Oracle)
    key_nc = "nonconformance_number" if "nonconformance_number" in df_nc.columns else ("id" if "id" in df_nc.columns else None)
    if key_nc is None:
        return df_nc.drop_duplicates().copy(), pd.DataFrame()

    # se esiste nonconformance_number, rendiamo id coerente (utile per tutto il resto dell'app)
    if key_nc == "nonconformance_number":
        df_nc["nonconformance_number"] = df_nc["nonconformance_number"].astype(str).str.strip()
        df_nc["id"] = df_nc["nonconformance_number"]

    ac_cols = [c for c in df_nc.columns if str(c).startswith("ac_")]
    if not ac_cols:
        return df_nc.drop_duplicates(subset=[key_nc]).copy(), pd.DataFrame()

    # NC unica
    df_nc_unique = df_nc.drop_duplicates(subset=[key_nc], keep="first").copy()

    # AC: prendiamo solo righe con ac_corrective_action_num valorizzato
    if "ac_corrective_action_num" not in df_nc.columns:
        return df_nc_unique, pd.DataFrame()

    ser_acnum = df_nc["ac_corrective_action_num"].astype(str).str.strip()
    ac_mask = ser_acnum.ne("") & df_nc["ac_corrective_action_num"].notna()

    # Costruisci df_ac prendendo la CHIAVE NC riga-per-riga (NON id fisso)
    df_ac = df_nc.loc[ac_mask, [key_nc] + ac_cols].copy()
    df_ac.rename(columns={key_nc: "nc_id"}, inplace=True)

    # id AC = numero azione correttiva (alfanumerico)
    df_ac["id"] = df_ac["ac_corrective_action_num"].astype(str).str.strip()
    df_ac["nc_id"] = df_ac["nc_id"].astype(str).str.strip()

    # normalizza date AC se presenti
    for col in ["ac_date_opened", "ac_date_required", "ac_end_date", "ac_follow_up_date", "created_at", "updated_at"]:
        if col in df_ac.columns:
            df_ac[col] = pd.to_datetime(df_ac[col], errors="coerce").dt.date

    # Dedup AC per id (se Oracle ripete righe identiche)
    df_ac = df_ac.drop_duplicates(subset=["id"], keep="first").copy()

    return df_nc_unique, df_ac


def clear_caches():
    load_nc_data.clear()
    load_ac_data.clear()
    load_platforms.clear()


def _serialize_dict(d: dict) -> dict:
    return {k: _json_safe(v) for k, v in (d or {}).items()}


def insert_nc_in_db(values: dict) -> str:
    """Crea una NC via Apps Script (tab NC)."""
    out = _api_post("create_nc", payload=_serialize_dict(values))
    clear_caches()
    return str(out.get("id", "")) if isinstance(out, dict) else ""



def update_nc_in_db(nc_id: str, values: dict):
    """Aggiorna una NC via Apps Script (tab NC)."""
    _api_post("update_nc", id=str(nc_id), patch=_serialize_dict(values))
    clear_caches()


def insert_ac_in_db(nc_id: str, values: dict):
    payload = {"nc_id": str(nc_id)}
    payload.update(values or {})
    _api_post("create_ac", payload=_serialize_dict(payload))
    clear_caches()


def update_ac_in_db(nc_id: str, ac_id: str, values: dict):
    """Aggiorna una AC *nel foglio NC wide* (NC e AC nello stesso tab).

    Richiede che la WebApp Apps Script supporti l'operazione POST:
      op="update_ac_wide"
      { nc_number: <NC>, ac_id: <AC>, patch: {...} }

    In assenza dell'endpoint, viene sollevato un errore esplicativo.
    """
    patch = _serialize_dict(values)
    # Aggiungo sempre anche l'identificativo AC, così se l'utente modifica campi chiave restano coerenti
    patch.setdefault("AC_CORRECTIVE_ACTION_NUM", str(ac_id))

    # Prova endpoint dedicato
    try:
        _api_post("update_ac_wide", nc_number=str(nc_id), ac_id=str(ac_id), patch=patch)
    except RuntimeError as e:
        msg = str(e)
        if "Unknown op" in msg or "update_ac_wide" in msg:
            raise RuntimeError(
                "La WebApp non supporta ancora l'operazione 'update_ac_wide'. "
                "Serve aggiungerla in Code.gs (DATA) per salvare le AC quando NC e AC sono nello stesso foglio."
            ) from e
        raise
    clear_caches()

@st.cache_data(show_spinner=False)
def load_platforms() -> list[str]:
    data = _api_get("list_platforms") or []
    return [str(x) for x in data if str(x).strip()]


def add_platform(name: str):
    name = (name or "").strip()
    if not name:
        return
    _api_post("add_platform", name=name)
    load_platforms.clear()


def get_next_nc_number(df_nc: pd.DataFrame) -> str:
    """Genera il prossimo numero NC nel formato NC-<n>-CVT leggendo il massimo esistente."""
    if df_nc is None or df_nc.empty or "nonconformance_number" not in df_nc.columns:
        return "NC-1-CVT"
    max_n = 0
    for val in df_nc["nonconformance_number"].astype(str).tolist():
        m = re.match(r"NC-(\d+)-CVT", val.strip())
        if m:
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except ValueError:
                pass
    return f"NC-{max_n + 1}-CVT"


def get_next_ac_number(df_ac: pd.DataFrame) -> int:
    """Restituisce il prossimo numero AC progressivo leggendo il massimo esistente."""
    if df_ac is None or df_ac.empty or "ac_corrective_action_num" not in df_ac.columns:
        return 1
    nums = []
    for v in df_ac["ac_corrective_action_num"].tolist():
        try:
            nums.append(int(str(v).strip()))
        except Exception:
            continue
    return (max(nums) + 1) if nums else 1





def get_status_options(df_nc: pd.DataFrame):
    """Lista di stati possibili per la NC (normalizzata).

    - In inserimento NC: solo New, Managed, Close
    - Cancelled esiste ma non è selezionabile in inserimento
    - Se nel DB ci sono valori storici (OPEN/CLOSED/...), li mappiamo per compatibilità.
    """
    base = list(NC_STATUS_ALL)

    if df_nc is None or df_nc.empty or "nonconformance_status" not in df_nc.columns:
        return base

    vals_raw = {
        str(v).strip()
        for v in df_nc["nonconformance_status"].dropna().tolist()
        if str(v).strip()
    }

    def norm(s: str) -> str:
        u = s.strip().upper()
        if u in ("OPEN", "NEW"):
            return "New"
        if u in ("CLOSED", "CLOSE", "CLOSED/VERIFIED", "CHIUSA"):
            return "Close"
        if u == "MANAGED":
            return "Managed"
        if u in ("CANCELLED", "CANCELED", "CANCELLATA"):
            return "Cancelled"
        if s in NC_STATUS_ALL:
            return s
        return s.title()

    # mantieni ordine: prima base, poi eventuali extra
    final = []
    for s in base:
        if s not in final:
            final.append(s)
    for s in [norm(v) for v in vals_raw]:
        if s and s not in final:
            final.append(s)
    return final






def _normalize_name_for_email(s: str) -> str:
    """Toglie accenti e caratteri strani per costruire l'email."""
    s = s.strip().lower()
    # rimuovo accenti tipo àèìòù
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # tengo solo lettere e -
    s = "".join(c for c in s if c.isalpha() or c in "-")
    return s


def suggest_email_from_name(name: str) -> str | None:
    """
    Costruisce email tipo f.cognome@clivet.it
    a partire da 'Nome Cognome'. Se non riconosce, torna None.
    """
    if not name:
        return None
    parts = [p for p in name.split() if p.strip()]
    if len(parts) < 2:
        return None
    first = _normalize_name_for_email(parts[0])
    last = _normalize_name_for_email(parts[-1])
    if not first or not last:
        return None
    return f"{first[0]}.{last}@clivet.it"

def call_gemini(prompt: str) -> str:
    """Chiama Gemini con il prompt dato e ritorna il testo generato."""
    if genai is None:
        raise RuntimeError(
            "Libreria google-generativeai non installata. "
            "Installa con: pip install google-generativeai"
        )
    if not GEMINI_API_KEY:
        raise RuntimeError(
            "Chiave GEMINI_API_KEY non configurata. "
            "Imposta la variabile d'ambiente o st.secrets."
        )

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)
    resp = model.generate_content(prompt)
    return resp.text or ""

def build_nc_ac_context(nc_row: pd.Series, df_ac_nc: pd.DataFrame) -> str:
    """Costruisce un testo con tutti i campi descrittivi di NC e AC."""
    lines = []

    lines.append(f"NC number: {nc_row.get('nonconformance_number')}")
    lines.append(f"Status: {nc_row.get('nonconformance_status')}")
    lines.append(f"Opened: {nc_row.get('date_opened')}")
    lines.append(f"Closed: {nc_row.get('date_closed')}")
    lines.append(f"Serie: {nc_row.get('serie')}")
    lines.append(f"Piattaforma: {nc_row.get('piattaforma')}")
    lines.append(f"Owner: {nc_row.get('owner')}")

    def add_if(label, col):
        val = nc_row.get(col)
        if val:
            lines.append(f"{label}: {val}")

    add_if("Short description", "short_description")
    add_if("Detailed description", "detailed_description")
    add_if("Problem description (DET_)", "det_problem_description")
    add_if("Cause (DET_CAUSE)", "det_cause")
    add_if("Close (DET_CLOSE)", "det_close")

    lines.append("\nCorrective Actions (AC):")
    if df_ac_nc is not None and not df_ac_nc.empty:
        for _, r in df_ac_nc.iterrows():
            lines.append(
                f"- AC {r.get('ac_corrective_action_num')} "
                f"(owner: {r.get('ac_owner')}, status: {r.get('ac_request_status')})"
            )
            if r.get("ac_short_description"):
                lines.append(f"  Short: {r.get('ac_short_description')}")
            if r.get("ac_detailed_description"):
                lines.append(f"  Detail: {r.get('ac_detailed_description')}")
            if r.get("ac_effective"):
                lines.append(f"  Effective: {r.get('ac_effective')}")
            if r.get("ac_evidence_verify"):
                lines.append(f"  Evidence: {r.get('ac_evidence_verify')}")
            lines.append(
                f"  Dates: opened={r.get('ac_date_opened')}, "
                f"required={r.get('ac_date_required')}, "
                f"closed={r.get('ac_end_date')}"
            )
    else:
        lines.append("  (no AC linked)")

    return "\n".join(lines)


def run_gemini_verifica_nc(nc_row: pd.Series, df_ac_nc: pd.DataFrame):
    """Esegue l'analisi di completezza della NC usando Gemini e mostra il risultato."""
    ctx = build_nc_ac_context(nc_row, df_ac_nc)

    prompt = f"""
Sei un esperto di gestione Non Conformità industriali.

Ti fornisco la descrizione di una NC e delle relative Azioni Correttive (AC).

Testo da analizzare:
--------------------
{ctx}
--------------------

Domande:

1. Questa gestione di NC è completa?
2. Sono state considerate e verificate le azioni di contenimento?
3. Sono state considerate e verificate le azioni correttive?
4. Sono state considerate le azioni da fare:
   - in magazzino,
   - presso i fornitori,
   - presso i clienti?
5. Ci sono retrofit, comunicazioni a clienti o fornitori o altri reparti che andrebbero considerati?

Rispondi in italiano, con un breve testo di suggerimento:
- se manca qualcosa, elenca in modo ordinato cosa consiglieresti di aggiungere o chiarire;
- se tutto è coperto in modo adeguato, scrivi che è un bel lavoro e indica brevemente i punti di forza.
La risposta deve essere concisa, chiara e per bullet points. Evita cose troppo lunghe, focalizzati sull'azione più che sulla dialettica. 
"""

    with st.spinner("Analisi NC con Gemini in corso..."):
        try:
            text = call_gemini(prompt)
            st.subheader("Suggerimenti di Gemini")
            st.write(text)
        except Exception as e:
            st.error(f"Impossibile contattare Gemini: {e}")

def run_gemini_8d_report(nc_row: pd.Series, df_ac_nc: pd.DataFrame, language_label: str):
    """Genera un 8D report (IT/EN) a partire da NC + AC usando Gemini."""
    ctx = build_nc_ac_context(nc_row, df_ac_nc)

    lang = "it" if language_label.lower().startswith("ital") else "en"

    if lang == "it":
        instr = """
Genera un report 8D completo in italiano, seguendo questa struttura:

- Intestazione con numero NC, testo "NONCONFORMANCE" e "8D Report"
- Riga con date di apertura e chiusura
- Campo "Source Problem" con una frase riassuntiva
- Campo "NC Owner"
- D1 - Team Members
- D2 - Problem Description
- D3 - Containment Actions (anche se non presenti esplicitamente, suggerisci o riassumi)
- D4 - Root Cause
- D5 - Corrective Actions (riassumendo e organizzando le AC collegate)
- D6 - Implementation and Verification of Corrective Actions
- D7 - Prevent Recurrence
- D8 - Closure

Usa come base i dati che ti passo, ma se mancano informazioni su qualche D, completa in modo ragionevole con suggerimenti o formulazioni tipo “N/A” o “Da definire”.
Scrivi in stile tecnico, ordinato e sintetico.
"""
    else:
        instr = """
Generate a complete 8D report in English, following this structure:

- Header with NC number, the word "NONCONFORMANCE" and "8D Report"
- Line with open and close dates
- "Source Problem" field with a short summary sentence
- "NC Owner"
- D1 - Team Members
- D2 - Problem Description
- D3 - Containment Actions
- D4 - Root Cause
- D5 - Corrective Actions (summarizing and organizing the linked ACs)
- D6 - Implementation and Verification of Corrective Actions
- D7 - Prevent Recurrence
- D8 - Closure

Use the data I provide as a base. If some D-sections are not explicitly covered, you may mark them as N/A or suggest what should be done.
Write in a technical, concise style.
"""

    prompt = f"""
You are helping to compile an 8D report for a Non Conformance.

Here is all the information available (NC + AC):

--------------------
{ctx}
--------------------

{instr}
"""

    with st.spinner("Generazione 8D report con Gemini..."):
        try:
            text = call_gemini(prompt)
            st.subheader(f"8D Report ({language_label})")
            st.text(text)
        except Exception as e:
            st.error(f"Impossibile generare l'8D con Gemini: {e}")


# ============================================================
# GESTIONE PIATTAFORME
# ============================================================





def send_email(to_addresses, subject, body):
    """Invia una mail di testo semplice tramite SMTP (Gmail o server aziendale)."""
    if isinstance(to_addresses, str):
        to_addresses = [to_addresses]
    if not to_addresses:
        return

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_addresses)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_addresses, msg.as_string())
    except Exception as e:
        try:
            st.error(f"Errore nell'invio della mail: {e}")
        except Exception:
            print("Errore nell'invio della mail:", e)


def get_nc_number_by_id(nc_id: str) -> str:
    """Ritorna il numero NC a partire dall'id interno."""
    try:
        data = _api_get("get_nc", id=str(nc_id))
    except Exception:
        data = None
    if isinstance(data, dict) and data.get("nonconformance_number"):
        return str(data["nonconformance_number"])
    return f"ID {nc_id}"


def get_nc_details(nc_id: str) -> dict:
    """Ritorna i dettagli della NC come dict (vuoto se non trovata)."""
    try:
        data = _api_get("get_nc", id=str(nc_id))
    except Exception:
        data = None
    return data if isinstance(data, dict) else {}


def get_ac_details_for_nc(nc_id: str) -> list[dict]:
    """Ritorna la lista delle AC collegate alla NC."""
    try:
        data = _api_get("list_ac_for_nc", nc_id=str(nc_id))
    except Exception:
        data = None
    return data if isinstance(data, list) else []


def _pick_first(d: dict, keys: list[str]) -> str:
    """Prende il primo valore non vuoto tra più chiavi."""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            return s
    return ""



def _operation_to_action(operation: str) -> str:
    """Mappa la descrizione operazione (UI) in action per l'endpoint send_mail."""
    op = (operation or "").lower()
    if "inser" in op or "crea" in op or "nuova" in op:
        return "create_nc"
    if "modif" in op or "aggiorn" in op or "update" in op:
        return "update_nc"
    # fallback
    return "update_nc"

def build_nc_email_message(nc_id: str | None, nc_number: str, operation: str) -> tuple[str, str]:
    """Costruisce subject e body dell'email includendo dettagli NC e AC."""
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")

    nc = get_nc_details(nc_id) if nc_id is not None else {}
    ac_list = get_ac_details_for_nc(nc_id) if nc_id is not None else []

    # Campi NC (fallback robusti)
    nc_subject = _pick_first(nc, ["subject", "oggetto", "incident_type", "nonconformance_source"]) or "(n/d)"
    nc_short = _pick_first(nc, ["short_description"]) or "(n/d)"
    nc_opened_by = _pick_first(nc, ["opened_by", "created_by", "opened_user", "created_user"]) or "(non indicato)"
    nc_owner = _pick_first(nc, ["owner"]) or "(non indicato)"
    nc_status = _pick_first(nc, ["nonconformance_status"]) or "(n/d)"

    # Subject compatto ma informativo
    short_part = f" - {nc_short}" if nc_short and nc_short != "(n/d)" else ""
    subject = f"[NC {nc_number}] {operation}{short_part}"

    lines: list[str] = []
    lines.append("Gentile collega,")
    lines.append("")
    lines.append(f"Aggiornamento su Non Conformità: {nc_number}")
    lines.append(f"Operazione: {operation}")
    lines.append(f"Data e ora: {now_str}")
    lines.append("")
    lines.append("DETTAGLI NC")
    lines.append(f"- Oggetto: {nc_subject}")
    lines.append(f"- Short description: {nc_short}")
    lines.append(f"- Aperta da: {nc_opened_by}")
    lines.append(f"- In carico a: {nc_owner}")
    lines.append(f"- Stato: {nc_status}")
    lines.append("")
    lines.append("AZIONI CORRETTIVE (AC)")

    if not ac_list:
        lines.append("- Nessuna AC presente.")
    else:
        for ac in ac_list:
            ac_num = _pick_first(ac, ["ac_corrective_action_num"]) or "(n/d)"
            ac_short = _pick_first(ac, ["ac_short_description"]) or "(n/d)"
            ac_owner = _pick_first(ac, ["ac_owner"]) or "(non indicato)"
            ac_status = _pick_first(ac, ["ac_request_status"]) or "(n/d)"
            ac_open = _pick_first(ac, ["ac_date_opened"]) or ""
            ac_req = _pick_first(ac, ["ac_date_required"]) or ""
            ac_end = _pick_first(ac, ["ac_end_date"]) or ""

            lines.append(f"- AC {ac_num}: {ac_short}")
            lines.append(f"  In carico a: {ac_owner} | Stato: {ac_status}")
            if ac_open:
                lines.append(f"  Data apertura: {ac_open}")
            if ac_req:
                lines.append(f"  Data richiesta chiusura: {ac_req}")
            if ac_end:
                lines.append(f"  Data chiusura effettiva: {ac_end}")

    lines.append("")
    lines.append("Puoi consultare i dettagli completi nell’applicativo NC Management.")
    lines.append("")
    lines.append("Questa è una comunicazione automatica: si prega di non rispondere.")
    body = "\n".join(lines)
    return subject, body

def get_emails_for_nc(nc_id: int) -> list[str]:
    """Ritorna tutti gli indirizzi email associati a una NC e alle sue AC."""
    emails = set()

    nc = get_nc_details(nc_id)
    if isinstance(nc, dict):
        v = (nc.get("email_address") or "").strip()
        if v:
            emails.add(v)

    ac_list = get_ac_details_for_nc(nc_id)
    for ac in ac_list or []:
        v = (ac.get("ac_email_address") or "").strip()
        if v:
            emails.add(v)

    return sorted(emails)


def trigger_email_prompt(nc_id: int, operation: str):
    """Imposta lo stato per mostrare il box di invio email."""
    st.session_state["email_nc_id"] = nc_id
    st.session_state["email_operation"] = operation
    st.session_state["show_email_prompt"] = True


def render_email_prompt():
    """Mostra (una sola volta) il box per inviare email di notifica agli owner."""
    if not st.session_state.get("show_email_prompt"):
        return

    nc_id = st.session_state.get("email_nc_id")
    operation = st.session_state.get("email_operation", "Aggiornamento NC")
    nc_number = get_nc_number_by_id(nc_id) if nc_id is not None else "n/d"
    emails = get_emails_for_nc(nc_id) if nc_id is not None else []

    # Chiavi univoche (evita StreamlitDuplicateElementKey)
    safe_op = re.sub(r"[^A-Za-z0-9]+", "_", str(operation))
    ctx = f"{nc_id}_{safe_op}"
    yes_key = f"email_send_yes_{ctx}"
    no_key = f"email_send_no_{ctx}"

    st.markdown("---")
    with st.container():
        st.subheader("Inviare le modifiche agli owner?")
        st.write(f"Vuoi inviare una mail agli owner della NC **{nc_number}**?")

        if emails:
            st.write("Gli indirizzi che verranno notificati sono:")
            for e in emails:
                st.write(f"- {e}")
        else:
            st.warning(
                "Nessun indirizzo email trovato nel database per questa NC. "
                "Compila i campi email nella NC o nelle AC."
            )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✉️ Sì, invia", key=yes_key):
                if emails:
                    # Invio via Apps Script dal browser (iframe invisibile):
                    # mittente reale = utente Workspace loggato
                    nc = get_nc_details(nc_id) if nc_id is not None else {}
                    ac_list = get_ac_details_for_nc(nc_id) if nc_id is not None else []

                    action = _operation_to_action(operation)
                    subject_val = _pick_first(nc, ["subject", "oggetto", "incident_type", "nonconformance_source"])

                    payload_key = f"email_payload_{ctx}"
                    st.session_state[payload_key] = {
                        "action": action,
                        "to": ", ".join(emails),
                        "nc": {
                            "nonconformance_number": nc.get("nonconformance_number", nc_number),
                            "subject": subject_val,
                            "short_description": nc.get("short_description", ""),
                            "opened_by": nc.get("created_by", "") or nc.get("owner", ""),
                            "responsibility": nc.get("responsibility", ""),
                            "nonconformance_status": nc.get("nonconformance_status", ""),
                            "piattaforma": nc.get("piattaforma", ""),
                        },
                        "ac_list": [
                            {
                                "ac_corrective_action_num": a.get("ac_corrective_action_num", ""),
                                "ac_short_description": a.get("ac_short_description", ""),
                                "ac_owner": a.get("ac_owner", ""),
                                "ac_request_status": a.get("ac_request_status", ""),
                            }
                            for a in (ac_list or [])
                        ],
                    }
                else:
                    st.warning("Nessun indirizzo email trovato: impossibile inviare.")

            # Se è stato premuto 'Sì, invia' mostro il widget di invio in iframe (senza redirect)
            payload_key = f"email_payload_{ctx}"
            if st.session_state.get(payload_key):
                send_mail_via_hidden_iframe(
                    _script_url_mail(),
                    st.session_state[payload_key],
                    key=f"mail_{ctx}",
                )
                if st.button("✅ Chiudi", key=f"email_close_{ctx}"):
                    st.session_state["show_email_prompt"] = False
                    st.session_state.pop(payload_key, None)
                    st.rerun()

        with col2:
            if st.button("❌ No, non inviare", key=no_key):
                st.session_state["show_email_prompt"] = False

# ============================================================
# UTIL GRAFICI E DATE
# ============================================================



def apply_nc_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Applica i filtri scelti in UI al DataFrame delle NC."""
    nc_number_filter = st.text_input("Numero NC contiene:", value="").strip()
    if nc_number_filter:
        df = df[
            df["nonconformance_number"]
            .astype(str)
            .str.contains(nc_number_filter, case=False, na=False)
        ]

    # Stati (lista fissa + eventuali stati storici)
    status_options = get_status_options(df)
    status_selected = st.multiselect("Stato NC", status_options, default=[])
    if status_selected and "nonconformance_status" in df.columns:
        # filtro su display_status per includere Managed da parent
        if "display_status" not in df.columns:
            df["display_status"] = df.apply(get_display_status, axis=1)
        df = df[df["display_status"].isin(status_selected)]

    # Responsabilità (lista fissa)
    responsibility_selected = st.multiselect("Responsabilità", RESPONSIBILITY_OPTIONS, default=[])
    if responsibility_selected and "responsibility" in df.columns:
        df = df[df["responsibility"].isin(responsibility_selected)]

    owner_list = sorted(df.get("owner", pd.Series(dtype=str)).dropna().unique().tolist())
    owner_selected = st.multiselect("Owner", owner_list, default=[])
    if owner_selected and "owner" in df.columns:
        df = df[df["owner"].isin(owner_selected)]

    return df



def style_ac_table(df_ac: pd.DataFrame) -> "pd.io.formats.style.Styler":
    """Stile base per la tabella AC."""
    return df_ac.style.set_properties(
        **{"white-space": "nowrap", "text-overflow": "ellipsis", "max-width": "300px"}
    )


def get_display_status(row: pd.Series) -> str:
    """Restituisce lo stato 'visuale' della NC (considerando eventuale parent)."""
    raw = str(row.get("nonconformance_status") or "").strip()
    parent_ref = str(row.get("nc_parent_ref") or "").strip()

    if parent_ref:
        return "Managed"

    u = raw.upper()
    if u in ("OPEN", "NEW"):
        return "New"
    if u in ("CLOSED", "CLOSE", "CLOSED/VERIFIED", "CHIUSA"):
        return "Close"
    if u in ("CANCELLED", "CANCELED", "CANCELLATA"):
        return "Cancelled"
    if u == "MANAGED":
        return "Managed"

    return raw if raw else "New"




def status_to_color(status: str) -> str:
    s = (status or "").strip().lower()
    if s in ("new", "open"):
        return "#cc0000"   # rosso
    if s == "managed":
        return "#ff8800"   # arancione
    if s in ("close", "closed", "cancelled", "canceled"):
        return "#008000"   # verde
    return "#555555"      # grigio



def render_status_html(status: str) -> str:
    color = status_to_color(status)
    return f"<span style='color:{color}; font-weight:bold'>{status}</span>"


def safe_date_for_input(val):
    if val is None or val == "":
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
        if isinstance(val, pd.Timestamp):
            return val.date()
    except Exception:
        pass

    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val

    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val).date()
        except ValueError:
            return None

    return None

def compute_trend_from_db(df_nc: pd.DataFrame, weeks_back: int = 52) -> pd.DataFrame:
    """Esempio di calcolo trend (semplificato) direttamente dal DB NC."""
    if df_nc.empty:
        return pd.DataFrame()

    df = df_nc.copy()
    df["date_opened"] = pd.to_datetime(df["date_opened"], errors="coerce")
    df = df.dropna(subset=["date_opened"])

    df["year_week"] = df["date_opened"].dt.strftime("%Y-%U")

    trend = (
        df.groupby("year_week")
        .agg(
            started=("id", "count"),
        )
        .reset_index()
    )
    trend.rename(columns={"started": "Started last 8 Week"}, inplace=True)
    trend["data_pubblicazione"] = pd.to_datetime(
        trend["year_week"] + "-1", format="%Y-%U-%w", errors="coerce"
    )
    trend = trend.sort_values("data_pubblicazione")
    return trend


@st.cache_data(show_spinner=False)
def load_trend_data() -> pd.DataFrame:
    """Carica il file Excel Trend se disponibile, altrimenti calcola da DB."""
    try:
        df = pd.read_excel(TREND_PATH)
        if "data_pubblicazione" in df.columns:
            df["data_pubblicazione"] = pd.to_datetime(
                df["data_pubblicazione"], errors="coerce"
            )
        return df
    except Exception:
        df_nc = load_nc_data()
        return compute_trend_from_db(df_nc)


# ============================================================
# VISTE
# ============================================================

def view_lista(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header("📋 Lista NC / AC")

    tipo = st.radio("Visualizza:", ("Non Conformità", "Azioni Correttive"), horizontal=True)

    if tipo == "Non Conformità":
        if df_nc.empty:
            st.warning("Nessuna NC presente nel database.")
            return

        df_filt = apply_nc_filters(df_nc.copy())

        base_columns = [
            "nonconformance_number",
            "nonconformance_status",
            "date_opened",
            "date_closed",
            "responsibility",
            "owner",
            "email_address",
            "nonconformance_source",
            "incident_type",
            "serie",
            "piattaforma",
            "short_description",
        ]
        cols = [c for c in base_columns if c in df_filt.columns]

        st.dataframe(df_filt[cols], use_container_width=True)
    else:
        if df_ac.empty:
            st.warning("Nessuna AC presente nel database.")
            return

        # Evita merge: se df_nc contiene duplicati (export Oracle wide), un merge può esplodere (cartesian).
        # Usiamo una mappa 1:1 id->numero NC.
        df = df_ac.copy()
        if "nc_id" in df.columns and "id" in df_nc.columns and "nonconformance_number" in df_nc.columns:
            nc_map = (
                df_nc.drop_duplicates(subset=["id"])
                .set_index("id")["nonconformance_number"]
                .astype(str)
            )
            df["nonconformance_number"] = df["nc_id"].astype(str).map(nc_map)
        else:
            # fallback (non dovrebbe servire)
            if "nonconformance_number" not in df.columns:
                df["nonconformance_number"] = ""

        ac_columns = [
            "nonconformance_number",
            "ac_corrective_action_num",
            "ac_request_status",
            "ac_request_priority",
            "ac_date_opened",
            "ac_date_required",
            "ac_end_date",
            "ac_owner",
            "ac_email_address",
            "ac_short_description",
        ]
        cols = [c for c in ac_columns if c in df.columns]
        st.dataframe(df[cols], use_container_width=True)

def view_gestione_piattaforme():
    st.header("🧩 Gestione piattaforme")

    platforms = load_platforms()
    if platforms:
        st.subheader("Piattaforme disponibili")
        st.dataframe(
            pd.DataFrame({"Piattaforma": platforms}),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Nessuna piattaforma ancora definita.")

    st.markdown("---")
    st.subheader("Aggiungi nuova piattaforma")

    new_name = st.text_input("Nuova piattaforma")
    if st.button("➕ Aggiungi piattaforma"):
        if not new_name.strip():
            st.error("Inserisci un nome per la piattaforma.")
        else:
            add_platform(new_name)
            st.success(f"Piattaforma '{new_name}' aggiunta.")



def view_consulta_nc(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    df_list = pd.DataFrame()
    st.header("🔍 Consulta NC")

    if df_nc.empty:
        st.warning("Nessuna NC presente nel database.")
        return

    # inizializza stato di navigazione
    if "consulta_mode" not in st.session_state:
        st.session_state["consulta_mode"] = "list"
        st.session_state["consulta_nc_id"] = None

    mode = st.session_state["consulta_mode"]
    selected_id = st.session_state["consulta_nc_id"]

    # ========= VISTA DI DETTAGLIO =========
    if mode == "detail" and selected_id is not None:
        row = df_nc[df_nc["id"] == selected_id]
        if row.empty:
            st.error("NC non trovata.")
            st.session_state["consulta_mode"] = "list"
            st.session_state["consulta_nc_id"] = None
            return
        row = row.iloc[0]
        display_status = get_display_status(row)
        parent_ref = str(row.get("nc_parent_ref") or "").strip()
        nc_id = str(row["id"]).strip()
        nc_number = row["nonconformance_number"]

        if st.button("⬅ Torna all’elenco"):
            st.session_state["consulta_mode"] = "list"
            st.session_state["consulta_nc_id"] = None
            st.rerun()

        st.subheader(f"NC {nc_number}")

        # dati principali NC
        # Stato con colore
        st.markdown(
            f"**Stato:** {render_status_html(display_status)}",
            unsafe_allow_html=True,
        )

        if parent_ref:
            st.write(f"**Parent NC:** {parent_ref} (questa NC è gestita come figlia)")

        info_cols = [
            ("Numero NC", "nonconformance_number"),
            ("Data apertura", "date_opened"),
            ("Data chiusura", "date_closed"),
            ("Serie", "serie"),
            ("Piattaforma", "piattaforma"),
            ("Priorità", "nonconform_priority"),
            ("Responsabilità", "responsibility"),
            ("Owner", "owner"),
            ("Email owner", "email_address"),
            ("Fonte", "nonconformance_source"),
            ("Tipo incidente", "incident_type"),
        ]
        for label, col in info_cols:
            if col in row.index:
                st.write(f"**{label}:** {row[col]}")

        st.markdown("### Descrizioni NC")
        for label, col in [
            ("Short description", "short_description"),
            ("Detailed description", "detailed_description"),
            ("Problem description (DET_)", "det_problem_description"),
            ("Cause (DET_CAUSE)", "det_cause"),
            ("Chiusura (DET_CLOSE)", "det_close"),
        ]:
            if col in row.index and row[col]:
                st.markdown(f"**{label}:**")
                st.write(row[col])

        st.markdown("---")
        st.subheader("Azioni Correttive collegate")

        df_ac_nc = df_ac[df_ac.get("nc_id", pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
        if df_ac_nc.empty:
            st.info("Nessuna AC collegata a questa NC.")
        else:
            ac_columns = [
                "ac_corrective_action_num",
                "ac_request_status",
                "ac_request_priority",
                "ac_date_opened",
                "ac_date_required",
                "ac_end_date",
                "ac_owner",
                "ac_email_address",
                "ac_short_description",
            ]
            cols = [c for c in ac_columns if c in df_ac_nc.columns]
            st.dataframe(df_ac_nc[cols], use_container_width=True)

        # --- Bottoni Gemini: Verifica NC + Genera 8D ---
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🤖 Verifica NC con Gemini"):
                run_gemini_verifica_nc(row, df_ac_nc)
        with col2:
            lang = st.selectbox("Lingua 8D", ["Italiano", "English"])
            if st.button("📄 Genera 8D report"):
                run_gemini_8d_report(row, df_ac_nc, lang)

        return

    # ========= VISTA ELENCO =========
    st.subheader("Elenco NC")

    # Base lista (sempre definita)
    df_list = df_nc.copy()

    # stato visualizzato (NEW/OPEN/MANAGED/CLOSED...)
    if not df_list.empty:
        df_list["display_status"] = df_list.apply(get_display_status, axis=1)

        counts = df_list["display_status"].value_counts()
        recap_parts = [f"{cnt} in stato {stato}" for stato, cnt in counts.items()]
        st.caption(" | ".join(recap_parts))
    else:
        st.caption("Nessuna NC selezionata o disponibile.")
    df_list = df_list.sort_values(
        ["date_opened", "nonconformance_number"], ascending=[False, True]
    )

    # filtro veloce
    filtro = st.text_input("Filtro per numero / descrizione / owner:", value="").strip()
    if filtro:
        mask = (
            df_list["nonconformance_number"].astype(str).str.contains(filtro, case=False, na=False)
            | df_list.get("short_description", pd.Series("", index=df_list.index))
                .astype(str).str.contains(filtro, case=False, na=False)
            | df_list.get("owner", pd.Series("", index=df_list.index))
                .astype(str).str.contains(filtro, case=False, na=False)
        )
        df_list = df_list[mask]

    if df_list.empty:
        st.info("Nessuna NC corrisponde ai filtri.")
        return

    # intestazione "tabella"
    header_cols = st.columns([1.4, 1, 1, 1, 1, 3, 1])
    header_cols[0].markdown("**Numero NC**")
    header_cols[1].markdown("**Stato**")
    header_cols[2].markdown("**Data apertura**")
    header_cols[3].markdown("**Serie**")
    header_cols[4].markdown("**Piattaforma**")
    header_cols[5].markdown("**Short description**")
    header_cols[6].markdown("**Owner**")

    st.markdown("---")

    for i, (_, r) in enumerate(df_list.iterrows()):
        c1, c2, c3, c4, c5, c6, c7 = st.columns([1.4, 1, 1, 1, 1, 3, 1])

        c1.write(r.get("nonconformance_number", ""))
        c2.markdown(
            render_status_html(r.get("display_status", "")),
            unsafe_allow_html=True,
        )
        c3.write(r.get("date_opened", ""))
        c4.write(r.get("serie", ""))
        c5.write(r.get("piattaforma", ""))
        c6.write(r.get("short_description", ""))
        c7.write(r.get("owner", ""))

        if c7.button("Dettaglio", key=f"det_nc_{i}_{r.get('id','')}"):
            st.session_state["consulta_mode"] = "detail"
            st.session_state["consulta_nc_id"] = str(r["id"]).strip()
            st.rerun()

        # linea orizzontale tra le NC
        st.markdown("<hr>", unsafe_allow_html=True)


def view_modifica_nc(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header("✏️ Modifica NC / AC")

    if df_nc.empty:
        st.warning("Nessuna NC presente nel database.")
        return

    status_options = get_status_options(df_nc)

    nc_numbers = sorted(df_nc["nonconformance_number"].dropna().unique().tolist())
    selected_nc = st.selectbox("Seleziona NC", nc_numbers)

    row = df_nc[df_nc["nonconformance_number"] == selected_nc]
    if row.empty:
        st.error("NC non trovata.")
        return
    row = row.iloc[0]
    nc_id = str(row["id"]).strip()

    st.subheader("Dati NC")

    with st.form(key="form_modifica_nc"):
        serie = st.text_input("Serie", value=row.get("serie") or "")
        platforms = load_platforms()
        if platforms:
            piattaforma = st.selectbox("Piattaforma *", platforms)
        else:
            piattaforma = st.text_input(
                "Piattaforma * (nessuna piattaforma definita a sistema)"
            )

        short_description = st.text_input(
            "Short description", value=row.get("short_description") or ""
        )

        current_mb = (row.get("item") or row.get("ITEM") or "").strip().lower()
        if current_mb not in MAKEBUY_OPTIONS:
            current_mb = ""
        mb_index = MAKEBUY_OPTIONS.index(current_mb) if current_mb in MAKEBUY_OPTIONS else 0
        makebuy = st.selectbox("Traded / Manufactured (colonna ITEM)", options=MAKEBUY_OPTIONS, index=mb_index)

        current_status = get_display_status(row)
        if current_status not in status_options:
            status_options = [current_status] + status_options
        status = st.selectbox(
            "Stato NC",
            options=status_options,
            index=status_options.index("New") if "New" in status_options else 0,
        )

        nonconform_priority = st.text_input(
            "Priorità NC", value=row.get("nonconform_priority") or ""
        )

        current_resp = (row.get("responsibility") or "").strip()
        resp_opts = list(RESPONSIBILITY_OPTIONS)
        if current_resp and current_resp not in resp_opts:
            resp_opts = [current_resp] + resp_opts
        responsibility = st.selectbox(
            "Responsabilità",
            options=resp_opts,
            index=resp_opts.index(current_resp) if current_resp in resp_opts else 0,
        )

        owner = st.text_input("Owner NC", value=row.get("owner") or "")
        email_address = st.text_input(
            "Email owner NC", value=row.get("email_address") or ""
        )
        nonconformance_source = st.text_input(
            "Fonte NC (source)", value=row.get("nonconformance_source") or ""
        )
        incident_type = st.text_input(
            "Incident type", value=row.get("incident_type") or ""
        )

        col1, col2 = st.columns(2)
        with col1:
            date_opened_val = safe_date_for_input(row.get("date_opened"))
            date_opened = st.date_input(
                "Data apertura", value=date_opened_val or date.today()
            )
        with col2:
            date_closed_val = safe_date_for_input(row.get("date_closed"))

            has_date_closed = st.checkbox(
                "Data chiusura impostata",
                value=(date_closed_val is not None),
                key=f"has_date_closed_{nc_id}",
            )

            if has_date_closed:
                date_closed = st.date_input(
                    "Data chiusura",
                    value=(date_closed_val or date.today()),
                    key=f"date_closed_{nc_id}",
            )
            else:
                date_closed = None
                st.caption("Data chiusura: —")

        detailed_description = st.text_area(
            "Descrizione dettagliata", value=row.get("detailed_description") or ""
        )
        det_problem_description = st.text_area(
            "Problem description (DET_)", value=row.get("det_problem_description") or ""
        )
        det_cause = st.text_area("Cause (DET_CAUSE)", value=row.get("det_cause") or "")
        det_close = st.text_area("Chiusura (DET_CLOSE)", value=row.get("det_close") or "")

        submitted_nc = st.form_submit_button("💾 Salva modifiche NC")

        if submitted_nc:
            errors = []
            if not serie.strip():
                errors.append("SERIE è obbligatoria.")
            if not piattaforma.strip():
                errors.append("PIATTAFORMA è obbligatoria.")
            if not short_description.strip():
                errors.append("SHORT_DESCRIPTION è obbligatoria.")

            else:
                owner_clean = owner.strip()
                email_clean = email_address.strip()
                if not email_clean and owner_clean:
                    suggestion = suggest_email_from_name(owner_clean)
                    if suggestion:
                        email_clean = suggestion

                vals = {
                    "serie": serie.strip(),
                    "piattaforma": piattaforma.strip(),
                    "item": makebuy,
                    "ITEM": makebuy,
                    "short_description": short_description.strip(),
                    "nonconformance_status": status.strip(),
                    "nonconform_priority": nonconform_priority.strip() or None,
                    "responsibility": responsibility.strip() or None,
                    "owner": owner_clean or None,
                    "email_address": email_clean or None,
                    "nonconformance_source": nonconformance_source.strip() or None,
                    "incident_type": incident_type.strip() or None,
                    "date_opened": date_opened.isoformat() if date_opened else None,
                    "date_closed": date_closed.isoformat() if date_closed else None,
                    "detailed_description": detailed_description or None,
                    "det_problem_description": det_problem_description or None,
                    "det_cause": det_cause or None,
                    "det_close": det_close or None,
                }
                update_nc_in_db(nc_id, vals)
                st.success("NC aggiornata con successo.")
                trigger_email_prompt(nc_id, "Modifica dati NC")

    # ---------- MODIFICA / AGGIUNGI AC ----------
    st.markdown("---")
    st.subheader("Azioni Correttive collegate")

    df_ac_nc = df_ac[df_ac.get("nc_id", pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()

    ac_ids = df_ac_nc["id"].tolist()
    ac_labels = [
        f"{row['ac_corrective_action_num']} - {row.get('ac_short_description','')}"
        for _, row in df_ac_nc.iterrows()
    ] if not df_ac_nc.empty else []

    selected_ac_label = None
    if ac_labels:
        selected_ac_label = st.selectbox("Seleziona AC da modificare", ac_labels)

    if selected_ac_label:
        idx = ac_labels.index(selected_ac_label)
        ac_row = df_ac_nc.iloc[idx]
        ac_id = str(ac_row["id"]).strip()

        st.write(f"**AC selezionata:** {ac_row['ac_corrective_action_num']}")

        with st.form(key="form_modifica_ac"):
            ac_request_status = st.text_input(
                "Stato AC", value=ac_row.get("ac_request_status") or ""
            )
            ac_request_priority = st.text_input(
                "Priorità AC", value=ac_row.get("ac_request_priority") or ""
            )
            ac_owner = st.text_input("Owner AC", value=ac_row.get("ac_owner") or "")
            ac_email_address = st.text_input(
                "Email owner AC", value=ac_row.get("ac_email_address") or ""
            )
            ac_short_description = st.text_input(
                "Short description AC", value=ac_row.get("ac_short_description") or ""
            )
            ac_detailed_description = st.text_area(
                "Descrizione dettagliata AC",
                value=ac_row.get("ac_detailed_description") or "",
            )
            ac_effective = st.text_input(
                "Effettiva (AC_EFFECTIVE)", value=ac_row.get("ac_effective") or ""
            )
            ac_evidence_verify = st.text_area(
                "Evidenze verifica (AC_EVIDENCE_VERIFY)",
                value=ac_row.get("ac_evidence_verify") or "",
            )

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                ac_date_opened = st.date_input(
                    "Data apertura AC",
                    value=safe_date_for_input(ac_row.get("ac_date_opened")),
                )
            with col2:
                ac_date_required = st.date_input(
                    "Data richiesta chiusura",
                    value=safe_date_for_input(ac_row.get("ac_date_required")),
                )
            with col3:
                ac_end_date = st.date_input(
                    "Data chiusura effettiva",
                    value=safe_date_for_input(ac_row.get("ac_end_date")),
                )
            with col4:
                ac_follow_up_date = st.date_input(
                    "Data follow-up",
                    value=safe_date_for_input(ac_row.get("ac_follow_up_date")),
                )

            submitted_ac = st.form_submit_button("💾 Salva modifiche AC")

            if submitted_ac:
                owner_clean = ac_owner.strip()
                email_clean = ac_email_address.strip()
                if not email_clean and owner_clean:
                    suggestion = suggest_email_from_name(owner_clean)
                    if suggestion:
                        email_clean = suggestion

                vals_ac = {
                    "ac_request_status": ac_request_status or None,
                    "ac_request_priority": ac_request_priority or None,
                    "ac_owner": ac_owner or None,
                    "ac_email_address": ac_email_address or None,
                    "ac_short_description": ac_short_description or None,
                    "ac_detailed_description": ac_detailed_description or None,
                    "ac_effective": ac_effective or None,
                    "ac_evidence_verify": ac_evidence_verify or None,
                    "ac_date_opened": ac_date_opened.isoformat()
                    if ac_date_opened
                    else None,
                    "ac_date_required": ac_date_required.isoformat()
                    if ac_date_required
                    else None,
                    "ac_end_date": ac_end_date.isoformat() if ac_end_date else None,
                    "ac_follow_up_date": ac_follow_up_date.isoformat()
                    if ac_follow_up_date
                    else None,
                }
                update_ac_in_db(nc_id, ac_id, vals_ac)
                st.success("AC aggiornata con successo.")
                trigger_email_prompt(nc_id, f"Modifica AC {ac_row['ac_corrective_action_num']}")

    st.markdown("---")
    st.subheader("➕ Aggiungi nuova AC per questa NC")

    df_ac_all = load_ac_data()
    next_ac_number = get_next_ac_number(df_ac_all)
    st.info(f"Nuovo numero AC proposto: **{next_ac_number}**")

    with st.form(key="form_inserisci_ac"):
        ac_short_description_new = st.text_input("Short description AC *")
        ac_owner_new = st.text_input("Owner AC")
        ac_email_address_new = st.text_input("Email owner AC")
        ac_request_status_new = st.text_input("Stato AC", value="OPEN")
        ac_request_priority_new = st.text_input("Priorità AC")
        ac_detailed_description_new = st.text_area("Descrizione dettagliata AC")
        ac_effective_new = st.text_input("Effettiva (AC_EFFECTIVE)")
        ac_evidence_verify_new = st.text_area("Evidenze verifica (AC_EVIDENCE_VERIFY)")
        col1, col2 = st.columns(2)
        today = date.today()
        with col1:
            ac_date_opened_new = st.date_input("Data apertura AC", value=today)
        with col2:
            ac_date_required_new = st.date_input(
                "Data richiesta chiusura", value=today
            )

        submitted_new_ac = st.form_submit_button("💾 Crea nuova AC")

        if submitted_new_ac:
            errors = []
            if not ac_short_description_new.strip():
                errors.append("Short description AC è obbligatoria.")
            if errors:
                for e in errors:
                    st.error(e)
            else:
                owner_clean = ac_owner_new.strip()
                email_clean = ac_email_address_new.strip()
                if not email_clean and owner_clean:
                    suggestion = suggest_email_from_name(owner_clean)
                    if suggestion:
                        email_clean = suggestion

                vals_new_ac = {
                    "ac_corrective_action_num": next_ac_number,
                    "ac_owner": ac_owner_new.strip() or None,
                    "ac_email_address": ac_email_address_new.strip() or None,
                    "ac_request_status": ac_request_status_new.strip() or None,
                    "ac_request_priority": ac_request_priority_new.strip() or None,
                    "ac_detailed_description": ac_detailed_description_new or None,
                    "ac_effective": ac_effective_new or None,
                    "ac_evidence_verify": ac_evidence_verify_new or None,
                    "ac_date_opened": ac_date_opened_new.isoformat(),
                    "ac_date_required": ac_date_required_new.isoformat()
                    if ac_date_required_new
                    else None,
                }
                insert_ac_in_db(nc_id, vals_new_ac)
                st.success(
                    f"AC {next_ac_number} creata con successo per la NC {selected_nc}."
                )
                trigger_email_prompt(nc_id, f"Nuova AC {next_ac_number} creata")


def view_inserisci_nc(df_nc: pd.DataFrame):
    st.header("➕ Inserisci nuova NC")

        # In inserimento: lista fissa (Cancelled non selezionabile)
    status_options = list(NC_STATUS_SELECTABLE)

    # Proposta numero NC (calcolata lato app dal massimo esistente)
    new_nc_number = get_next_nc_number(df_nc)

    st.info(f"Il nuovo numero NC proposto è: **{new_nc_number}**")

    today = date.today()

    with st.form(key="form_inserisci_nc"):
        st.text_input("Numero NC", value=new_nc_number, disabled=True)

        # --- QUI IL BLOCCO CORRETTO PER LA PIATTAFORMA ---
        serie = st.text_input("Serie *")

        platforms = load_platforms()
        current_plat = ""  # nuova NC: nessun valore preesistente

        if platforms:
            piattaforma = st.selectbox(
                "Piattaforma",
                options=platforms,
                index=0,
            )
        else:
            piattaforma = st.text_input("Piattaforma", value=current_plat)
        # --- FINE BLOCCO PIATTAFORMA ---

        short_description = st.text_input("Short description *")

        makebuy = st.selectbox("Traded / Manufactured (colonna ITEM)", options=MAKEBUY_OPTIONS, index=0)
        status = st.selectbox(
            "Stato NC",
            options=status_options,
            index=status_options.index("New") if "New" in status_options else 0,
        )

        nonconform_priority = st.text_input("Priorità NC")
        responsibility = st.selectbox("Responsabilità", options=RESPONSIBILITY_OPTIONS, index=0)
        owner = st.text_input("Owner NC")
        email_address = st.text_input("Email owner NC")
        nonconformance_source = st.text_input("Fonte NC (source)")
        incident_type = st.text_input("Incident type")

        st.date_input("Data apertura (auto)", value=today, disabled=True)

        detailed_description = st.text_area("Descrizione dettagliata")
        det_problem_description = st.text_area("Problem description (DET_)")
        det_cause = st.text_area("Cause (DET_CAUSE)")
        det_close = st.text_area("Chiusura (DET_CLOSE)")

        submitted = st.form_submit_button("💾 Crea NC")

        if submitted:
            errors = []
            if not serie.strip():
                errors.append("SERIE è obbligatoria.")
            if not piattaforma.strip():
                errors.append("PIATTAFORMA è obbligatoria.")
            if not short_description.strip():
                errors.append("SHORT_DESCRIPTION è obbligatoria.")

            if errors:
                for e in errors:
                    st.error(e)
            else:
                owner_clean = owner.strip()
                email_clean = email_address.strip()
                if not email_clean and owner_clean:
                    suggestion = suggest_email_from_name(owner_clean)
                    if suggestion:
                        email_clean = suggestion

                vals = {
                    "nonconformance_number": new_nc_number,
                    "date_opened": today.isoformat(),
                    "nonconformance_status": status.strip(),
                    "serie": serie.strip(),
                    "piattaforma": piattaforma.strip(),
                    "item": makebuy,
                    "ITEM": makebuy,
                    "short_description": short_description.strip(),
                    "nonconform_priority": nonconform_priority.strip() or None,
                    "responsibility": responsibility.strip() or None,
                    "owner": owner_clean or None,
                    "email_address": email_clean or None,
                    "nonconformance_source": nonconformance_source.strip() or None,
                    "incident_type": incident_type.strip() or None,
                    "detailed_description": detailed_description or None,
                    "det_problem_description": det_problem_description or None,
                    "det_cause": det_cause or None,
                    "det_close": det_close or None,
                }
                nc_id = insert_nc_in_db(vals)
                st.success(f"NC {new_nc_number} creata con successo.")
                trigger_email_prompt(nc_id, "Nuova NC creata")



def view_trend_nc_quality_db(df_nc: pd.DataFrame):
    st.header("📈 Trend NC (ultime 8 settimane)")

    if df_nc is None or df_nc.empty:
        st.warning("Nessuna NC disponibile.")
        return

    df = df_nc.copy()

    # date
    df["date_opened_dt"] = pd.to_datetime(df.get("date_opened"), errors="coerce")
    df["date_closed_dt"] = pd.to_datetime(df.get("date_closed"), errors="coerce")

    # make/buy in colonna ITEM (valori: manufactured / traded)
    mb = df.get("item", pd.Series("", index=df.index)).astype(str)
    if "ITEM" in df.columns:
        mb = mb.where(mb.str.strip().ne(""), df["ITEM"].astype(str))
    df["makebuy"] = mb.str.strip().str.lower()

    today = pd.Timestamp.today().normalize()
    # ultime 8 settimane (inclusa quella corrente)
    week_starts = pd.date_range(end=today, periods=8, freq="W-MON")  # lunedì
    weeks = pd.DataFrame({"week_start": week_starts})
    weeks["week_end"] = weeks["week_start"] + pd.Timedelta(days=6)

    def count_started(ws, we):
        m = (df["date_opened_dt"] >= ws) & (df["date_opened_dt"] <= we)
        return int(m.sum())

    def count_closed(ws, we):
        m = (df["date_closed_dt"] >= ws) & (df["date_closed_dt"] <= we)
        return int(m.sum())

    def count_still_open(at_end, manufactured_only=False):
        opened = df["date_opened_dt"].notna() & (df["date_opened_dt"] <= at_end)
        not_closed = df["date_closed_dt"].isna() | (df["date_closed_dt"] > at_end)
        m = opened & not_closed
        if manufactured_only:
            m = m & (df["makebuy"] == "manufactured")
        return int(m.sum())

    started = []
    closed = []
    still_open = []
    still_open_make = []

    for _, w in weeks.iterrows():
        ws = w["week_start"]
        we = w["week_end"]
        started.append(count_started(ws, we))
        closed.append(count_closed(ws, we))
        still_open.append(count_still_open(we, manufactured_only=False))
        still_open_make.append(count_still_open(we, manufactured_only=True))

    out = weeks.copy()
    out["Started last 8 weeks"] = started
    out["Closed last 8 weeks"] = closed
    out["Still open"] = still_open
    out["Still open Make"] = still_open_make

    # Melt per Altair
    melt = out.melt(
        id_vars=["week_start"],
        value_vars=["Started last 8 weeks", "Closed last 8 weeks", "Still open", "Still open Make"],
        var_name="metric",
        value_name="value",
    )

    # colori richiesti per started/closed; gli altri coerenti
    color_scale = alt.Scale(
        domain=["Started last 8 weeks", "Closed last 8 weeks", "Still open", "Still open Make"],
        range=["#ff8800", "#008000", "#555555", "#2f6fed"],
    )

    chart = (
        alt.Chart(melt)
        .mark_line(point=True)
        .encode(
            x=alt.X("week_start:T", title="Settimana (inizio)"),
            y=alt.Y("value:Q", title="N° NC"),
            color=alt.Color("metric:N", scale=color_scale, title="Metrica"),
            tooltip=["week_start:T", "metric:N", "value:Q"],
        )
        .properties(height=420)
    )

    st.altair_chart(chart, use_container_width=True)

    with st.expander("Dettaglio tabella"):
        st.dataframe(out[["week_start", "Started last 8 weeks", "Closed last 8 weeks", "Still open", "Still open Make"]], width="stretch", hide_index=True)





# ============================================================
# MAIN
# ============================================================

def main():
    st.set_page_config(
        page_title="Gestione Non Conformità",
        layout="wide",
    )

    st.sidebar.title("Menu")
    scelta = st.sidebar.radio(
        "Seleziona funzione",
        (
            "1) Lista NC/AC",
            "2) Consulta NC",
            "3) Modifica NC/AC",
            "4) Inserisci NC",
            "5) Trend NC Quality",
            "6) Gestione piattaforme",
        ),
    )

    st.caption("Backend: Google Sheets (Apps Script)")


    import time
    t0 = time.perf_counter()
    st.info("⏳ Carico dati dal backend…")
    try:
        df_nc = load_nc_data()
        df_ac = load_ac_data()
    except Exception as e:
        st.error("Errore caricamento dati dal backend")
        st.exception(e)
        st.stop()
    finally:
        dt = time.perf_counter() - t0
        st.sidebar.write(f"⏱️ Load time: {dt:.2f}s")

    # Healthcheck rapido (sempre visibile)
    with st.sidebar.expander("📟 Healthcheck", expanded=True):
        st.write("DATA URL:", _script_url_data())
        st.write("NC rows:", 0 if df_nc is None else len(df_nc))
        st.write("AC rows:", 0 if df_ac is None else len(df_ac))
        if df_nc is not None and not df_nc.empty:
            st.write("NC cols (first 15):", list(df_nc.columns)[:15])
        if df_ac is not None and not df_ac.empty:
            st.write("AC cols (first 15):", list(df_ac.columns)[:15])


    # --- DEBUG (puoi disattivare quando tutto è ok) ---
    with st.sidebar.expander('Debug backend', expanded=False):
        st.write('DATA URL:', _script_url_data())
        st.write('NC rows:', len(df_nc))
        st.write('AC rows:', len(df_ac))
        st.write('NC columns:', list(df_nc.columns)[:25], '...' if len(df_nc.columns)>25 else '')
        if len(df_nc) > 0:
            st.write('NC sample (first row):')
            st.json(df_nc.iloc[0].to_dict())
        if len(df_ac) > 0:
            st.write('AC sample (first row):')
            st.json(df_ac.iloc[0].to_dict())

    # Se NC e AC arrivano dallo stesso foglio (export Oracle), separa qui
    if df_ac.empty:
        df_nc, df_ac_from_nc = _split_combined_nc_ac(df_nc)
        if not df_ac_from_nc.empty:
            df_ac = df_ac_from_nc

    if df_nc.empty:
        st.warning('Nessuna NC trovata. Controlla la WebApp DATA (?op=list_nc) e il nome del TAB nel Google Sheet.')
        st.stop()
    else:
        # comunque dedup NC per id per evitare duplicati in lista
        if not df_nc.empty and "id" in df_nc.columns:
            df_nc = df_nc.drop_duplicates(subset=["id"]).copy()

    if scelta.startswith("1"):
        view_lista(df_nc, df_ac)
    elif scelta.startswith("2"):
        view_consulta_nc(df_nc, df_ac)
    elif scelta.startswith("3"):
        view_modifica_nc(df_nc, df_ac)
    elif scelta.startswith("4"):
        view_inserisci_nc(df_nc)
    elif scelta.startswith("5"):
        view_trend_nc_quality_db(df_nc)
    elif scelta.startswith("6"):
        view_gestione_piattaforme()
    # Box di invio email, se richiesto da una delle view
    render_email_prompt()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Fallback: se l'eccezione avviene prima che Streamlit riesca a renderizzare
        import streamlit as st
        try:
            st.set_page_config(page_title="NC Management", layout="wide")
        except Exception:
            pass
        st.error("L'app si è interrotta con un errore. Copia/incolla questo stacktrace qui in chat.")
        st.exception(e)

