
# app_v21.py
# Streamlit app – NC Management (v19-fixed6)
# - AC progressive detection from strings 'AC <n> CVT'
# - AC creation uses formatted code and unique id
# - Email prompt allows manual recipients (prefill from owner suggestion)
# - AC list maps nc_id to NC number using id or number fallback
# - Keeps all fixes from fixed5

import os, re, json, unicodedata
from datetime import date, datetime
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import altair as alt
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections.abc import Mapping
import plotly.express as px
import plotly.graph_objects as go

try:
    import google.generativeai as genai
except ImportError:
    genai = None

# ============================================================
# SECRETS / CONFIG
# ============================================================

def secret_any(*candidates, default: str = ""):
    for c in candidates:
        if isinstance(c, str):
            v = os.getenv(c)
            if v is None and hasattr(st, 'secrets') and c in st.secrets:
                v = st.secrets[c]
            if v is not None:
                s = str(v).strip()
                if s:
                    return s
            continue
        try:
            cur = st.secrets
            ok = True
            for p in c:
                if isinstance(cur, Mapping) and p in cur:
                    cur = cur[p]
                else:
                    ok = False
                    break
            if ok:
                s = str(cur).strip()
                if s:
                    return s
        except Exception:
            pass
    return default

DATA_SCRIPT_URL = secret_any(("google","data_script_url"), ("google","script_url"), "DATA_SCRIPT_URL", "SCRIPT_URL", default="").split("?")[0]
MAIL_SCRIPT_URL = secret_any(("google","mail_script_url"), ("google","script_url"), "MAIL_SCRIPT_URL", default="").split("?")[0]
if not MAIL_SCRIPT_URL:
    MAIL_SCRIPT_URL = DATA_SCRIPT_URL
DATA_KEY        = secret_any(("security","data_api_key"), "DATA_API_KEY", "data_api_key", default="")
GEMINI_API_KEY  = secret_any(("gemini","api_key"), "GEMINI_API_KEY", "gemini_api_key", default="")
GEMINI_MODEL    = secret_any(("gemini","model"), "GEMINI_MODEL", "gemini_model", default="gemini-2.5-flash")
# SMTP relay interno (no-auth)
SMTP_SERVER = secret_any(("mail-server","smtp_server"), ("mail","smtp_server"), "SMTP_SERVER", default="")
SMTP_PORT   = int(secret_any(("mail-server","smtp_server_port"), ("mail","smtp_port"), "SMTP_PORT", default="25") or 25)

MAIL_FROM_HEADER  = secret_any(("mail-sender","from_header"), default="Clivet NC System <no-reply@clivet.it>")
MAIL_ENVELOPE_FROM = secret_any(("mail-sender","envelope_from"), default="no-reply@clivet.it>")  # sistemiamo sotto
MAIL_ENVELOPE_FROM = MAIL_ENVELOPE_FROM.replace(">", "").strip()  # safety se incolli col >TREND_PATH      = secret_any("TREND_PATH", default=r"P:\\QA\\007 Validazione Prodotti\\11 Non conformità\\Trend _NC Quality_.xlsx")

if not DATA_SCRIPT_URL:
    raise RuntimeError("Manca la URL Apps Script DATA: imposta 'google.data_script_url' (o DATA_SCRIPT_URL/SCRIPT_URL).")

# ============================================================
# JSON / DATA HELPERS
# ============================================================

def _json_safe(v):
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v

def _remove_duplicate_keys(records):
    out = []
    for rec in records or []:
        if isinstance(rec, dict):
            clean = {}
            for k, v in rec.items():
                if k not in clean:
                    clean[k] = v
            out.append(clean)
        else:
            out.append(rec)
    return out

# robust per-cell date parser

def _parse_any_date(x):
    try:
        import pandas as _pd
        from datetime import datetime as _dt, date as _date
        if x is None or (isinstance(x, float) and _pd.isna(x)):
            return None
        if isinstance(x, _pd.Timestamp):
            return x.date()
        if isinstance(x, _dt):
            return x.date()
        if isinstance(x, _date):
            return x
        if isinstance(x, dict):
            for k in ('date','datetime','value','Date','DateTime','Value'):
                if k in x and x[k] not in (None, ''):
                    ts = _pd.to_datetime(x[k], errors='coerce')
                    return ts.date() if _pd.notna(ts) else None
            y = x.get('year') or x.get('y') or x.get('Y')
            m = x.get('month') or x.get('m') or x.get('M')
            d = x.get('day') or x.get('d') or x.get('D') or 1
            try:
                return _date(int(y), int(m), int(d)) if (y and m) else None
            except Exception:
                return None
        s = str(x).strip()
        if not s or s.lower() in ('nat','nan','none'):
            return None
        ts = _pd.to_datetime(s, errors='coerce')
        return ts.date() if _pd.notna(ts) else None
    except Exception:
        return None

def _remove_duplicate_nc(data: list[dict]) -> list[dict]:
    seen = {}
    for r in data:
        key = (
            str(r.get("id") or "").strip()
            or str(r.get("nonconformance_number") or "").strip()
        )
        if not key:
            key = json.dumps(r, sort_keys=True)
        seen[key] = r
    return list(seen.values())


@st.cache_data(show_spinner=False)
def _dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    names = list(out.columns)
    if len(set(names)) == len(names):
        return out
    seen = {}
    for i, n in enumerate(names):
        seen.setdefault(n, []).append(i)
    dup_names = [n for n, idxs in seen.items() if len(idxs) > 1]
    for n in dup_names:
        idxs = seen[n]
        block = out.iloc[:, idxs]
        coalesced = block.iloc[:, 0].copy()
        for j in range(1, block.shape[1]):
            s = block.iloc[:, j]
            try:
                m = coalesced.isna() | (coalesced.astype(str).str.strip() == "")
            except Exception:
                m = coalesced.isna()
            coalesced = coalesced.where(~m, s)
        first = idxs[0]
        out = out.drop(out.columns[idxs], axis=1)
        out.insert(first, n, coalesced)
        names = list(out.columns)
        seen = {}
        for i, nm in enumerate(names):
            seen.setdefault(nm, []).append(i)
    return out

def _ensure_unique_columns(df):
    try:
        if df is None or df.empty:
            return df
        df2 = df.loc[:, ~df.columns.duplicated()].copy()
        df2 = _dedup_columns(df2)
        return df2
    except Exception:
        return df

# --- AC progressive helpers ---

def _extract_ac_progressive(val) -> int | None:
    s = str(val or "").strip()
    if not s:
        return None
    m = re.search(r"\bAC\s*(\d+)\s*CVT\b", s, flags=re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    m = re.search(r"(\d+)", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    try:
        return int(s)
    except Exception:
        return None

def _next_ac_progressive(df_ac_all: pd.DataFrame) -> int:
    maxn = 0
    if df_ac_all is None or df_ac_all.empty:
        return 1
    ser = df_ac_all.get('ac_corrective_action_num')
    if ser is None:
        return 1
    if isinstance(ser, pd.DataFrame):
        ser = ser.iloc[:, 0]
    for v in ser.astype(str).tolist():
        n = _extract_ac_progressive(v)
        if n is not None and n > maxn:
            maxn = n
    return maxn + 1

# ============================================================
# BACKEND API (Apps Script)
# ============================================================

def _api_get(op: str, **params):
    url = DATA_SCRIPT_URL
    q = {"op": op, **params}
    if DATA_KEY:
        q['key'] = DATA_KEY
    r = requests.get(url, params=q, timeout=(5,30), headers={"Accept":"application/json"})
    r.raise_for_status()
    try:
        j = r.json()
    except Exception:
        snippet = (r.text or "").strip()[:800]
        raise RuntimeError("Risposta non-JSON dalla WebApp. Controlla il deploy. Snippet: " + snippet)
    if not j.get('ok'):
        raise RuntimeError(j.get('error', 'API error'))
    return j.get('data')

def _api_post(op: str, **body):
    url = DATA_SCRIPT_URL
    payload = {"op": op, **body}
    if DATA_KEY:
        payload['key'] = DATA_KEY
    r = requests.post(url, json=payload, timeout=(5,60), headers={"Accept":"application/json"})
    r.raise_for_status()
    try:
        j = r.json()
    except Exception:
        snippet = (r.text or "").strip()[:800]
        raise RuntimeError("Risposta non-JSON dalla WebApp (POST). Snippet: " + snippet)
    if not j.get('ok'):
        raise RuntimeError(j.get('error', 'API error'))
    return j.get('data')

# ============================================================
# LOADERS
# ============================================================

@st.cache_data(show_spinner=False)
def load_nc_data() -> pd.DataFrame:
    data = _api_get("list_nc") or []
    data = _remove_duplicate_nc(data)
    df = pd.DataFrame(data)

    if df.empty:
        return df
    def _clean(c: str) -> str:
        c = str(c or "").replace("\u00a0"," ").replace("\xa0"," ")
        return re.sub(r"\s+"," ", c).strip()
    df.columns = [_clean(c) for c in df.columns]
    df = _dedup_columns(df)
    def _norm_key(s: str) -> str:
        s = str(s or "").upper().strip()
        s = s.replace(" ", "_")
        s = s.replace(".", "")
        s = re.sub(r"[^A-Z0-9_]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    norm_cols = {_norm_key(c): c for c in df.columns}  # norm -> originale

    def _rename_if_exists(norm_name: str, new_name: str, ren: dict):
        if norm_name in norm_cols:
            ren[norm_cols[norm_name]] = new_name

    cols_up = {str(c).strip().upper(): c for c in df.columns}
    def _have(*names):
        for n in names:
            k = str(n).strip().upper()
            if k in cols_up:
                return cols_up[k]
        return None
    pre = {}
    ncnum = _have('NONCONFORMANCE_NUMBER','NC_NUMBER','NONCONFORMANCE NO','NONCONFORMANCE NUM')
    if ncnum and ncnum != 'NONCONFORMANCE_NUMBER': pre[ncnum] = 'NONCONFORMANCE_NUMBER'
    ncstat = _have('NONCONFORMANCE_STATUS')
    if ncstat and ncstat != 'NONCONFORMANCE_STATUS': pre[ncstat] = 'NONCONFORMANCE_STATUS'
    opend  = _have('DATE_OPENED');  closed = _have('DATE_CLOSED')
    if opend  and opend  != 'DATE_OPENED':  pre[opend]  = 'DATE_OPENED'
    if closed and closed != 'DATE_CLOSED':  pre[closed] = 'DATE_CLOSED'
    if pre:
        df = df.rename(columns=pre)
    oracle_nc_map = {
        'NONCONFORMANCE_NUMBER':'nonconformance_number',
        'NONCONFORMANCE_STATUS':'nonconformance_status',
        'DATE_OPENED':'date_opened',
        'DATE_CLOSED':'date_closed',
        'OWNER':'owner',
        'RESPONSIBILITY':'responsibility',
        'PIATTAFORMA':'piattaforma', 'PIATT.':'piattaforma',
        'SHORT_DESCRIPTION':'short_description',
        'DETAILED_DESCRIPTION':'detailed_description',
        'NC_PARENT_REF':'nc_parent_ref',
        'MOB':'mob',
    }
    ren = {}

    # --- campi base NC ---
    _rename_if_exists("NONCONFORMANCE_NUMBER", "nonconformance_number", ren)
    _rename_if_exists("NONCONFORMANCE_STATUS", "nonconformance_status", ren)
    _rename_if_exists("DATE_OPENED", "date_opened", ren)
    _rename_if_exists("DATE_CLOSED", "date_closed", ren)

    _rename_if_exists("NC_PARENT_Y_N", "nc_parent_y_n", ren)
    _rename_if_exists("NC_PARENT_REF", "nc_parent_ref", ren)

    _rename_if_exists("NONCONFORMANCE_SOURCE", "nonconformance_source", ren)
    _rename_if_exists("SERVICE_REQUEST", "service_request", ren)
    _rename_if_exists("INCIDENT_TYPE", "incident_type", ren)

    _rename_if_exists("ITEM_INSTANCE_SERIAL", "item_instance_serial", ren)
    _rename_if_exists("ITEM_ID", "item_id", ren)

    # Attenzione: hai ITEM due volte nel tuo elenco.
    # Qui rinomino il primo che trovo come "item" e l'altro (se presente come colonna distinta) lo gestirai se serve.
    _rename_if_exists("ITEM", "item", ren)
    _rename_if_exists("ITEM_DESC", "item_desc", ren)

    _rename_if_exists("SERIE", "serie", ren)
    _rename_if_exists("GRANDEZZA", "grandezza", ren)

    # Piattaforma: supporta PIATT / PIATTAFORMA / PIATT.
    _rename_if_exists("PIATT", "piattaforma", ren)
    _rename_if_exists("PIATTAFORMA", "piattaforma", ren)

    # Macro piattaforma: supporta "MACRO PIATT." e "MACRO PIATTAFORMA"
    _rename_if_exists("MACRO_PIATT", "macro_piattaforma", ren)
    _rename_if_exists("MACRO_PIATTAFORMA", "macro_piattaforma", ren)

    _rename_if_exists("MOB", "mob", ren)

    _rename_if_exists("SHORT_DESCRIPTION", "short_description", ren)
    _rename_if_exists("DETAILED_DESCRIPTION", "detailed_description", ren)

    _rename_if_exists("NONCONFORM_PRIORITY", "nonconform_priority", ren)

    _rename_if_exists("SUPPLIER", "supplier", ren)
    _rename_if_exists("ENTERED_BY_USER", "created_by", ren)
    _rename_if_exists("OWNER", "owner", ren)
    _rename_if_exists("EMAIL_ADDRESS", "email_address", ren)
    _rename_if_exists("SEND_EMAIL", "send_email", ren)

    _rename_if_exists("QUANTITY_NONCONFORMING", "quantity_nonconforming", ren)
    _rename_if_exists("NONCONFORMING_UOM", "nonconforming_uom", ren)
    _rename_if_exists("DAYS_TO_CLOSE", "days_to_close", ren)

    _rename_if_exists("COST_SMRY_INTERNAL", "cost_smry_internal", ren)
    _rename_if_exists("COST_SMRY_CUSTOMER", "cost_smry_customer", ren)

    _rename_if_exists("RESPONSIBILITY", "responsibility", ren)

    # --- DET_* (usati in UI e in Gemini) ---
    _rename_if_exists("DET_PROBLEM_DESCRIPTION", "det_problem_description", ren)
    _rename_if_exists("DET_CAUSE", "det_cause", ren)
    _rename_if_exists("DET_CLOSE", "det_close", ren)

    # applica rename "nuovo"
    if ren:
        df = df.rename(columns=ren)

    # applica anche mapping legacy (se presenti colonne Oracle)
    ren2 = {k: v for k, v in oracle_nc_map.items() if k in df.columns}
    if ren2:
        df = df.rename(columns=ren2)

    # da qui in poi: SEMPRE
    df = _dedup_columns(df)

    if 'nonconformance_number' in df.columns:
       df['id'] = df.get('id', '').astype(str).str.strip()
       mask = (df['id'] == '') | df['id'].isna()
       df.loc[mask, 'id'] = df.loc[mask, 'nonconformance_number']


    for col in ['date_opened', 'date_closed', 'created_at', 'updated_at']:
        if col in df.columns:
            df[col] = df[col].apply(_parse_any_date)

    if 'id' in df.columns:
        df['id'] = df['id'].astype(str).str.strip()

    if 'nonconformance_number' in df.columns:
        nn = df['nonconformance_number'].astype(str).str.strip()
        if (
            'id' not in df.columns
            or df['id'].nunique(dropna=False) <= 1
            or (df['id'].astype(str).str.strip() == "").all()
        ):
            df['id'] = nn

    return df
@st.cache_data(show_spinner=False)
def load_ac_data() -> pd.DataFrame:
    data = _api_get("list_ac") or []
    data = _remove_duplicate_keys(data)
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=['id','nc_id'])
    def _clean(c: str) -> str:
        c = str(c or "").replace("\u00a0"," ").replace("\xa0"," ")
        return re.sub(r"\s+", " ", c).strip()
    df.columns = [_clean(c) for c in df.columns]
    df = _dedup_columns(df)

    # normalizza intestazioni (spazi, punti, ecc.)
    def _norm_key(s: str) -> str:
        s = str(s or "").upper().strip()
        s = s.replace(" ", "_").replace(".", "")
        s = re.sub(r"[^A-Z0-9_]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    norm_cols = {_norm_key(c): c for c in df.columns}

    def _rename_if_exists(norm_name: str, new_name: str, ren: dict):
        if norm_name in norm_cols:
            ren[norm_cols[norm_name]] = new_name

    ren = {}

    # chiavi / collegamento NC
    _rename_if_exists("ID", "id", ren)
    _rename_if_exists("NC_ID", "nc_id", ren)

    # campi AC (nuovi fogli separati)
    _rename_if_exists("AC_CORRECTIVE_ACTION_NUM", "ac_corrective_action_num", ren)
    _rename_if_exists("AC_REQUEST_SOURCE", "ac_request_source", ren)
    _rename_if_exists("AC_IMPLEMENTATION_TYPE", "ac_implementation_type", ren)
    _rename_if_exists("AC_DATE_OPENED", "ac_date_opened", ren)
    _rename_if_exists("AC_REQUESTOR", "ac_requestor", ren)
    _rename_if_exists("AC_OWNER", "ac_owner", ren)
    _rename_if_exists("AC_SEND_EMAIL", "ac_send_email", ren)
    _rename_if_exists("AC_EMAIL_ADDRESS", "ac_email_address", ren)
    _rename_if_exists("AC_SHORT_DESCRIPTION", "ac_short_description", ren)
    _rename_if_exists("AC_REQUEST_PRIORITY", "ac_request_priority", ren)
    _rename_if_exists("AC_DATE_REQUIRED", "ac_date_required", ren)
    _rename_if_exists("AC_DETAILED_DESCRIPTION", "ac_detailed_description", ren)
    _rename_if_exists("AC_COST_SMRY_INTERNAL", "ac_cost_smry_internal", ren)
    _rename_if_exists("AC_END_DATE", "ac_end_date", ren)
    _rename_if_exists("AC_EFFECTIVE", "ac_effective", ren)
    _rename_if_exists("AC_EVIDENCE_VERIFY", "ac_evidence_verify", ren)
    _rename_if_exists("AC_FOLLOW_UP_DATE", "ac_follow_up_date", ren)
    _rename_if_exists("AC_REQUEST_STATUS", "ac_request_status", ren)
    _rename_if_exists("AC_DAYS_TO_CLOSE", "ac_days_to_close", ren)
    _rename_if_exists("AC_CAR_CLASS", "ac_car_class", ren)
    _rename_if_exists("NEW_MACRO_PIATTAFORMA", "new_macro_piattaforma", ren)
    _rename_if_exists("NEW_MACRO_PIATTAFORMA_", "new_macro_piattaforma", ren)  # tolleranza

    # compatibilità vecchia colonna piattaforma (se presente)
    _rename_if_exists("PIATTAFORMA", "piattaforma", ren)

    if ren:
        df = df.rename(columns=ren)

    df = _dedup_columns(df)

    # date parsing
    for col in ['ac_date_opened','ac_date_required','ac_end_date','ac_follow_up_date','created_at','updated_at']:
        if col in df.columns:
            df[col] = df[col].apply(_parse_any_date)

    # coerci id
    if 'id' not in df.columns and 'ac_corrective_action_num' in df.columns:
        df['id'] = df['ac_corrective_action_num'].astype(str).str.strip()

    for c in ['id','nc_id']:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df

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

@st.cache_data(show_spinner=False)
def _split_combined_nc_ac(df_nc: pd.DataFrame):
    if df_nc is None or df_nc.empty:
        return df_nc, pd.DataFrame()
    key_nc = 'nonconformance_number' if 'nonconformance_number' in df_nc.columns else ('id' if 'id' in df_nc.columns else None)
    if key_nc is None:
        return df_nc.drop_duplicates().copy(), pd.DataFrame()
    if key_nc == 'nonconformance_number':
        df_nc['nonconformance_number'] = df_nc['nonconformance_number'].astype(str).str.strip()
        df_nc['id'] = df_nc['nonconformance_number']
    ac_cols = [c for c in df_nc.columns if str(c).startswith('ac_')]
    if not ac_cols:
        return df_nc.drop_duplicates(subset=[key_nc]).copy(), pd.DataFrame()
    df_nc_unique = df_nc.drop_duplicates(subset=[key_nc], keep='first').copy()
    if 'ac_corrective_action_num' not in df_nc.columns:
        return df_nc_unique, pd.DataFrame()
    ser_acnum = df_nc['ac_corrective_action_num'].astype(str).str.strip()
    m = ser_acnum.ne("") & df_nc['ac_corrective_action_num'].notna()
    df_ac = df_nc.loc[m, [key_nc] + ac_cols].copy()
    df_ac = df_ac.rename(columns={key_nc:'nc_id'})
    df_ac = _ensure_unique_columns(df_ac)
    df_ac['id'] = df_ac['ac_corrective_action_num'].astype(str).str.strip()
    df_ac['nc_id'] = df_ac['nc_id'].astype(str).str.strip()
    for col in ['ac_date_opened','ac_date_required','ac_end_date','ac_follow_up_date','created_at','updated_at']:
        if col in df_ac.columns:
            df_ac[col] = df_ac[col].apply(_parse_any_date)
    df_ac = df_ac.drop_duplicates(subset=['id'], keep='first').copy()
    return df_nc_unique, df_ac

# ============================================================
# CRUD WRAPPERS
# ============================================================

def _serialize_dict(d: dict) -> dict:
    return {k: _json_safe(v) for k,v in (d or {}).items()}

def insert_nc_in_db(values: dict) -> str:
    out = _api_post("create_nc", payload=_serialize_dict(values))
    load_nc_data.clear(); load_ac_data.clear()
    return str(out.get('id','')) if isinstance(out, dict) else ""

def update_nc_in_db(nc_id: str, values: dict):
    _api_post("update_nc", id=str(nc_id), patch=_serialize_dict(values))
    load_nc_data.clear(); load_ac_data.clear()

def insert_ac_in_db(nc_id: str, values: dict):
    payload = {"nc_id": str(nc_id)}
    payload.update(values or {})
    _api_post("create_ac", payload=_serialize_dict(payload))
    load_ac_data.clear()

def update_ac_in_db(nc_id: str, ac_id: str, values: dict):
    patch = _serialize_dict(values)
    _api_post("update_ac", id=str(ac_id), patch=patch)
    load_ac_data.clear()

# ============================================================
# EMAIL via iframe (Template)
# ============================================================
from string import Template

def send_mail_via_hidden_iframe(script_url: str, payload: dict, key: str = "sendmail"):
    """
    TOP-only: apre l'invio mail in nuova scheda (necessario per login Google).
    Manteniamo il nome per compatibilità con il resto dell'app.
    """
    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_js = json.dumps(payload_json).replace("</", "<\\/")

    tmpl_str = r"""
<div id="$key_wrap" style="padding:6px 0;">
  <div style="margin-bottom:6px;">📧 Pronto per inviare l’email.</div>
  <button id="$key_btn"
    style="display:inline-block; padding:6px 10px; border:1px solid #999; border-radius:6px; background:#fff; cursor:pointer;">
    Apri invio email (nuova scheda)
  </button>
  <div style="padding:6px 0; font-size:12px; color:#666;">
    Nota: l’invio avviene con l’account Google attualmente loggato nel browser.
  </div>
</div>

<script>
(function(){
  const payload = $payload_js;
  const scriptUrl = "$script_url";

  function toB64Unicode(str){ return btoa(unescape(encodeURIComponent(str))); }
  const b64 = toB64Unicode(payload);

  const url = scriptUrl
    + '?op=send_mail'
    + '&mode=top'
    + '&payload_b64=' + encodeURIComponent(b64);

  const btn = document.getElementById("$key_btn");
  btn.addEventListener("click", function(){
    window.open(url, "_blank", "noopener");
  });
})();
</script>
"""
    html = Template(tmpl_str).substitute(
        script_url=script_url,
        payload_js=payload_js,
        key=key,
        key_wrap=f"{key}_wrap",
        key_btn=f"{key}_btn",
    )
    components.html(html, height=120)# SMTP fallback

def _parse_recipients(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = re.split(r"[;, \n\r\t]+", raw)
    out = []
    seen = set()
    for p in parts:
        p = p.strip()
        if not p or "@" not in p:
            continue
        k = p.lower()
        if k not in seen:
            out.append(p)
            seen.add(k)
    return out


def send_email(to_addresses, subject, body):
    """
    Invio tramite SMTP relay interno (no-auth) su internal-mx.clivet.it:25.
    """
    # accetta stringa "a@;b@" oppure lista
    if isinstance(to_addresses, str):
        to_list = _parse_recipients(to_addresses)
    else:
        to_list = []
        for x in (to_addresses or []):
            to_list += _parse_recipients(str(x))
    if not to_list:
        return

    msg = MIMEMultipart()
    msg['From'] = MAIL_FROM_HEADER
    msg['To'] = ", ".join(to_list)
    msg['Subject'] = subject
    msg.attach(MIMEText(body or "", 'plain'))

    try:
        if not SMTP_SERVER or not SMTP_PORT:
            raise RuntimeError("SMTP_SERVER/SMTP_PORT non configurati in secrets")

        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT), timeout=15) as server:
            server.ehlo()
            # no TLS, no login
            server.sendmail(MAIL_ENVELOPE_FROM, to_list, msg.as_string())

    except Exception as e:
        try:
            st.error(f"Errore nell'invio della mail (SMTP relay): {e}")
        except Exception:
            print("Errore nell'invio della mail (SMTP relay):", e)

# ============================================================
# NC/AC HELPERS & EMAIL PROMPT
# ============================================================

def _normalize_name_for_email(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = "".join(c for c in s if c.isalpha() or c in "-")
    return s

def suggest_email_from_name(name: str):
    if not name: return None
    parts = [p for p in name.split() if p.strip()]
    if len(parts) < 2: return None
    first = _normalize_name_for_email(parts[0])
    last  = _normalize_name_for_email(parts[-1])
    if not first or not last: return None
    return f"{first[0]}.{last}@clivet.it"

def _pick_first(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None: continue
        s = str(v).strip()
        if s and s.lower() != 'nan':
            return s
    return ""
def normalize_nc_dict(raw: dict) -> dict:
    """Converte un dict NC proveniente da Apps Script (header legacy) in chiavi canoniche snake_case."""
    d = raw or {}

    def P(*keys):
        return _pick_first(d, list(keys))

    return {
        "nonconformance_number": P("nonconformance_number", "NONCONFORMANCE_NUMBER", "NC_NUMBER"),
        "nonconformance_status": P("nonconformance_status", "NONCONFORMANCE_STATUS"),
        "date_opened": P("date_opened", "DATE_OPENED"),
        "date_closed": P("date_closed", "DATE_CLOSED"),
        "serie": P("serie", "SERIE"),
        "piattaforma": P("piattaforma", "PIATTAFORMA", "PIATT.", "PIATT"),
        "mob": P("mob", "MOB"),
        "responsibility": P("responsibility", "RESPONSIBILITY"),
        "owner": P("owner", "OWNER"),
        "email_address": P("email_address", "EMAIL_ADDRESS"),
        "short_description": P("short_description", "SHORT_DESCRIPTION"),
        "detailed_description": P("detailed_description", "DETAILED_DESCRIPTION"),
    }

def get_nc_details(nc_id: str) -> dict:
    try: data = _api_get('get_nc', id=str(nc_id))
    except Exception: data = None
    return data if isinstance(data, dict) else {}

def get_ac_details_for_nc(nc_id: str) -> list[dict]:
    try: data = _api_get('list_ac_for_nc', nc_id=str(nc_id))
    except Exception: data = None
    return data if isinstance(data, list) else []

def get_emails_for_nc(nc_id: str) -> list[str]:
    emails = set()
    nc = get_nc_details(nc_id)
    if isinstance(nc, dict):
        v = (nc.get('email_address') or '').strip()
        if v: emails.add(v)
    for ac in get_ac_details_for_nc(nc_id) or []:
        v = (ac.get('ac_email_address') or '').strip()
        if v: emails.add(v)
    return sorted(emails)

def _operation_to_action(operation: str) -> str:
    op = (operation or '').lower()
    if 'inser' in op or 'crea' in op or 'nuova' in op: return 'create_nc'
    if 'modif' in op or 'aggiorn' in op or 'update' in op: return 'update_nc'
    return 'update_nc'

def trigger_email_prompt(nc_id: str, operation: str, default_to: str = ""):
    st.session_state['email_nc_id'] = nc_id
    st.session_state['email_operation'] = operation
    st.session_state['email_default_to'] = (default_to or "").strip()
    st.session_state['show_email_prompt'] = True

def render_email_prompt():
    if not st.session_state.get('show_email_prompt'):
        return

    nc_id = st.session_state.get('email_nc_id')
    operation = st.session_state.get('email_operation', 'Aggiornamento NC')

    try:
        data = _api_get('get_nc', id=str(nc_id))
        if isinstance(data, dict):
            nc_number = _pick_first(data, ["nonconformance_number","NONCONFORMANCE_NUMBER","NC_NUMBER"]) or str(nc_id)
        else:
            nc_number = str(nc_id)
    except Exception:
        nc_number = str(nc_id)

    emails = get_emails_for_nc(nc_id) if nc_id is not None else []
    safe_op = re.sub(r"[^A-Za-z0-9]+", "_", str(operation))
    ctx = f"{nc_id}_{safe_op}"
    payload_key = f"email_payload_{ctx}"
    sent_key = f"email_sent_{ctx}"
    yes_key = f"email_send_yes_{ctx}"
    no_key = f"email_send_no_{ctx}"

    st.markdown('---')
    st.subheader('Inviare le modifiche agli owner?')
    st.write(f"Vuoi inviare una mail agli owner della NC **{nc_number}**?")

    nc_raw0 = get_nc_details(nc_id) if nc_id is not None else {}
    nc_norm0 = normalize_nc_dict(nc_raw0)
    owner_name = nc_norm0.get('owner') or ''
    suggested = suggest_email_from_name(owner_name) if owner_name else ''

    default_to = (st.session_state.get('email_default_to') or '').strip()
    prefill = default_to or (', '.join(emails) if emails else (suggested or ''))
    recipients_input = st.text_input('Destinatari (separati da ,)', value=prefill)

    if not recipients_input.strip():
        st.info("Inserisci almeno un destinatario per inviare l'email.")

    c1, c2 = st.columns(2)
    with c1:
        if st.button('✉️ Sì, invia', key=yes_key):
            to_value = recipients_input.strip()
            if to_value:
                nc_raw = get_nc_details(nc_id)
                nc_norm = normalize_nc_dict(nc_raw)

                ac_list = get_ac_details_for_nc(nc_id) or []
                subject_val, body_val = generate_email_subject_body(operation, nc_norm, ac_list)

                action = _operation_to_action(operation)

                st.session_state[payload_key] = {
                    'action': action,
                    'use_gemini': True,
                    'to': to_value,
                    'subject': subject_val,
                    'body': body_val,
                    'nc': {
                        'nonconformance_number': nc_norm.get('nonconformance_number') or nc_number,
                        'short_description': nc_norm.get('short_description',''),
                        'opened_by': (nc_raw or {}).get('created_by','') or nc_norm.get('owner',''),
                        'responsibility': nc_norm.get('responsibility',''),
                        'nonconformance_status': nc_norm.get('nonconformance_status',''),
                        'piattaforma': nc_norm.get('piattaforma',''),
                        'mob': nc_norm.get('mob',''),
                    },
                    'ac_list': [
                        {
                            'ac_corrective_action_num': a.get('ac_corrective_action_num','') or a.get('AC_CORRECTIVE_ACTION_NUM',''),
                            'ac_short_description': a.get('ac_short_description','') or a.get('AC_SHORT_DESCRIPTION',''),
                            'ac_owner': a.get('ac_owner','') or a.get('AC_OWNER',''),
                            'ac_request_status': a.get('ac_request_status','') or a.get('AC_REQUEST_STATUS',''),
                        } for a in ac_list
                    ],
                }
            else:
                st.warning('Nessun indirizzo email: impossibile inviare.')

            if st.session_state.get(payload_key) and not st.session_state.get(sent_key):
                p = st.session_state[payload_key]
                # invio SMTP interno
                send_email(p.get("to", ""), p.get("subject", ""), p.get("body", ""))

                st.session_state[sent_key] = True
                st.session_state.pop(payload_key, None)
                st.success("Email inviata via SMTP relay interno.")

        if st.button('✅ Chiudi', key=f"email_close_{ctx}"):
            st.session_state['show_email_prompt'] = False
            st.session_state.pop('email_default_to', None)
            st.session_state.pop(payload_key, None)
            st.session_state.pop(sent_key, None)
            st.rerun()

    with c2:
        if st.button('❌ No, non inviare', key=no_key):
            st.session_state['show_email_prompt'] = False
            st.session_state.pop('email_default_to', None)
            st.session_state.pop(sent_key, None)

# ============================================================
# GEMINI
# ============================================================

def call_gemini(prompt: str) -> str:
    if genai is None:
        raise RuntimeError("Libreria google-generativeai non installata. 'pip install google-generativeai'")
    if not GEMINI_API_KEY:
        raise RuntimeError("Chiave GEMINI_API_KEY mancante (env o st.secrets).")
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)
    resp = model.generate_content(prompt)
    return resp.text or ""



def generate_email_subject_body(operation: str, nc: dict, ac_list: list[dict] | None = None) -> tuple[str, str]:
    """Genera oggetto e corpo email con Gemini. Fallback deterministico se Gemini non disponibile."""
    ac_list = ac_list or []
    op = (operation or "").lower()

    if "nuova ac" in op or ("ac" in op and ("creat" in op or "nuov" in op)):
        event = "AC_CREATED"
    elif "modifica ac" in op or ("ac" in op and ("modif" in op or "aggiorn" in op)):
        event = "AC_UPDATED"
    elif "nuova nc" in op or ("nc" in op and ("creat" in op or "nuov" in op)):
        event = "NC_CREATED"
    elif "modifica" in op or "aggiorn" in op:
        event = "NC_UPDATED"
    else:
        event = "NC_UPDATED"

    # contesto minimo e stabile (chiavi principali richieste)
    ncn = (nc or {}).get("nonconformance_number", "")
    serie = (nc or {}).get("serie", "")
    opened = (nc or {}).get("date_opened", "")
    sdesc = (nc or {}).get("short_description", "")
    ldesc = (nc or {}).get("detailed_description", "")

    ac_lines = []
    for a in ac_list[:10]:
        ac_lines.append(
            f"- {a.get('ac_corrective_action_num','')} | {a.get('ac_request_status','')} | "
            f"{a.get('ac_owner','')} | {a.get('ac_short_description','')}"
        )
    ac_block = "\n".join(ac_lines) if ac_lines else "(nessuna AC collegata)"

    prompt = f"""
Sei un assistente per Quality Management (Non Conformità e Azioni Correttive).
Devi generare una email professionale in italiano.

Evento: {event}

Dati NC:
- Numero NC: {ncn}
- Serie: {serie}
- Data apertura: {opened}
- Short description: {sdesc}
- Descrizione dettagliata: {ldesc}

AC collegate (se presenti):
{ac_block}

Requisiti:
- Se evento NC_CREATED: scrivi che la NC è stata creata.
- Se evento NC_UPDATED: scrivi che la NC è stata modificata.
- Se evento AC_CREATED: scrivi che è stata inserita una nuova Azione Correttiva collegata alla NC.
- Se evento AC_UPDATED: scrivi che è stata modificata una Azione Correttiva collegata alla NC.
- Tono: chiaro, sintetico, operativo. Non inventare dati.
- Restituisci ESCLUSIVAMENTE un JSON con due chiavi: "subject" e "body".
- Il body deve essere testo semplice (no HTML), con elenco puntato se utile.
"""

    try:
        out = call_gemini(prompt)
        m = re.search(r"\{.*\}", out, flags=re.S)
        if m:
            j = json.loads(m.group(0))
            subject = str(j.get("subject", "")).strip()
            body = str(j.get("body", "")).strip()
            if subject and body:
                return subject, body
    except Exception:
        pass

    # Fallback deterministico
    if event == "NC_CREATED":
        subj = f"[{ncn}] NC creata"
    elif event == "NC_UPDATED":
        subj = f"[{ncn}] NC modificata"
    elif event == "AC_CREATED":
        subj = f"[{ncn}] Nuova AC collegata"
    else:
        subj = f"[{ncn}] AC aggiornata"

    body = (
        f"Ciao,\n\n"
        f"Evento: {event}\n"
        f"NC: {ncn}\n"
        f"Serie: {serie}\n"
        f"Data apertura: {opened}\n\n"
        f"Sintesi: {sdesc}\n\n"
        f"Dettaglio: {ldesc}\n\n"
        f"AC collegate:\n{ac_block}\n\n"
        f"Grazie.\n"
    )
    return subj, body

def build_nc_ac_context(nc_row: pd.Series, df_ac_nc: pd.DataFrame) -> str:
    lines = []
    g = nc_row.get
    lines.append(f"NC number: {g('nonconformance_number')}")
    lines.append(f"Status: {g('nonconformance_status')}")
    lines.append(f"Opened: {g('date_opened')}")
    lines.append(f"Closed: {g('date_closed')}")
    lines.append(f"Serie: {g('serie')}")
    lines.append(f"Piattaforma: {g('piattaforma')}")
    if 'mob' in nc_row.index:
        lines.append(f"Make/Buy (MOB): {g('mob')}")
    lines.append(f"Owner: {g('owner')}")
    for label, col in [
        ("Short description","short_description"),
        ("Detailed description","detailed_description"),
        ("Problem description (DET_)","det_problem_description"),
        ("Cause (DET_CAUSE)","det_cause"),
        ("Chiusura (DET_CLOSE)","det_close"),
        ("Responsabilità","responsibility"),
    ]:
        val = g(col)
        if val:
            lines.append(f"{label}: {val}")
    lines.append("\nCorrective Actions (AC):")
    if df_ac_nc is not None and not df_ac_nc.empty:
        for _, r in df_ac_nc.iterrows():
            lines.append(f"- AC {r.get('ac_corrective_action_num')} (owner: {r.get('ac_owner')}, status: {r.get('ac_request_status')})")
            if r.get('ac_short_description'):
                lines.append(f"  Short: {r.get('ac_short_description')}")
            if r.get('ac_detailed_description'):
                lines.append(f"  Detail: {r.get('ac_detailed_description')}")
            lines.append(f"  Dates: opened={r.get('ac_date_opened')}, required={r.get('ac_date_required')}, closed={r.get('ac_end_date')}")
    else:
        lines.append(" (no AC linked)")
    return "\n".join(lines)

def run_gemini_verifica_nc(nc_row: pd.Series, df_ac_nc: pd.DataFrame):
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
4. Sono state considerate le azioni su magazzino, fornitori, clienti?
5. Vedi retrofit o comunicazioni utili da considerare?
Rispondi in italiano, con bullet points brevi e azionabili.
"""
    with st.spinner("Analisi NC con Gemini in corso..."):
        try:
            st.subheader("Suggerimenti di Gemini")
            st.write(call_gemini(prompt))
        except Exception as e:
            st.error(f"Impossibile contattare Gemini: {e}")

def run_gemini_8d_report(nc_row: pd.Series, df_ac_nc: pd.DataFrame, language_label: str):
    ctx = build_nc_ac_context(nc_row, df_ac_nc)
    lang = 'it' if language_label.lower().startswith('ital') else 'en'
    instr = (
        """
Genera un report 8D completo in italiano, con sezioni D1..D8. Se mancano informazioni, indica N/A o suggerimenti.
Stile tecnico e sintetico.
""" if lang == 'it' else """
Generate a complete 8D report in English (D1..D8). If data is missing, mark N/A or add suggestions.
Technical, concise style.
"""
    )
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
            st.subheader(f"8D Report ({language_label})")
            st.text(call_gemini(prompt))
        except Exception as e:
            st.error(f"Impossibile generare l'8D con Gemini: {e}")

# ============================================================
# UI HELPERS
# ============================================================

# --- Streamlit compatibility wrappers (use_container_width removed end-2025) ---

def st_plotly(fig, **kwargs):
    """Safe st.plotly_chart wrapper across Streamlit versions."""
    try:
        return st.plotly_chart(fig, use_container_width=True, **kwargs)
    except TypeError:
        return st.plotly_chart(fig, **kwargs)

def apply_nc_filters(df: pd.DataFrame) -> pd.DataFrame:
    f = st.text_input("Numero NC contiene:", value="").strip()
    if f:
        df = df[df['nonconformance_number'].astype(str).str.contains(f, case=False, na=False)]
    status_list = sorted(df['nonconformance_status'].dropna().unique().tolist()) if 'nonconformance_status' in df.columns else []
    status_selected = st.multiselect("Stato NC", status_list, default=[])
    if status_selected:
        df = df[df['nonconformance_status'].isin(status_selected)]
    resp_list = sorted(df['responsibility'].dropna().unique().tolist()) if 'responsibility' in df.columns else []
    resp_sel = st.multiselect("Responsabilità", resp_list, default=[])
    if resp_sel:
        df = df[df['responsibility'].isin(resp_sel)]
    owner_list = sorted(df['owner'].dropna().unique().tolist()) if 'owner' in df.columns else []
    owner_sel = st.multiselect("Owner", owner_list, default=[])
    if owner_sel:
        df = df[df['owner'].isin(owner_sel)]
    return df

def get_display_status(row: pd.Series) -> str:
    raw = (row.get('nonconformance_status') or '').upper().strip()
    parent_ref = str(row.get('nc_parent_ref') or '').strip()
    return 'MANAGED' if parent_ref else (raw or 'NEW')

def status_to_color(status: str) -> str:
    s = (status or '').upper()
    if s in ('NEW','OPEN'): return '#cc0000'
    if s == 'MANAGED': return '#ff8800'
    if s in ('CLOSED','CLOSE','CLOSED/VERIFIED','CANCELLED','CANCELED','CHIUSA'): return '#008000'
    return '#555555'

def render_status_html(status: str) -> str:
    return f"<span style='color:{status_to_color(status)}; font-weight:bold'>{status}</span>"

def safe_date_for_input(val):
    if val is None or val == '': return None
    try:
        if pd.isna(val): return None
        if isinstance(val, pd.Timestamp): return val.date()
    except Exception: pass
    if isinstance(val, datetime): return val.date()
    if isinstance(val, date): return val
    if isinstance(val, str):
        try: return datetime.fromisoformat(val).date()
        except ValueError: return None
    return None

def compute_trend_from_db(df_nc: pd.DataFrame, weeks_back: int = 52) -> pd.DataFrame:
    if df_nc.empty: return pd.DataFrame()
    df = df_nc.copy()
    df['date_opened'] = pd.to_datetime(df['date_opened'], errors='coerce')
    df = df.dropna(subset=['date_opened'])
    df['year_week'] = df['date_opened'].dt.strftime('%Y-%U')
    trend = df.groupby('year_week').agg(started=('id','count')).reset_index()
    trend.rename(columns={'started': 'Started last 8 Week'}, inplace=True)
    trend['data_pubblicazione'] = pd.to_datetime(trend['year_week'] + '-1', format='%Y-%U-%w', errors='coerce')
    return trend.sort_values('data_pubblicazione')

@st.cache_data(show_spinner=False)
def load_trend_data() -> pd.DataFrame:
    try:
        if TREND_PATH and os.path.exists(TREND_PATH):
            df = pd.read_excel(TREND_PATH)
            if 'data_pubblicazione' in df.columns:
                df['data_pubblicazione'] = pd.to_datetime(df['data_pubblicazione'], errors='coerce')
            return df
    except Exception: pass
    return compute_trend_from_db(load_nc_data())


# ============================================================
# SCHEMA ALIGNMENT HELPERS (NC/AC)
# ============================================================

def _pick_col(df: pd.DataFrame, *names):
    """
    Ritorna la prima colonna presente (case-insensitive).
    Supporta chiamate in 2 modi:
      - _pick_col(df, "a", "b", "c")
      - _pick_col(df, ["a","b","c"])
    """
    # se hanno passato una lista/tuple singola: _pick_col(df, [..])
    if len(names) == 1 and isinstance(names[0], (list, tuple, set)):
        names = tuple(names[0])

    cols = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n is None:
            continue
        key = str(n).lower()
        if key in cols:
            return cols[key]
    return None

def standardize_nc_df(df_nc: pd.DataFrame) -> pd.DataFrame:
    """Ensure df_nc has at least 'nonconformance_number' and 'id' columns.
    This is defensive: it prevents KeyError in list views if backend/header changes."""
    if df_nc is None:
        return pd.DataFrame()
    df = df_nc.copy()
    # Normalize NC number column name
    nc_col = _pick_col(df, ['nonconformance_number','NONCONFORMANCE_NUMBER','NC_NUMBER','numero_nc','nc_number'])
    if nc_col and nc_col != 'nonconformance_number':
        df = df.rename(columns={nc_col: 'nonconformance_number'})
    # Create missing columns if df is empty (no headers) or missing
    if 'nonconformance_number' not in df.columns:
        df['nonconformance_number'] = ''
    # Ensure id exists and is usable as join key with AC.nc_id
    if 'id' not in df.columns or df['id'].astype(str).str.strip().eq('').all():
        df['id'] = df['nonconformance_number'].astype(str).str.strip()
    else:
        df['id'] = df['id'].astype(str).str.strip()
    df['nonconformance_number'] = df['nonconformance_number'].astype(str).str.strip()
    return df

def standardize_ac_df(df_ac: pd.DataFrame) -> pd.DataFrame:
    """Ensure df_ac has 'nc_id' as string if present, and a 'nonconformance_number' display column."""
    if df_ac is None:
        return pd.DataFrame(columns=['id','nc_id'])
    df = df_ac.copy()
    if 'nc_id' in df.columns:
        df['nc_id'] = df['nc_id'].astype(str).str.strip()
    return df

# ============================================================
# VIEWS
# ============================================================

def st_df(df, **kwargs):
    """
    Wrapper compatibile per Streamlit:
    - prova use_container_width se esiste
    - se non esiste lo ignora
    """
    try:
        return st.dataframe(df, use_container_width=True, **kwargs)
    except TypeError:
        return st.dataframe(df, **kwargs)

def view_lista(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header('📋 Lista NC / AC')
    tipo = st.radio('Visualizza:', ('Non Conformità','Azioni Correttive'), horizontal=True)
    if tipo == 'Non Conformità':
        if df_nc.empty:
            st.warning('Nessuna NC presente nel database.')
            return
        df_filt = apply_nc_filters(df_nc.copy())
        base_columns = ['nonconformance_number','nonconformance_status','date_opened','date_closed','responsibility','owner','email_address','nonconformance_source','incident_type','serie','piattaforma','mob','short_description']
        cols = [c for c in base_columns if c in df_filt.columns]
        st_df(df_filt[cols])
    else:
        if df_ac.empty:
            st.warning('Nessuna AC presente nel database.')
            return
        df = _ensure_unique_columns(standardize_ac_df(df_ac.copy()))
        df_nc_std = standardize_nc_df(df_nc)

        # Prova a risalire al numero NC per ogni AC:
        # - prima usando mapping id -> nonconformance_number (se AC.nc_id contiene l'id)
        # - poi fallback: se AC.nc_id contiene già il numero NC, lo copiamo direttamente
        if 'nc_id' in df.columns:
            df['nc_id_str'] = df['nc_id'].astype(str).str.strip()

            # 1) mapping per id (join robusto)
            if 'id' in df_nc_std.columns and 'nonconformance_number' in df_nc_std.columns:
                try:
                    map_by_id = df_nc_std.drop_duplicates(subset=['id']).set_index('id')['nonconformance_number'].astype(str)
                    df['nonconformance_number'] = df['nc_id_str'].map(map_by_id)
                except Exception:
                    # se qualcosa va storto non blocchiamo la UI
                    df['nonconformance_number'] = df.get('nonconformance_number', '')

            # 2) fallback: se nc_id contiene già il numero NC (o il mapping sopra non ha trovato match)
            if 'nonconformance_number' in df_nc_std.columns:
                try:
                    mask_missing = df.get('nonconformance_number')
                    if mask_missing is None:
                        mask_missing = pd.Series(True, index=df.index)
                    else:
                        mask_missing = mask_missing.isna() | (mask_missing.astype(str).str.strip() == "")
                    # qui mappare su se stesso non serve: prendiamo direttamente nc_id_str
                    df.loc[mask_missing, 'nonconformance_number'] = df.loc[mask_missing, 'nc_id_str']
                except Exception:
                    pass

            df.drop(columns=['nc_id_str'], inplace=True, errors='ignore')
        else:
            if 'nonconformance_number' not in df.columns:
                df['nonconformance_number'] = ''
        ac_columns = ['nonconformance_number','ac_corrective_action_num','ac_request_status','ac_request_priority','ac_date_opened','ac_date_required','ac_end_date','ac_owner','ac_email_address','ac_short_description']
        cols = [c for c in ac_columns if c in df.columns]
        st_df(df[cols])

def view_gestione_piattaforme():
    st.header('🧩 Gestione piattaforme')
    platforms = load_platforms()
    if platforms:
        st.subheader('Piattaforme disponibili')
        st_df(pd.DataFrame({'Piattaforma': platforms}), use_container_width=True, hide_index=True)
    else:
        st.info('Nessuna piattaforma ancora definita.')
    st.markdown('---')
    st.subheader('Aggiungi nuova piattaforma')
    new_name = st.text_input('Nuova piattaforma')
    if st.button('➕ Aggiungi piattaforma'):
        if not new_name.strip():
            st.error('Inserisci un nome per la piattaforma.')
        else:
            add_platform(new_name)
            st.success(f"Piattaforma '{new_name}' aggiunta.")

def view_consulta_nc(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header('🔍 Consulta NC')
    if df_nc.empty:
        st.warning('Nessuna NC presente nel database.')
        return
    if 'consulta_mode' not in st.session_state:
        st.session_state['consulta_mode'] = 'list'
        st.session_state['consulta_nc_id'] = None
    mode = st.session_state['consulta_mode']
    selected_id = st.session_state['consulta_nc_id']

    if mode == 'detail' and selected_id is not None:
        row = df_nc[df_nc['id'] == selected_id]
        if row.empty:
            st.error('NC non trovata.')
            st.session_state['consulta_mode'] = 'list'
            st.session_state['consulta_nc_id'] = None
            return
        row = row.iloc[0]
        display_status = get_display_status(row)
        parent_ref = str(row.get('nc_parent_ref') or '').strip()
        nc_id = str(row['id']).strip()
        nc_number = row['nonconformance_number']
        if st.button('⬅ Torna all’elenco'):
            st.session_state['consulta_mode'] = 'list'
            st.session_state['consulta_nc_id'] = None
            st.rerun()
        st.subheader(f"NC {nc_number}")
        st.markdown(f"**Stato:** {render_status_html(display_status)}", unsafe_allow_html=True)
        if parent_ref:
            st.write(f"**Parent NC:** {parent_ref} (questa NC è gestita come figlia)")
        info_cols = [
            ('Numero NC','nonconformance_number'), ('Data apertura','date_opened'), ('Data chiusura','date_closed'),
            ('Serie','serie'), ('Piattaforma','piattaforma'), ('Make/Buy (MOB)','mob'), ('Priorità','nonconform_priority'),
            ('Responsabilità','responsibility'), ('Owner','owner'), ('Email owner','email_address'),
            ('Fonte','nonconformance_source'), ('Tipo incidente','incident_type'),
        ]
        for lbl, col in info_cols:
            if col in row.index:
                st.write(f"**{lbl}:** {row[col]}")
        st.markdown('### Descrizioni NC')
        for lbl, col in [
            ('Short description','short_description'), ('Detailed description','detailed_description'),
            ('Problem description (DET_)','det_problem_description'), ('Cause (DET_CAUSE)','det_cause'), ('Chiusura (DET_CLOSE)','det_close')
        ]:
            if col in row.index and row[col]:
                st.markdown(f"**{lbl}:**")
                st.write(row[col])
        st.markdown('---')
        st.subheader('Azioni Correttive collegate')
        df_ac_nc = df_ac[df_ac.get('nc_id', pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
        df_ac_nc = _ensure_unique_columns(df_ac_nc)
        if df_ac_nc.empty:
            st.info('Nessuna AC collegata a questa NC.')
        else:
            ac_columns = ['ac_corrective_action_num','ac_request_status','ac_request_priority','ac_date_opened','ac_date_required','ac_end_date','ac_owner','ac_email_address','ac_short_description']
            cols = [c for c in ac_columns if c in df_ac_nc.columns]
            st_df(df_ac_nc[cols])
        st.markdown('---')
        c1, c2 = st.columns(2)
        with c1:
            if st.button('🤖 Verifica NC con Gemini'):
                run_gemini_verifica_nc(row, df_ac_nc)
        with c2:
            lang = st.selectbox('Lingua 8D', ['Italiano','English'])
            if st.button('📄 Genera 8D report'):
                run_gemini_8d_report(row, df_ac_nc, lang)
        return

    st.subheader('Elenco NC')
    df_list = df_nc.copy()
    if not df_list.empty:
        df_list['display_status'] = df_list.apply(get_display_status, axis=1)
        counts = df_list['display_status'].value_counts()
        recap = [f"{cnt} in stato {stato}" for stato, cnt in counts.items()]
        st.caption("  •  ".join(recap))
    df_list = df_list.sort_values(['date_opened','nonconformance_number'], ascending=[False, True])
    filtro = st.text_input('Filtro per numero / descrizione / owner:', value='').strip()
    if filtro:
        mask = (
            df_list['nonconformance_number'].astype(str).str.contains(filtro, case=False, na=False)
            | df_list.get('short_description', pd.Series('', index=df_list.index)).astype(str).str.contains(filtro, case=False, na=False)
            | df_list.get('owner', pd.Series('', index=df_list.index)).astype(str).str.contains(filtro, case=False, na=False)
        )
        df_list = df_list[mask]
    if df_list.empty:
        st.info('Nessuna NC corrisponde ai filtri.')
        return
    h = st.columns([1.4,1,1,1,1,3,1])
    h[0].markdown('**Numero NC**'); h[1].markdown('**Stato**'); h[2].markdown('**Data apertura**'); h[3].markdown('**Serie**'); h[4].markdown('**Piattaforma**'); h[5].markdown('**Short description**'); h[6].markdown('**Owner**')
    st.markdown('---')
    for i, (_, r) in enumerate(df_list.iterrows()):
        c1,c2,c3,c4,c5,c6,c7 = st.columns([1.4,1,1,1,1,3,1])
        c1.write(r.get('nonconformance_number',''))
        c2.markdown(render_status_html(r.get('display_status','')), unsafe_allow_html=True)
        c3.write(r.get('date_opened',''))
        c4.write(r.get('serie',''))
        c5.write(r.get('piattaforma',''))
        c6.write(r.get('short_description',''))
        c7.write(r.get('owner',''))
        if c7.button('Dettaglio', key=f"det_nc_{i}_{r.get('id','')}"):
            st.session_state['consulta_mode'] = 'detail'
            st.session_state['consulta_nc_id'] = str(r['id']).strip()
            st.rerun()
        st.markdown('<hr>', unsafe_allow_html=True)


def get_status_options(df_nc: pd.DataFrame):
    if df_nc is None or df_nc.empty or 'nonconformance_status' not in df_nc.columns:
        return ['OPEN','CLOSED','CANCELLED']
    vals = sorted({str(v).strip() for v in df_nc['nonconformance_status'].dropna().tolist() if str(v).strip()})
    base = ['OPEN','CLOSED','CANCELLED']
    for v in vals:
        if v not in base: base.append(v)
    return base

def _truthy_flag(v) -> bool:
    s = str(v or "").strip().upper()
    return s in ("Y","YES","TRUE","1","SI","SÌ")


def get_next_nc_number(df_nc: pd.DataFrame) -> str:
    """Genera il prossimo NONCONFORMANCE_NUMBER nel formato NC-XXXX-CVT.

    La numerazione viene calcolata leggendo il valore più alto esistente nella colonna
    NONCONFORMANCE_NUMBER (rinominata in 'nonconformance_number' dal loader).

    Regole:
    - Considera solo i valori che matchano: NC-<numero>-CVT (case-insensitive)
    - Il numero è formattato a 4 cifre con zeri a sinistra (XXXX)
    - Se non trova nulla, parte da NC-0001-CVT
    """
    try:
        if df_nc is None or df_nc.empty:
            return "NC-0001-CVT"

        col = None
        if "nonconformance_number" in df_nc.columns:
            col = "nonconformance_number"
        elif "NONCONFORMANCE_NUMBER" in df_nc.columns:
            col = "NONCONFORMANCE_NUMBER"
        if col is None:
            return "NC-0001-CVT"

        rx = re.compile(r"^NC-(\d+)-CVT$", re.IGNORECASE)
        max_n = 0
        for v in df_nc[col].dropna().astype(str).str.strip():
            m = rx.match(v)
            if not m:
                continue
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except Exception:
                continue

        next_n = max_n + 1 if max_n > 0 else 1
        return f"NC-{next_n:04d}-CVT"
    except Exception:
        return "NC-0001-CVT"

def get_next_ac_number(df_ac: pd.DataFrame) -> str:
    """Genera il prossimo AC_CORRECTIVE_ACTION_NUM nel formato AC-XXXX-CVT.

    Legge il valore più alto esistente nella colonna AC_CORRECTIVE_ACTION_NUM
    (rinominata in 'ac_corrective_action_num' dal loader).

    Regole:
    - Considera solo valori che matchano: AC-<numero>-CVT (case-insensitive)
    - Numero a 4 cifre (XXXX)
    - Se non trova nulla, parte da AC-0001-CVT
    """
    try:
        if df_ac is None or df_ac.empty:
            return "AC-0001-CVT"

        col = None
        if "ac_corrective_action_num" in df_ac.columns:
            col = "ac_corrective_action_num"
        elif "AC_CORRECTIVE_ACTION_NUM" in df_ac.columns:
            col = "AC_CORRECTIVE_ACTION_NUM"
        if col is None:
            return "AC-0001-CVT"

        rx = re.compile(r"^AC-(\d+)-CVT$", re.IGNORECASE)
        max_n = 0
        for v in df_ac[col].dropna().astype(str).str.strip():
            m = rx.match(v)
            if not m:
                continue
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except Exception:
                continue

        next_n = max_n + 1 if max_n > 0 else 1
        return f"AC-{next_n:04d}-CVT"
    except Exception:
        return "AC-0001-CVT"

def render_nc_form(df_nc: pd.DataFrame, defaults: dict | None = None, mode: str = "create") -> dict:
    """Rende la UI della NC (create/edit) e ritorna un dict pronto per il backend."""
    defaults = defaults or {}
    today = date.today()

    def D(key, fallback=""):
        v = defaults.get(key, fallback)
        return "" if v is None else v

    all_status = get_status_options(df_nc) if df_nc is not None and not df_nc.empty else ['OPEN','CLOSED','CANCELLED']
    status_options = [s for s in all_status if s.upper() != 'CANCELLED'] or ['OPEN','CLOSED']

    # Numero NC
    if mode == "create":
        nc_number = get_next_nc_number(df_nc)
        date_opened = today
        cur_status = 'OPEN'
    else:
        nc_number = str(D('nonconformance_number', D('id',''))).strip()
        date_opened = safe_date_for_input(D('date_opened')) or today
        cur_status = str(D('nonconformance_status','OPEN')).strip() or 'OPEN'
        if cur_status not in status_options:
            status_options = [cur_status] + status_options

    st.subheader("1) Identità NC")
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
    with c1:
        st.text_input("Numero NC", value=nc_number, disabled=True)
    with c2:
        status = st.selectbox("Stato NC", options=status_options, index=status_options.index(cur_status))
    with c3:
        pr_opts = ["HIGH", "MEDIUM", "LOW"]
        cur_pr = str(D('nonconform_priority', '')).strip().upper()
        if cur_pr not in pr_opts:
            cur_pr = "MEDIUM"
        nonconform_priority = st.selectbox("Priorità NC", options=pr_opts, index=pr_opts.index(cur_pr))
    with c4:
        st.text_input("Data apertura", value=str(date_opened), disabled=True)

    # Parent NC (nascosto in UI)
    is_child = False
    nc_parent_ref = ""

    st.subheader("2) Contesto prodotto")
    a1, a2, a3, a4 = st.columns([1, 1, 1, 1])
    with a1:
        serie = st.text_input("Serie *", value=str(D('serie','')))
    with a2:
        grandezza = st.text_input("Grandezza", value=str(D('grandezza','')))
    with a3:
        item_instance_serial = st.text_input("Matricola / Serial", value=str(D('item_instance_serial','')))
    with a4:
        mob_cur = (str(D('mob','')) or '').strip()
        mob = st.selectbox("Make/Buy (MOB)", ['Make','Buy'], index=0 if mob_cur != 'Buy' else 1)

    b1 = st.columns([1])[0]
    with b1:
        platforms = load_platforms()
        if platforms:
            cur_pl = str(D('piattaforma', platforms[0] if platforms else '')).strip()
            idx = platforms.index(cur_pl) if cur_pl in platforms else 0
            piattaforma = st.selectbox("Piattaforma *", options=platforms, index=idx)
        else:
            piattaforma = st.text_input("Piattaforma * (nessuna piattaforma definita)", value=str(D('piattaforma','')))

    # campi nascosti ma lasciamo variabili “vuote” per compatibilità payload
    macro_piattaforma = None
    item_id = None
    item = None
    item_desc = None

    c5, c6 = st.columns([1.2, 2.8])
    with c5:
        item = st.text_input("Item", value=str(D('item','')))
    with c6:
        item_desc = st.text_input("Item descrizione", value=str(D('item_desc','')))

    st.subheader("3) Origine evento")

    src_opts = ["Service - IService", "Complain"]
    cur_src = str(D('nonconformance_source', '')).strip()
    if cur_src not in src_opts:
        # default sensato
        cur_src = "Service - IService"

    nonconformance_source = st.selectbox("Fonte (source)", options=src_opts, index=src_opts.index(cur_src))

    # Incident type + Service request: SOLO per Service
    incident_type = None
    service_request = None
    if nonconformance_source == "Service - IService":
        o2, o3 = st.columns(2)
        with o2:
            incident_type = st.text_input("Incident type", value=str(D('incident_type','')) or "SERVICE")
        with o3:
            service_request = st.text_input("Service request / Ticket", value=str(D('service_request','')))
    else:
        st.caption("Fonte = Complain: Incident type e Service request non richiesti.")

    # campi rimossi (nascosti)
    supplier = None
    quantity_nonconforming = None
    nonconforming_uom = None

    st.subheader("4) Owner e descrizione")
    h1, h2, h3 = st.columns([1.2, 1.2, 1.0])
    with h1:
        owner = st.text_input("Owner NC", value=str(D('owner','')))
    with h2:
        email_address = st.text_input("Email owner", value=str(D('email_address','')))
    with h3:
        responsibility = st.text_input("Responsabilità", value=str(D('responsibility','')))

    short_description = st.text_input("Short description *", value=str(D('short_description','')))
    detailed_description = st.text_area("Descrizione dettagliata", value=str(D('detailed_description','')), height=140)

    st.subheader("5) Dettagli di chiusura (Analisi DET_*)")
    det_problem_description = st.text_area("Problem description (DET_*)", value=str(D('det_problem_description','')), height=110)
    det_cause = st.text_area("Cause (DET_CAUSE)", value=str(D('det_cause','')), height=110)
    det_close = st.text_area("Chiusura (DET_CLOSE)", value=str(D('det_close','')), height=110)

    # autocorrezione email da owner
    owner_clean = owner.strip()
    email_clean = str(email_address or '').strip()
    if not email_clean and owner_clean:
        sug = suggest_email_from_name(owner_clean)
        if sug:
            email_clean = sug

    vals = {
        'nonconformance_number': nc_number,
        'date_opened': date_opened.isoformat() if date_opened else None,
        'nonconformance_status': (status or '').strip() or None,
        'nonconform_priority': (nonconform_priority or '').strip() or None,

        'nc_parent_y_n': 'N',
        'nc_parent_ref': None,

        'serie': (serie or '').strip(),
        'grandezza': (grandezza or '').strip() or None,
        'item_instance_serial': (item_instance_serial or '').strip() or None,
        'mob': mob,

        'piattaforma': (piattaforma or '').strip(),
        'macro_piattaforma': None,
        'item_id': None,
        'item': None,
        'item_desc': None,

        'nonconformance_source': (nonconformance_source or '').strip() or None,
        'incident_type': (incident_type or '').strip() or None,
        'service_request': (service_request or '').strip() or None,
        'supplier': None,
        'quantity_nonconforming': None,
        'nonconforming_uom': None,

        'responsibility': (responsibility or '').strip() or None,
        'owner': owner_clean or None,
        'email_address': email_clean or None,

        'short_description': (short_description or '').strip(),
        'detailed_description': detailed_description or None,

        'det_problem_description': det_problem_description or None,
        'det_cause': det_cause or None,
        'det_close': det_close or None,
    }
    return vals

def render_ac_form(defaults: dict | None = None, mode: str = "create", proposed_code: str | None = None, key_prefix: str = "ac") -> dict:
    """Rende la UI della AC (create/edit) e ritorna un dict pronto per il backend.

    Nota Streamlit: i widget mantengono stato in session_state. Per ricaricare correttamente i valori
    quando si cambia AC, ogni widget deve avere una key univoca (key_prefix).
    """
    defaults = defaults or {}
    today = date.today()

    def D(key, fallback=""):
        v = defaults.get(key, fallback)
        return "" if v is None else v

    def K(name: str) -> str:
        return f"{key_prefix}__{name}"

    st.subheader("1) Identità AC")
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
    with c1:
        ac_num_val = str(D('ac_corrective_action_num', D('ac_number',''))).strip() or (proposed_code or "")
        st.text_input("AC number", value=ac_num_val, disabled=(mode=="edit" or bool(proposed_code)), key=K("ac_num_display"))
    with c2:
        cur_st = (str(D('ac_request_status', 'OPEN')) or 'OPEN').strip().upper()
        status_opts = ["OPEN", "CLOSED"]
        if cur_st not in status_opts:
            status_opts = [cur_st] + status_opts  # mantiene eventuali valori legacy
        ac_request_status = st.selectbox("Stato AC", options=status_opts, index=status_opts.index(cur_st), key=K("ac_request_status"))
    with c3:
        AC_PRIORITIES = ["HIGH", "MEDIUM", "LOW"]
        cur_pr = str(D('ac_request_priority', 'MEDIUM') or 'MEDIUM').strip().upper()
        if cur_pr not in AC_PRIORITIES:
            AC_PRIORITIES = [cur_pr] + AC_PRIORITIES
        ac_request_priority = st.selectbox(
            "Priorità AC",
            options=AC_PRIORITIES,
            index=AC_PRIORITIES.index(cur_pr),
            key=K("ac_request_priority")
        )
    with c4:
        # campo solo display; la data apertura vera viene impostata in creazione lato chiamante
        st.text_input("Data apertura", value=str(D('ac_date_opened', today)), disabled=True, key=K("ac_date_opened_display"))

    # Date con calendario
    d1, d2, d3 = st.columns(3)
    with d1:
        v = safe_date_for_input(D('ac_date_required')) or today
        ac_date_required = st.date_input("Data richiesta", value=v, key=K("ac_date_required"))
    with d2:
        v = safe_date_for_input(D('ac_end_date')) or today
        ac_end_date = st.date_input("Data fine", value=v, key=K("ac_end_date"))
    with d3:
        v = safe_date_for_input(D('ac_follow_up_date')) or today
        ac_follow_up_date = st.date_input("Follow-up", value=v, key=K("ac_follow_up_date"))

    st.subheader("2) Owner / email / source")
    o1, o2, o3, o4 = st.columns([1.2, 1.4, 0.9, 1.2])

    with o2:
        ac_email_address = st.text_input("Email owner", value=str(D('ac_email_address','')), key=K("ac_email_address"))
  
    with o4:
        ac_requestor = st.text_input("Requestor", value=str(D('ac_requestor','')), key=K("ac_requestor"))

    ac_short_description = st.text_input("Short description *", value=str(D('ac_short_description','')), key=K("ac_short_description"))
    ac_detailed_description = st.text_area("Descrizione dettagliata", value=str(D('ac_detailed_description','')), height=140, key=K("ac_detailed_description"))

    st.subheader("3) Classificazione / tipo")
    s1, s2, s3 = st.columns(3)
    with s1:
        ac_request_source = st.text_input("Request source", value=str(D('ac_request_source','')), key=K("ac_request_source"))
    with s2:
        ac_implementation_type = st.text_input("Implementation type", value=str(D('ac_implementation_type','')), key=K("ac_implementation_type"))
    with s3:
        ac_car_class = st.text_input("CAR class", value=str(D('ac_car_class','')), key=K("ac_car_class"))

    st.subheader("4) Costi / piattaforma")
    k1, k2 = st.columns(2)
    with k1:
        ac_cost_smry_internal = st.text_input("Costo interno (smry)", value=str(D('ac_cost_smry_internal','')), key=K("ac_cost_smry_internal"))
    with k2:
        new_macro_piattaforma = st.text_input("New macro piattaforma", value=str(D('new_macro_piattaforma','')), key=K("new_macro_piattaforma"))

    st.subheader("5) Chiusura AC")
    ac_effective = st.text_area("Efficacia (AC_EFFECTIVE)", value=str(D('ac_effective','')), height=90, key=K("ac_effective"))
    ac_evidence_verify = st.text_area("Evidenze (AC_EVIDENCE_VERIFY)", value=str(D('ac_evidence_verify','')), height=90, key=K("ac_evidence_verify"))

    # autocorrezione email da owner
    ac_owner_val = str(D('ac_owner', D('AC_OWNER', ''))).strip()
    owner_clean = ac_owner_val
    email_clean = str(ac_email_address or '').strip()
    if not email_clean and owner_clean:
        sug = suggest_email_from_name(owner_clean)
        if sug:
            email_clean = sug
    vals = {
        'ac_corrective_action_num': ac_num_val.strip() or None,
        'ac_request_status': (ac_request_status or '').strip() or None,
        'ac_request_priority': (ac_request_priority or '').strip() or None,

        'ac_date_required': ac_date_required.isoformat() if ac_date_required else None,
        'ac_end_date': ac_end_date.isoformat() if ac_end_date else None,
        'ac_follow_up_date': ac_follow_up_date.isoformat() if ac_follow_up_date else None,

        'ac_requestor': (ac_requestor or '').strip() or None,
        'ac_owner': owner_clean or None,
        'ac_email_address': email_clean or None,
        'ac_send_email': 'N',

        'ac_short_description': (ac_short_description or '').strip(),
        'ac_detailed_description': ac_detailed_description or None,

        'ac_request_source': (ac_request_source or '').strip() or None,
        'ac_implementation_type': (ac_implementation_type or '').strip() or None,
        'ac_car_class': (ac_car_class or '').strip() or None,

        'ac_cost_smry_internal': (ac_cost_smry_internal or '').strip() or None,
        'new_macro_piattaforma': (new_macro_piattaforma or '').strip() or None,

        'ac_effective': ac_effective or None,
        'ac_evidence_verify': ac_evidence_verify or None,
    }
    return vals


def view_modifica_nc(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header('✏️ Modifica NC / AC')
    if df_nc.empty:
        st.warning('Nessuna NC presente nel database.')
        return

    nc_numbers = sorted(df_nc['nonconformance_number'].dropna().astype(str).unique().tolist())
    selected_nc = st.selectbox('Seleziona NC', nc_numbers)
    row_df = df_nc[df_nc['nonconformance_number'].astype(str) == str(selected_nc)]
    if row_df.empty:
        st.error('NC non trovata.')
        return
    row = row_df.iloc[0]
    nc_id = str(row.get('id') or row.get('nonconformance_number') or '').strip()

    st.subheader('Dati NC')
    with st.form(key='form_modifica_nc_levels'):
        vals = render_nc_form(df_nc=df_nc, defaults=row.to_dict(), mode="edit")
        submitted_nc = st.form_submit_button('💾 Salva modifiche NC')

    if submitted_nc:
        errors = []
        if not (vals.get('serie') or '').strip(): errors.append('SERIE è obbligatoria.')
        if not (vals.get('piattaforma') or '').strip(): errors.append('PIATTAFORMA è obbligatoria.')
        if not (vals.get('short_description') or '').strip(): errors.append('SHORT_DESCRIPTION è obbligatoria.')
        if errors:
            for e in errors: st.error(e)
        else:
            # in modifica non vogliamo cambiare il numero NC
            vals_patch = dict(vals)
            vals_patch.pop('nonconformance_number', None)
            update_nc_in_db(nc_id, vals_patch)
            st.success('NC aggiornata con successo.')
            trigger_email_prompt(nc_id, 'Modifica dati NC', default_to=str(vals.get('email_address','')))

    st.markdown('---')
    st.subheader('Azioni Correttive (AC)')

    df_ac_nc = df_ac[df_ac.get('nc_id', pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
    df_ac_nc = _ensure_unique_columns(df_ac_nc)

    ac_labels = []
    if not df_ac_nc.empty:
        for _, r in df_ac_nc.iterrows():
            ac_labels.append(f"{r.get('ac_corrective_action_num','')} - {str(r.get('ac_short_description',''))[:80]}")
    if ac_labels:
        pick = st.selectbox('Seleziona AC da modificare', ac_labels)
        idx = ac_labels.index(pick)
        ac_row = df_ac_nc.iloc[idx]
        ac_id = str(ac_row.get('id') or ac_row.get('ac_corrective_action_num') or '').strip()
        st.caption(f"NC: {row.get('nonconformance_number')}  •  AC: {ac_row.get('ac_corrective_action_num')}")
        with st.form(key='form_modifica_ac_levels'):
            vals_ac = render_ac_form(defaults=ac_row.to_dict(), mode="edit", key_prefix=f"ac_edit_{ac_id}")
            upd = st.form_submit_button('💾 Salva modifiche AC')
        if upd:
            if not (vals_ac.get('ac_short_description') or '').strip():
                st.error('Short description AC è obbligatoria.')
            else:
                # non cambiare codice AC in modifica
                vals_ac_patch = dict(vals_ac)
                vals_ac_patch.pop('ac_corrective_action_num', None)
                update_ac_in_db(nc_id, ac_id, vals_ac_patch)
                st.success('AC aggiornata con successo.')
                trigger_email_prompt(nc_id, f"Modifica AC {ac_row.get('ac_corrective_action_num')}", default_to=str(vals_ac.get('ac_email_address','')))
    else:
        st.info('Nessuna AC collegata a questa NC.')

    st.markdown('---')
    st.subheader('➕ Aggiungi nuova AC per questa NC')

    df_ac_all = _ensure_unique_columns(load_ac_data())
    ac_code = get_next_ac_number(df_ac_all)
    st.info(f"Nuovo numero AC proposto: **{ac_code}**")

    with st.form(key='form_inserisci_ac_levels'):
        vals_new = render_ac_form(defaults={}, mode="create", proposed_code=ac_code, key_prefix=f"ac_new_{nc_id}")
        submit_new = st.form_submit_button('💾 Crea nuova AC')

    if submit_new:
        errors = []
        if not (vals_new.get('ac_short_description') or '').strip():
            errors.append('Short description AC è obbligatoria.')
        if errors:
            for e in errors: st.error(e)
        else:
            # completa payload richiesto dal backend
            payload = dict(vals_new)
            payload['id'] = ac_code
            payload['ac_corrective_action_num'] = ac_code
            payload.setdefault('ac_request_status', 'OPEN')
            payload['ac_date_opened'] = date.today().isoformat()
            if not payload.get('ac_date_required'):
                payload['ac_date_required'] = date.today().isoformat()
            insert_ac_in_db(nc_id, payload)
            st.success(f"AC {ac_code} creata con successo per la NC {selected_nc}.")
            trigger_email_prompt(nc_id, f"Nuova AC {ac_code} creata", default_to=str(vals_new.get('ac_email_address','')))

def view_inserisci_nc(df_nc: pd.DataFrame):
    st.header('➕ Inserisci nuova NC')
    with st.form(key='form_inserisci_nc_levels'):
        vals = render_nc_form(df_nc=df_nc, defaults={}, mode="create")
        submitted = st.form_submit_button('💾 Crea NC')

    if submitted:
        errors = []
        if not (vals.get('serie') or '').strip(): errors.append('SERIE è obbligatoria.')
        if not (vals.get('piattaforma') or '').strip(): errors.append('PIATTAFORMA è obbligatoria.')
        if not (vals.get('short_description') or '').strip(): errors.append('SHORT_DESCRIPTION è obbligatoria.')
        if errors:
            for e in errors: st.error(e)
            return

        nc_number = vals.get('nonconformance_number')
        payload = dict(vals)
        payload['id'] = nc_number
        payload['nonconformance_number'] = nc_number
        nc_id = insert_nc_in_db(payload)
        st.success(f"NC {nc_number} creata con successo.")
        trigger_email_prompt(nc_id, 'Nuova NC creata', default_to=str(vals.get('email_address','')))

# =========================
# Trend NC Quality (SERVICE) - pandas + plotly
# =========================



def _to_dt_series(s: pd.Series) -> pd.Series:
    """Datetime robusto: vuoti/errori -> NaT."""
    return pd.to_datetime(s, errors="coerce")

def _norm_mob(x) -> str:
    v = str(x or "").strip().title()
    if v in ("Make", "Buy"):
        return v
    return "Unknown"

def _build_week_index(last_years: int = 2) -> pd.DatetimeIndex:
    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=365 * last_years)
    return pd.date_range(start=start, end=end, freq="W-MON")  # settimana ancorata al lunedì

def _prepare_nc_trend_df(df_nc: pd.DataFrame) -> pd.DataFrame:
    df = df_nc.copy()

    c_open = _pick_col(df, "DATE_OPENED", "date_opened")
    c_close = _pick_col(df, "DATE_CLOSED", "date_closed")
    c_mob = _pick_col(df, "MOB", "mob")
    c_src = _pick_col(df, "NONCONFORMANCE_SOURCE", "nonconformance_source")

    missing = [x for x in [("DATE_OPENED", c_open), ("DATE_CLOSED", c_close), ("MOB", c_mob), ("NONCONFORMANCE_SOURCE", c_src)] if x[1] is None]
    if missing:
        need = ", ".join([m[0] for m in missing])
        st.error(f"Trend NC Quality: colonne mancanti nel dataset NC: {need}")
        return pd.DataFrame()

    df["_opened"] = _to_dt_series(df[c_open])
    df["_closed"] = _to_dt_series(df[c_close])
    df["_mob"] = df[c_mob].apply(_norm_mob)
    df["_source"] = df[c_src].astype(str).str.strip().str.upper()

    # usa solo SERVICE
    df = df[df["_source"] == "SERVICE"].copy()
    df = df[df["_opened"].notna()].copy()

    return df

def _compute_flow(df: pd.DataFrame, weeks: pd.DatetimeIndex, window_weeks: int = 8) -> pd.DataFrame:
    """
    Flow: per ogni settimana W calcola:
      - Started_8w = aperte (DATE_OPENED) nelle ultime 8 settimane
      - Closed_8w  = chiuse (DATE_CLOSED) nelle ultime 8 settimane
    per Make, Buy e Total
    """
    out = []
    mobs = ["Total", "Make", "Buy"]

    for mob in mobs:
        d = df if mob == "Total" else df[df["_mob"] == mob]
        opened = d["_opened"].dropna().sort_values()
        closed = d["_closed"].dropna().sort_values()

        for w in weeks:
            w_start = w - pd.Timedelta(weeks=window_weeks) + pd.Timedelta(days=1)
            started_8w = int(((opened >= w_start) & (opened <= w)).sum())
            closed_8w = int(((closed >= w_start) & (closed <= w)).sum())
            out.append({"week": w, "MOB": mob, "Started_8w": started_8w, "Closed_8w": closed_8w})

    return pd.DataFrame(out)

def _compute_stock(df: pd.DataFrame, weeks: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Stock: NC ancora aperte alla settimana W se:
      opened <= W AND (closed is null OR closed > W)
    Output long: week, MOB, OpenStock
    """
    rows = []
    for w in weeks:
        opened_ok = df["_opened"] <= w
        not_closed = df["_closed"].isna() | (df["_closed"] > w)
        is_open = opened_ok & not_closed

        snap = df[is_open].groupby("_mob").size().reindex(["Make", "Buy"], fill_value=0)
        rows.append({"week": w, "Make": int(snap["Make"]), "Buy": int(snap["Buy"])})

    stock = pd.DataFrame(rows)
    return stock.melt(id_vars=["week"], value_vars=["Make", "Buy"], var_name="MOB", value_name="OpenStock")

def _plot_flow(flow_df: pd.DataFrame) -> go.Figure:
    mobs = ["Total", "Make", "Buy"]

    fig = go.Figure()
    for mob in mobs:
        d = flow_df[flow_df["MOB"] == mob].sort_values("week")
        fig.add_trace(go.Scatter(
            x=d["week"], y=d["Started_8w"], mode="lines+markers",
            name=f"{mob} - Started (8w)", visible=(mob == "Total")
        ))
        fig.add_trace(go.Scatter(
            x=d["week"], y=d["Closed_8w"], mode="lines+markers",
            name=f"{mob} - Closed (8w)", visible=(mob == "Total")
        ))

    buttons = []
    for i, mob in enumerate(mobs):
        vis = [False] * (2 * len(mobs))
        vis[2*i] = True
        vis[2*i + 1] = True
        buttons.append(dict(
            label=mob, method="update",
            args=[{"visible": vis}, {"title": f"Flow (Started vs Closed) – {mob} – finestra 8 settimane"}]
        ))

    fig.update_layout(
        title="Flow (Started vs Closed) – Total – finestra 8 settimane",
        xaxis_title="Settimana",
        yaxis_title="Conteggio NC (8 settimane mobili)",
        updatemenus=[dict(type="dropdown", x=1.02, y=1.0, xanchor="left", yanchor="top", buttons=buttons)],
        legend_title="Serie",
        hovermode="x unified",
        margin=dict(l=40, r=230, t=70, b=40),
    )
    return fig

def _plot_stock(stock_long: pd.DataFrame):
    fig = px.area(
        stock_long.sort_values("week"),
        x="week", y="OpenStock", color="MOB",
        title="Stock (Still Open) – NC ancora aperte",
        labels={"week": "Settimana", "OpenStock": "NC aperte", "MOB": "MOB"},
    )
    fig.update_layout(hovermode="x unified")
    return fig


def view_trend_nc_quality_db(df_nc: pd.DataFrame):
    st.header("📈 Trend NC Quality")

    df = _prepare_nc_trend_df(df_nc)
    if df.empty:
        st.warning("Nessun dato disponibile per Trend NC Quality (filtro: NONCONFORMANCE_SOURCE = SERVICE).")
        return

    weeks = _build_week_index(last_years=2)

    flow_df = _compute_flow(df, weeks, window_weeks=8)
    stock_long = _compute_stock(df, weeks)

    st.caption("Dati dal Google Sheet NC. Asse X settimanale (ultimi 2 anni). Legenda cliccabile per isolare serie.")

    fig1 = _plot_flow(flow_df)
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = _plot_stock(stock_long)
    st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# MAIN
# ============================================================

def main():
    st.set_page_config(page_title='Gestione Non Conformità (v19-fixed6)', layout='wide')
    st.sidebar.title('Menu')
    scelta = st.sidebar.radio('Seleziona funzione', ('1) Lista NC/AC','2) Consulta NC','3) Modifica NC/AC','4) Inserisci NC','5) Trend NC Quality','6) Gestione piattaforme'))
    st.caption('Backend: Google Sheets (Apps Script)')

    import time
    t0 = time.perf_counter(); st.info('⏳ Carico dati dal backend…')
    try:
        df_nc = load_nc_data(); df_ac = load_ac_data()
    except Exception as e:
        st.error('Errore caricamento dati dal backend'); st.exception(e); st.stop()
    finally:
        dt = time.perf_counter() - t0; st.sidebar.write(f"⏱️ Load time: {dt:.2f}s")

    with st.sidebar.expander('🧭 Healthcheck', expanded=True):
        st.write('DATA URL:', DATA_SCRIPT_URL); st.write('MAIL URL:', MAIL_SCRIPT_URL or '(= DATA URL)')
        st.write('NC rows:', 0 if df_nc is None else len(df_nc)); st.write('AC rows:', 0 if df_ac is None else len(df_ac))
        if df_nc is not None and not df_nc.empty: st.write('NC cols:', list(df_nc.columns)[:15], '...' if len(df_nc.columns)>15 else '')
        if df_ac is not None and not df_ac.empty: st.write('AC cols:', list(df_ac.columns)[:15], '...' if len(df_ac.columns)>15 else '')
