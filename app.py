# app_v19_fixed6.py
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
SMTP_SERVER     = secret_any(("mail","smtp_server"), "SMTP_SERVER", default="")
SMTP_PORT       = int(secret_any(("mail","smtp_port"), "SMTP_PORT", default="0") or 0)
SMTP_USER       = secret_any(("mail","username"), "SMTP_USER", default="")
SMTP_PASSWORD   = secret_any(("mail","password"), "SMTP_PASSWORD", default="")
TREND_PATH      = secret_any("TREND_PATH", default=r"P:\\QA\\007 Validazione Prodotti\\11 Non conformità\\Trend _NC Quality_.xlsx")

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
    data = _remove_duplicate_keys(data)
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

    if 'id' not in df.columns and 'nonconformance_number' in df.columns:
        df['id'] = df['nonconformance_number'].astype(str).str.strip()

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
    oracle_ac_map = {
        'ID':'id', 'NC_ID':'nc_id',
        'AC_CORRECTIVE_ACTION_NUM':'ac_corrective_action_num',
        'AC_REQUEST_STATUS':'ac_request_status',
        'AC_REQUEST_PRIORITY':'ac_request_priority',
        'AC_DATE_OPENED':'ac_date_opened',
        'AC_DATE_REQUIRED':'ac_date_required',
        'AC_END_DATE':'ac_end_date',
        'AC_FOLLOW_UP_DATE':'ac_follow_up_date',
        'AC_OWNER':'ac_owner',
        'AC_EMAIL_ADDRESS':'ac_email_address',
        'AC_SHORT_DESCRIPTION':'ac_short_description',
        'AC_DETAILED_DESCRIPTION':'ac_detailed_description',
        'AC_EFFECTIVE':'ac_effective',
        'AC_EVIDENCE_VERIFY':'ac_evidence_verify',
        'PIATTAFORMA':'piattaforma',
    }
    ren = {k:v for k,v in oracle_ac_map.items() if k in df.columns}
    if ren:
        df = df.rename(columns=ren)
    for col in ['ac_date_opened','ac_date_required','ac_end_date','ac_follow_up_date','created_at','updated_at']:
        if col in df.columns:
            df[col] = df[col].apply(_parse_any_date)
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
    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_js = json.dumps(payload_json).replace("</", "<\\/")
    tmpl_str = r"""
<div id="$key_wrap"></div>
<iframe name="$key_frame" style="display:none;"></iframe>
<form id="$key_form" action="$script_url" method="POST" target="$key_frame">
  <input type="hidden" name="op" value="send_mail" />
  <input type="hidden" name="mode" value="iframe" />
  <input type="hidden" name="payload_b64" id="$key_payload_b64" value="" />
</form>
<script>
(function(){
  const payload = $payload_js;
  function toB64Unicode(str){ return btoa(unescape(encodeURIComponent(str))); }
  const wrap = document.getElementById("$key_wrap");
  wrap.innerHTML = '<div style="padding:6px 0;">\ud83d\udce7 Invio email in corso...</div>';
  function onMsg(ev){
    try{
      const d = ev.data; if(!d || (typeof d !== 'object')) return;
      if(d.ok){ wrap.innerHTML = '<div style="padding:6px 0; color:green;">✅ Email inviata.</div>'; }
      else     { wrap.innerHTML = '<div style="padding:6px 0; color:red;">❌ Errore invio email: ' + (d.error || 'sconosciuto') + '</div>'; }
    }catch(e){}
    window.removeEventListener("message", onMsg);
  }
  window.addEventListener("message", onMsg);
  document.getElementById("$key_payload_b64").value = toB64Unicode(payload);
  document.getElementById("$key_form").submit();
})();
</script>
"""
    html = Template(tmpl_str).substitute(script_url=script_url, key=key, payload_js=payload_js)
    components.html(html, height=60)

# SMTP fallback

def send_email(to_addresses, subject, body):
    if isinstance(to_addresses, str):
        to_addresses = [to_addresses]
    if not to_addresses:
        return
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ", ".join(to_addresses)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
            server.starttls(); server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_addresses, msg.as_string())
    except Exception as e:
        try: st.error(f"Errore nell'invio della mail: {e}")
        except Exception: print("Errore nell'invio della mail:", e)

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

def trigger_email_prompt(nc_id: str, operation: str):
    st.session_state['email_nc_id'] = nc_id
    st.session_state['email_operation'] = operation
    st.session_state['show_email_prompt'] = True

def render_email_prompt():
    if not st.session_state.get('show_email_prompt'):
        return
    nc_id = st.session_state.get('email_nc_id')
    operation = st.session_state.get('email_operation', 'Aggiornamento NC')
    try:
        data = _api_get('get_nc', id=str(nc_id))
        nc_number = str(data.get('nonconformance_number', nc_id)) if isinstance(data, dict) else str(nc_id)
    except Exception:
        nc_number = str(nc_id)
    emails = get_emails_for_nc(nc_id) if nc_id is not None else []
    safe_op = re.sub(r"[^A-Za-z0-9]+", "_", str(operation))
    ctx = f"{nc_id}_{safe_op}"
    yes_key = f"email_send_yes_{ctx}"; no_key = f"email_send_no_{ctx}"

    st.markdown('---')
    st.subheader('Inviare le modifiche agli owner?')
    st.write(f"Vuoi inviare una mail agli owner della NC **{nc_number}**?")
    # Campo destinatari precompilato con elenco o suggerimento da owner
    nc_details = get_nc_details(nc_id) if nc_id is not None else {}
    owner_name = (nc_details or {}).get('owner') or ''
    suggested = suggest_email_from_name(owner_name) if owner_name else ''
    prefill = ', '.join(emails) if emails else (suggested or '')
    recipients_input = st.text_input('Destinatari (separati da ,)', value=prefill)
    if not recipients_input.strip():
        st.info("Inserisci almeno un destinatario per inviare l'email.")
    c1, c2 = st.columns(2)
    with c1:
        if st.button('✉️ Sì, invia', key=yes_key):
            to_value = recipients_input.strip()
            if to_value:
                nc = get_nc_details(nc_id)
                ac_list = get_ac_details_for_nc(nc_id)
                action = _operation_to_action(operation)
                subject_val = _pick_first(nc, ['subject','oggetto','incident_type','nonconformance_source']) if nc else ''
                payload_key = f"email_payload_{ctx}"
                st.session_state[payload_key] = {
                    'action': action,
                    'to': to_value,  # usa i destinatari inseriti
                    'nc': {
                        'nonconformance_number': (nc or {}).get('nonconformance_number', nc_number),
                        'subject': subject_val,
                        'short_description': (nc or {}).get('short_description',''),
                        'opened_by': (nc or {}).get('created_by','') or (nc or {}).get('owner',''),
                        'responsibility': (nc or {}).get('responsibility',''),
                        'nonconformance_status': (nc or {}).get('nonconformance_status',''),
                        'piattaforma': (nc or {}).get('piattaforma',''),
                        'mob': (nc or {}).get('mob',''),
                    },
                    'ac_list': [
                        {
                            'ac_corrective_action_num': a.get('ac_corrective_action_num',''),
                            'ac_short_description': a.get('ac_short_description',''),
                            'ac_owner': a.get('ac_owner',''),
                            'ac_request_status': a.get('ac_request_status',''),
                        } for a in (ac_list or [])
                    ],
                }
            else:
                st.warning('Nessun indirizzo email: impossibile inviare.')
        payload_key = f"email_payload_{ctx}"
        if st.session_state.get(payload_key):
            send_mail_via_hidden_iframe(MAIL_SCRIPT_URL or DATA_SCRIPT_URL, st.session_state[payload_key], key=f"mail_{ctx}")
        if st.button('✅ Chiudi', key=f"email_close_{ctx}"):
            st.session_state['show_email_prompt'] = False
            st.session_state.pop(payload_key, None)
            st.rerun()
    with c2:
        if st.button('❌ No, non inviare', key=no_key):
            st.session_state['show_email_prompt'] = False

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
# VIEWS
# ============================================================

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
        st.dataframe(df_filt[cols], use_container_width=True)
    else:
        if df_ac.empty:
            st.warning('Nessuna AC presente nel database.')
            return
        df = _ensure_unique_columns(df_ac.copy())
        if 'nc_id' in df.columns:
            df['nc_id_str'] = df['nc_id'].astype(str).str.strip()
            if 'id' in df_nc.columns and 'nonconformance_number' in df_nc.columns:
                map_by_id = df_nc.drop_duplicates(subset=['id']).set_index('id')['nonconformance_number'].astype(str)
                df['nonconformance_number'] = df['nc_id_str'].map(map_by_id)
            if 'nonconformance_number' in df_nc.columns:
                mask_missing = df.get('nonconformance_number').isna() if 'nonconformance_number' in df.columns else pd.Series(True, index=df.index)
                map_by_num = df_nc.drop_duplicates(subset=['nonconformance_number']).set_index('nonconformance_number')['nonconformance_number']
                df.loc[mask_missing, 'nonconformance_number'] = df.loc[mask_missing, 'nc_id_str'].map(map_by_num)
            df.drop(columns=['nc_id_str'], inplace=True, errors='ignore')
        else:
            if 'nonconformance_number' not in df.columns:
                df['nonconformance_number'] = ''
        ac_columns = ['nonconformance_number','ac_corrective_action_num','ac_request_status','ac_request_priority','ac_date_opened','ac_date_required','ac_end_date','ac_owner','ac_email_address','ac_short_description']
        cols = [c for c in ac_columns if c in df.columns]
        st.dataframe(df[cols], use_container_width=True)

def view_gestione_piattaforme():
    st.header('🧩 Gestione piattaforme')
    platforms = load_platforms()
    if platforms:
        st.subheader('Piattaforme disponibili')
        st.dataframe(pd.DataFrame({'Piattaforma': platforms}), use_container_width=True, hide_index=True)
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
            st.dataframe(df_ac_nc[cols], use_container_width=True)
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

def view_modifica_nc(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header('✏️ Modifica NC / AC')
    if df_nc.empty:
        st.warning('Nessuna NC presente nel database.')
        return
    status_options = get_status_options(df_nc)
    nc_numbers = sorted(df_nc['nonconformance_number'].dropna().unique().tolist())
    selected_nc = st.selectbox('Seleziona NC', nc_numbers)
    row = df_nc[df_nc['nonconformance_number'] == selected_nc]
    if row.empty:
        st.error('NC non trovata.')
        return
    row = row.iloc[0]
    nc_id = str(row['id']).strip()

    st.subheader('Dati NC')
    with st.form(key='form_modifica_nc'):
        serie = st.text_input('Serie', value=row.get('serie') or '')
        platforms = load_platforms()
        if platforms:
            current = row.get('piattaforma') or (platforms[0] if platforms else '')
            piattaforma = st.selectbox('Piattaforma *', platforms, index=max(0, platforms.index(current)) if current in platforms else 0)
        else:
            piattaforma = st.text_input('Piattaforma * (nessuna piattaforma definita a sistema)', value=row.get('piattaforma') or '')
        short_description = st.text_input('Short description', value=row.get('short_description') or '')
        current_status = row.get('nonconformance_status') or 'OPEN'
        if current_status not in status_options:
            status_options = [current_status] + status_options
        status = st.selectbox('Stato NC', options=status_options, index=status_options.index(current_status))
        nonconform_priority = st.text_input('Priorità NC', value=row.get('nonconform_priority') or '')
        responsibility = st.text_input('Responsabilità', value=row.get('responsibility') or '')
        owner = st.text_input('Owner NC', value=row.get('owner') or '')
        email_address = st.text_input('Email owner NC', value=row.get('email_address') or '')
        nonconformance_source = st.text_input('Fonte NC (source)', value=row.get('nonconformance_source') or '')
        incident_type = st.text_input('Incident type', value=row.get('incident_type') or '')
        c1,c2 = st.columns(2)
        with c1:
            d_open = safe_date_for_input(row.get('date_opened'))
            date_opened = st.date_input('Data apertura', value=d_open or date.today())
        with c2:
            d_close = safe_date_for_input(row.get('date_closed'))
            has_close = st.checkbox('Data chiusura impostata', value=(d_close is not None), key=f"has_date_closed_{nc_id}")
            if has_close:
                date_closed = st.date_input('Data chiusura', value=(d_close or date.today()), key=f"date_closed_{nc_id}")
            else:
                date_closed = None
                st.caption('Data chiusura: —')
        detailed_description = st.text_area('Descrizione dettagliata', value=row.get('detailed_description') or '')
        det_problem_description = st.text_area('Problem description (DET_)', value=row.get('det_problem_description') or '')
        det_cause = st.text_area('Cause (DET_CAUSE)', value=row.get('det_cause') or '')
        det_close = st.text_area('Chiusura (DET_CLOSE)', value=row.get('det_close') or '')
        mob_cur = (row.get('mob') or '').strip()
        mob = st.selectbox('Make/Buy (MOB)', ['Make','Buy'], index=0 if mob_cur != 'Buy' else 1)
        submitted_nc = st.form_submit_button('💾 Salva modifiche NC')
        if submitted_nc:
            errors = []
            if not (serie or '').strip(): errors.append('SERIE è obbligatoria.')
            if not (piattaforma or '').strip(): errors.append('PIATTAFORMA è obbligatoria.')
            if not (short_description or '').strip(): errors.append('SHORT_DESCRIPTION è obbligatoria.')
            if errors:
                for e in errors: st.error(e)
            else:
                owner_clean = owner.strip(); email_clean = email_address.strip()
                if not email_clean and owner_clean:
                    sug = suggest_email_from_name(owner_clean)
                    if sug: email_clean = sug
                vals = {
                    'serie': serie.strip(), 'piattaforma': (piattaforma or '').strip(), 'short_description': (short_description or '').strip(),
                    'nonconformance_status': (status or '').strip(), 'nonconform_priority': (nonconform_priority or '').strip() or None,
                    'responsibility': (responsibility or '').strip() or None, 'owner': owner_clean or None, 'email_address': email_clean or None,
                    'nonconformance_source': (nonconformance_source or '').strip() or None, 'incident_type': (incident_type or '').strip() or None,
                    'date_opened': date_opened.isoformat() if date_opened else None, 'date_closed': date_closed.isoformat() if date_closed else None,
                    'detailed_description': detailed_description or None, 'det_problem_description': det_problem_description or None,
                    'det_cause': det_cause or None, 'det_close': det_close or None, 'mob': mob,
                }
                update_nc_in_db(nc_id, vals)
                st.success('NC aggiornata con successo.')
                trigger_email_prompt(nc_id, 'Modifica dati NC')

    st.markdown('---')
    st.subheader('Azioni Correttive (AC)')
    df_ac_nc = df_ac[df_ac.get('nc_id', pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
    df_ac_nc = _ensure_unique_columns(df_ac_nc)
    ac_labels = [f"{r['ac_corrective_action_num']} - {r.get('ac_short_description','')}" for _, r in df_ac_nc.iterrows()] if not df_ac_nc.empty else []
    if ac_labels:
        pick = st.selectbox('Seleziona AC da modificare', ac_labels)
        if pick:
            idx = ac_labels.index(pick)
            ac_row = df_ac_nc.iloc[idx]
            ac_id = str(ac_row['id']).strip()
            st.write(f"**AC selezionata:** {ac_row['ac_corrective_action_num']}")
            with st.form(key='form_modifica_ac'):
                ac_request_status = st.text_input('Stato AC', value=ac_row.get('ac_request_status') or '')
                ac_request_priority = st.text_input('Priorità AC', value=ac_row.get('ac_request_priority') or '')
                ac_owner = st.text_input('Owner AC', value=ac_row.get('ac_owner') or '')
                ac_email_address = st.text_input('Email owner AC', value=ac_row.get('ac_email_address') or '')
                ac_short_description = st.text_input('Short description AC', value=ac_row.get('ac_short_description') or '')
                ac_detailed_description = st.text_area('Descrizione dettagliata AC', value=ac_row.get('ac_detailed_description') or '')
                ac_effective = st.text_input('Effettiva (AC_EFFECTIVE)', value=ac_row.get('ac_effective') or '')
                ac_evidence_verify = st.text_area('Evidenze verifica (AC_EVIDENCE_VERIFY)', value=ac_row.get('ac_evidence_verify') or '')
                d1,d2,d3,d4 = st.columns(4)
                with d1: ac_date_opened = st.date_input('Data apertura AC', value=safe_date_for_input(ac_row.get('ac_date_opened')))
                with d2: ac_date_required = st.date_input('Data richiesta chiusura', value=safe_date_for_input(ac_row.get('ac_date_required')))
                with d3: ac_end_date = st.date_input('Data chiusura effettiva', value=safe_date_for_input(ac_row.get('ac_end_date')))
                with d4: ac_follow_up_date = st.date_input('Data follow-up', value=safe_date_for_input(ac_row.get('ac_follow_up_date')))
                upd = st.form_submit_button('💾 Salva modifiche AC')
                if upd:
                    owner_clean = ac_owner.strip(); email_clean = ac_email_address.strip()
                    if not email_clean and owner_clean:
                        sug = suggest_email_from_name(owner_clean)
                        if sug: email_clean = sug
                    vals_ac = {
                        'ac_request_status': ac_request_status or None, 'ac_request_priority': ac_request_priority or None,
                        'ac_owner': owner_clean or None, 'ac_email_address': email_clean or None,
                        'ac_short_description': ac_short_description or None, 'ac_detailed_description': ac_detailed_description or None,
                        'ac_effective': ac_effective or None, 'ac_evidence_verify': ac_evidence_verify or None,
                        'ac_date_opened': ac_date_opened.isoformat() if ac_date_opened else None,
                        'ac_date_required': ac_date_required.isoformat() if ac_date_required else None,
                        'ac_end_date': ac_end_date.isoformat() if ac_end_date else None,
                        'ac_follow_up_date': ac_follow_up_date.isoformat() if ac_follow_up_date else None,
                    }
                    update_ac_in_db(nc_id, ac_id, vals_ac)
                    st.success('AC aggiornata con successo.')
                    trigger_email_prompt(nc_id, f"Modifica AC {ac_row['ac_corrective_action_num']}")

    st.markdown('---')
    st.subheader('➕ Aggiungi nuova AC per questa NC')
    df_ac_all = _ensure_unique_columns(load_ac_data())
    next_num = _next_ac_progressive(df_ac_all)
    ac_code = f"AC {next_num} CVT"
    st.info(f"Nuovo numero AC proposto: **{ac_code}**")
    with st.form(key='form_inserisci_ac'):
        ac_short_description_new = st.text_input('Short description AC *')
        ac_owner_new = st.text_input('Owner AC')
        ac_email_address_new = st.text_input('Email owner AC')
        ac_request_status_new = st.text_input('Stato AC', value='OPEN')
        ac_request_priority_new = st.text_input('Priorità AC')
        ac_detailed_description_new = st.text_area('Descrizione dettagliata AC')
        ac_effective_new = st.text_input('Effettiva (AC_EFFECTIVE)')
        ac_evidence_verify_new = st.text_area('Evidenze verifica (AC_EVIDENCE_VERIFY)')
        c1,c2 = st.columns(2)
        today = date.today()
        with c1: ac_date_opened_new = st.date_input('Data apertura AC', value=today)
        with c2: ac_date_required_new = st.date_input('Data richiesta chiusura', value=today)
        submit_new = st.form_submit_button('💾 Crea nuova AC')
        if submit_new:
            errors = []
            if not ac_short_description_new.strip(): errors.append('Short description AC è obbligatoria.')
            if errors:
                for e in errors: st.error(e)
            else:
                owner_clean = ac_owner_new.strip(); email_clean = ac_email_address_new.strip()
                if not email_clean and owner_clean:
                    sug = suggest_email_from_name(owner_clean)
                    if sug: email_clean = sug
                vals_new_ac = {
                    'id': ac_code,
                    'ac_corrective_action_num': ac_code,
                    'ac_owner': owner_clean or None, 'ac_email_address': email_clean or None,
                    'ac_request_status': ac_request_status_new.strip() or None, 'ac_request_priority': ac_request_priority_new.strip() or None,
                    'ac_detailed_description': ac_detailed_description_new or None, 'ac_effective': ac_effective_new or None, 'ac_evidence_verify': ac_evidence_verify_new or None,
                    'ac_date_opened': ac_date_opened_new.isoformat(), 'ac_date_required': ac_date_required_new.isoformat() if ac_date_required_new else None,
                }
                insert_ac_in_db(nc_id, vals_new_ac)
                st.success(f"AC {ac_code} creata con successo per la NC {selected_nc}.")
                trigger_email_prompt(nc_id, f"Nuova AC {ac_code} creata")


def get_next_nc_number(df_nc: pd.DataFrame) -> str:
    if df_nc is None or df_nc.empty or 'nonconformance_number' not in df_nc.columns:
        return 'NC-1-CVT'
    mx = 0
    for val in df_nc['nonconformance_number'].astype(str).tolist():
        m = re.match(r"NC-(\d+)-CVT", val.strip())
        if m:
            try:
                n = int(m.group(1)); mx = max(mx, n)
            except ValueError:
                pass
    return f"NC-{mx+1}-CVT"

def view_inserisci_nc(df_nc: pd.DataFrame):
    st.header('➕ Inserisci nuova NC')
    all_status = get_status_options(df_nc) if not df_nc.empty else ['OPEN','CLOSED','CANCELLED']
    status_options_insert = [s for s in all_status if s.upper() != 'CANCELLED'] or ['OPEN','CLOSED']
    new_nc_number = get_next_nc_number(df_nc)
    st.info(f"Il nuovo numero NC proposto è: **{new_nc_number}**")
    today = date.today()
    with st.form(key='form_inserisci_nc'):
        st.text_input('Numero NC', value=new_nc_number, disabled=True)
        serie = st.text_input('Serie *')
        platforms = load_platforms()
        if platforms:
            piattaforma = st.selectbox('Piattaforma', options=platforms, index=0)
        else:
            piattaforma = st.text_input('Piattaforma')
        short_description = st.text_input('Short description *')
        status = st.selectbox('Stato NC', options=status_options_insert, index=(status_options_insert.index('OPEN') if 'OPEN' in status_options_insert else 0))
        nonconform_priority = st.text_input('Priorità NC')
        responsibility = st.text_input('Responsabilità')
        owner = st.text_input('Owner NC')
        email_address = st.text_input('Email owner NC')
        nonconformance_source = st.text_input('Fonte NC (source)')
        incident_type = st.text_input('Incident type')
        st.date_input('Data apertura (auto)', value=today, disabled=True)
        detailed_description = st.text_area('Descrizione dettagliata')
        det_problem_description = st.text_area('Problem description (DET_)')
        det_cause = st.text_area('Cause (DET_CAUSE)')
        det_close = st.text_area('Chiusura (DET_CLOSE)')
        mob = st.selectbox('Make/Buy (MOB)', ['Make','Buy'], index=0)
        submitted = st.form_submit_button('💾 Crea NC')
        if submitted:
            errors = []
            if not serie.strip(): errors.append('SERIE è obbligatoria.')
            if not (piattaforma or '').strip(): errors.append('PIATTAFORMA è obbligatoria.')
            if not short_description.strip(): errors.append('SHORT_DESCRIPTION è obbligatoria.')
            if errors:
                for e in errors: st.error(e)
            else:
                owner_clean = owner.strip(); email_clean = email_address.strip()
                if not email_clean and owner_clean:
                    sug = suggest_email_from_name(owner_clean)
                    if sug: email_clean = sug
                vals = {
                    'id': new_nc_number,
                    'nonconformance_number': new_nc_number,
                    'date_opened': today.isoformat(),
                    'nonconformance_status': status.strip(),
                    'serie': serie.strip(),
                    'piattaforma': (piattaforma or '').strip(),
                    'short_description': short_description.strip(),
                    'nonconform_priority': nonconform_priority.strip() or None,
                    'responsibility': responsibility.strip() or None,
                    'owner': owner_clean or None,
                    'email_address': email_clean or None,
                    'nonconformance_source': nonconformance_source.strip() or None,
                    'incident_type': incident_type.strip() or None,
                    'detailed_description': detailed_description or None,
                    'det_problem_description': det_problem_description or None,
                    'det_cause': det_cause or None,
                    'det_close': det_close or None,
                    'mob': mob,
                }
                nc_id = insert_nc_in_db(vals)
                st.success(f"NC {new_nc_number} creata con successo.")
                trigger_email_prompt(nc_id, 'Nuova NC creata')


def view_trend_nc_quality_db(df_nc: pd.DataFrame):
    st.header('📈 Trend NC Quality')
    trend_df = load_trend_data()
    if trend_df.empty: st.warning('Nessun dato trend disponibile.'); return
    trend_df = trend_df.dropna(subset=['data_pubblicazione'])
    if trend_df.empty: st.warning('Nessun dato trend con data valida.'); return
    min_d = trend_df['data_pubblicazione'].min().date()
    max_d = trend_df['data_pubblicazione'].max().date()
    st.caption(f"Dati disponibili da {min_d} a {max_d}")
    a,b = st.columns(2)
    with a: sdate = st.date_input('Da data', value=min_d, min_value=min_d, max_value=max_d)
    with b: edate = st.date_input('A data', value=max_d, min_value=min_d, max_value=max_d)
    mask = (trend_df['data_pubblicazione'].dt.date >= sdate) & (trend_df['data_pubblicazione'].dt.date <= edate)
    t = trend_df[mask].copy()
    if t.empty: st.warning('Nessun dato nel range selezionato.'); return
    value_cols = [c for c in t.columns if c not in ['data_pubblicazione','year_week']]
    melt = t.melt(id_vars=['data_pubblicazione'], value_vars=value_cols, var_name='metrica', value_name='valore')
    chart = alt.Chart(melt).mark_line(point=True).encode(x='data_pubblicazione:T', y='valore:Q', color='metrica:N', tooltip=['data_pubblicazione:T','metrica:N','valore:Q']).properties(width='container', height=400)
    st.altair_chart(chart, use_container_width=True)

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

    if df_ac.empty:
        df_nc, df_ac_from_nc = _split_combined_nc_ac(df_nc)
        if not df_ac_from_nc.empty: df_ac = df_ac_from_nc
    if not df_nc.empty and 'id' in df_nc.columns:
        df_nc = df_nc.drop_duplicates(subset=['id']).copy()

    if scelta.startswith('1'): view_lista(df_nc, df_ac)
    elif scelta.startswith('2'): view_consulta_nc(df_nc, df_ac)
    elif scelta.startswith('3'): view_modifica_nc(df_nc, df_ac)
    elif scelta.startswith('4'): view_inserisci_nc(df_nc)
    elif scelta.startswith('5'): view_trend_nc_quality_db(df_nc)
    elif scelta.startswith('6'): view_gestione_piattaforme()

    render_email_prompt()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        try: st.set_page_config(page_title='NC Management (v19-fixed6)', layout='wide')
        except Exception: pass
        st.error("L'app si è interrotta con un errore. Copia/incolla questo stacktrace in chat.")
        st.exception(e)
