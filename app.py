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
NC_TO_SHEET = {
    "id": "ID",
    "nonconformance_number": "NONCONFORMANCE_NUMBER",
    "nonconformance_status": "NONCONFORMANCE_STATUS",
    "nc_parent_y_n": "NC_PARENT_Y_N",
    "nc_parent_ref": "NC_PARENT_REF",
    "date_opened": "DATE_OPENED",
    "nonconformance_source": "NONCONFORMANCE_SOURCE",
    "service_request": "SERVICE_REQUEST",
    "incident_type": "INCIDENT_TYPE",
    "item_instance_serial": "ITEM_INSTANCE_SERIAL",
    "item_id": "ITEM_ID",
    "item": "ITEM",
    "serie": "SERIE",
    "grandezza": "GRANDEZZA",
    "piattaforma": "PIATTAFORMA",
    "macro_piattaforma": "MACRO PIATTAFORMA",   # o "MACRO PIATT." se è quello reale
    "mob": "MOB",
    "short_description": "SHORT_DESCRIPTION",
    "detailed_description": "DETAILED_DESCRIPTION",
    "nonconform_priority": "NONCONFORM_PRIORITY",
    "item_desc": "ITEM_DESC",
    "supplier": "SUPPLIER",
    "created_by": "ENTERED_BY_USER",
    "owner": "OWNER",
    "email_address": "EMAIL_ADDRESS",
    "send_email": "SEND_EMAIL",
    "date_closed": "DATE_CLOSED",
    "quantity_nonconforming": "QUANTITY_NONCONFORMING",
    "nonconforming_uom": "NONCONFORMING_UOM",
    "days_to_close": "DAYS_TO_CLOSE",
    "cost_smry_internal": "COST_SMRY_INTERNAL",
    "cost_smry_customer": "COST_SMRY_CUSTOMER",
    "responsibility": "RESPONSIBILITY",
    "det_problem_description": "DET_PROBLEM_DESCRIPTION",
    "det_cause": "DET_CAUSE",
    "det_close": "DET_CLOSE",
}

AC_TO_SHEET = {
    "id": "ID",
    "nc_id": "nc_id",  # qui dipende: se nel foglio AC è proprio "nc_id" lascialo così; se è "NC_ID" metti "NC_ID"
    "ac_corrective_action_num": "AC_CORRECTIVE_ACTION_NUM",
    "ac_request_source": "AC_REQUEST_SOURCE",
    "ac_implementation_type": "AC_IMPLEMENTATION_TYPE",
    "ac_date_opened": "AC_DATE_OPENED",
    "ac_requestor": "AC_REQUESTOR",
    "ac_owner": "AC_OWNER",
    "ac_send_email": "AC_SEND_EMAIL",
    "ac_email_address": "AC_EMAIL_ADDRESS",
    "ac_short_description": "AC_SHORT_DESCRIPTION",
    "ac_request_priority": "AC_REQUEST_PRIORITY",
    "ac_date_required": "AC_DATE_REQUIRED",
    "ac_detailed_description": "AC_DETAILED_DESCRIPTION",
    "ac_cost_smry_internal": "AC_COST_SMRY_INTERNAL",
    "ac_end_date": "AC_END_DATE",
    "ac_effective": "AC_EFFECTIVE",
    "ac_evidence_verify": "AC_EVIDENCE_VERIFY",
    "ac_follow_up_date": "AC_FOLLOW_UP_DATE",
    "ac_request_status": "AC_REQUEST_STATUS",
    "ac_days_to_close": "AC_DAYS_TO_CLOSE",
    "ac_car_class": "AC_CAR_CLASS",
    "new_macro_piattaforma": "NEW_MACRO PIATTAFORMA",
}

def _to_sheet_keys(d: dict, mapping: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        kk = mapping.get(k, k)   # se non mappato, lo lascia invariato
        out[kk] = v
    return out


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
    payload = _to_sheet_keys(values, NC_TO_SHEET)
    out = _api_post("create_nc", payload=_serialize_dict(payload))
    load_nc_data.clear(); load_ac_data.clear()
    return str(out.get('id','')) if isinstance(out, dict) else ""

def update_nc_in_db(nc_id: str, values: dict):
    patch = _to_sheet_keys(values, NC_TO_SHEET)
    _api_post("update_nc", id=str(nc_id), patch=_serialize_dict(patch))
    load_nc_data.clear(); load_ac_data.clear()

def insert_ac_in_db(nc_id: str, values: dict):
    payload = {"nc_id": str(nc_id)}
    payload.update(values or {})
    payload = _to_sheet_keys(payload, AC_TO_SHEET)
    _api_post("create_ac", payload=_serialize_dict(payload))
    load_ac_data.clear()

def update_ac_in_db(nc_id: str, ac_id: str, values: dict):
    patch = _to_sheet_keys(values, AC_TO_SHEET)
    _api_post("update_ac", id=str(ac_id), patch=_serialize_dict(patch))
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

def _truthy_flag(v) -> bool:
    s = str(v or "").strip().upper()
    return s in ("Y","YES","TRUE","1","SI","SÌ")


def get_next_nc_number(df_nc: pd.DataFrame, prefix="NC", suffix="CVT", width=4) -> str:
    if df_nc is None or df_nc.empty:
        return f"{prefix}-{str(1).zfill(width)}-{suffix}"

    col = None
    for candidate in ["nonconformance_number", "NONCONFORMANCE_NUMBER"]:
        if candidate in df_nc.columns:
            col = candidate
            break
    if not col:
        return f"{prefix}-{str(1).zfill(width)}-{suffix}"

    series = df_nc[col].astype(str).fillna("").str.strip()
    rx = re.compile(rf"^{prefix}[\s\-]*?(\d+)[\s\-]*?{suffix}$", re.IGNORECASE)

    max_n = 0
    for s in series:
        s2 = s.replace("\u00a0", " ").strip()
        m = rx.match(s2)
        if m:
            try:
                n = int(m.group(1))
                max_n = max(max_n, n)
            except:
                pass

    next_n = max_n + 1 if max_n > 0 else 1
    return f"{prefix}-{str(next_n).zfill(width)}-{suffix}"


def get_next_ac_number(df_ac: pd.DataFrame, prefix="AC", suffix="CVT", width=4) -> str:
    if df_ac is None or df_ac.empty:
        return f"{prefix}-{str(1).zfill(width)}-{suffix}"

    # prova più nomi possibili della colonna
    col = None
    for candidate in ["ac_corrective_action_num", "AC_CORRECTIVE_ACTION_NUM", "ac_number"]:
        if candidate in df_ac.columns:
            col = candidate
            break
    if not col:
        return f"{prefix}-{str(1).zfill(width)}-{suffix}"

    series = df_ac[col].astype(str).fillna("")

    # regex più tollerante: cerca AC ... numero ... CVT ovunque nella stringa
    rx = re.compile(rf"\b{re.escape(prefix)}[\s\-]*?(\d+)[\s\-]*?{re.escape(suffix)}\b", re.IGNORECASE)

    max_n = 0
    for s in series:
        s2 = str(s)

        # normalizzazioni Google Sheets / Unicode
        s2 = s2.replace("\u00a0", " ").strip()     # NBSP
        s2 = s2.lstrip("'").strip()                # apostrofo iniziale
        s2 = s2.replace("–", "-").replace("—", "-").replace("-", "-").replace("−", "-")  # trattini unicode
        s2 = re.sub(r"\s+", " ", s2)               # spazi multipli

        m = rx.search(s2)
        if m:
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except ValueError:
                pass

    next_n = max_n + 1 if max_n > 0 else 1
    return f"{prefix}-{str(next_n).zfill(width)}-{suffix}"

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
        nonconform_priority = st.text_input("Priorità NC", value=str(D('nonconform_priority','')))
    with c4:
        st.text_input("Data apertura", value=str(date_opened), disabled=True)

    p1, p2 = st.columns([0.9, 2.1])
    with p1:
        is_child = st.checkbox("NC derivata da altra NC (Parent)?", value=_truthy_flag(D('nc_parent_y_n')))
    with p2:
        nc_parent_ref = st.text_input("Riferimento NC padre", value=str(D('nc_parent_ref','')), disabled=(not is_child))

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

    b1, b2, b3 = st.columns([1.3, 1.3, 1.1])
    with b1:
        platforms = load_platforms()
        if platforms:
            cur_pl = str(D('piattaforma', platforms[0] if platforms else '')).strip()
            idx = platforms.index(cur_pl) if cur_pl in platforms else 0
            piattaforma = st.selectbox("Piattaforma *", options=platforms, index=idx)
        else:
            piattaforma = st.text_input("Piattaforma * (nessuna piattaforma definita)", value=str(D('piattaforma','')))
    with b2:
        macro_piattaforma = st.text_input("Macro piattaforma", value=str(D('macro_piattaforma','')))
    with b3:
        item_id = st.text_input("Item ID", value=str(D('item_id','')))

    c5, c6 = st.columns([1.2, 2.8])
    with c5:
        item = st.text_input("Item", value=str(D('item','')))
    with c6:
        item_desc = st.text_input("Item descrizione", value=str(D('item_desc','')))

    st.subheader("3) Origine evento")
    o1, o2, o3, o4 = st.columns(4)
    with o1:
        nonconformance_source = st.text_input("Fonte (source)", value=str(D('nonconformance_source','')))
    with o2:
        incident_type = st.text_input("Incident type", value=str(D('incident_type','')))
    with o3:
        service_request = st.text_input("Service request / Ticket", value=str(D('service_request','')))
    with o4:
        supplier = st.text_input("Supplier", value=str(D('supplier','')))

    q1, q2 = st.columns(2)
    with q1:
        quantity_nonconforming = st.text_input("Quantità non conforme", value=str(D('quantity_nonconforming','')))
    with q2:
        nonconforming_uom = st.text_input("UoM", value=str(D('nonconforming_uom','')))

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

        'nc_parent_y_n': 'Y' if is_child else 'N',
        'nc_parent_ref': (nc_parent_ref or '').strip() if is_child else None,

        'serie': (serie or '').strip(),
        'grandezza': (grandezza or '').strip() or None,
        'item_instance_serial': (item_instance_serial or '').strip() or None,
        'mob': mob,

        'piattaforma': (piattaforma or '').strip(),
        'macro_piattaforma': (macro_piattaforma or '').strip() or None,
        'item_id': (item_id or '').strip() or None,
        'item': (item or '').strip() or None,
        'item_desc': (item_desc or '').strip() or None,

        'nonconformance_source': (nonconformance_source or '').strip() or None,
        'incident_type': (incident_type or '').strip() or None,
        'service_request': (service_request or '').strip() or None,
        'supplier': (supplier or '').strip() or None,
        'quantity_nonconforming': (quantity_nonconforming or '').strip() or None,
        'nonconforming_uom': (nonconforming_uom or '').strip() or None,

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

def render_ac_form(defaults: dict | None = None, mode: str = "create", proposed_code: str | None = None) -> dict:
    """Rende la UI della AC (create/edit) e ritorna un dict pronto per il backend."""
    defaults = defaults or {}
    today = date.today()

    def D(key, fallback=""):
        v = defaults.get(key, fallback)
        return "" if v is None else v

    st.subheader("1) Identità AC")
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
    with c1:
        ac_num_val = str(D('ac_corrective_action_num', D('ac_number',''))).strip() or (proposed_code or "")
        st.text_input("AC number", value=ac_num_val, disabled=(mode=="edit" or bool(proposed_code)))
    with c2:
        ac_request_status = st.text_input("Stato AC", value=str(D('ac_request_status','OPEN')) or "OPEN")
    with c3:
        ac_request_priority = st.text_input("Priorità AC", value=str(D('ac_request_priority','')))
    with c4:
        st.text_input("Data apertura", value=str(D('ac_date_opened', today)), disabled=True)

    d1, d2, d3 = st.columns(3)
    with d1:
        ac_date_required = st.text_input("Data richiesta", value=str(D('ac_date_required','')))
    with d2:
        ac_end_date = st.text_input("Data fine", value=str(D('ac_end_date','')))
    with d3:
        ac_follow_up_date = st.text_input("Follow-up", value=str(D('ac_follow_up_date','')))

    st.subheader("2) Owner / email / source")
    o1, o2, o3, o4 = st.columns([1.2, 1.4, 0.9, 1.2])
    with o1:
        ac_owner = st.text_input("Owner AC", value=str(D('ac_owner','')))
    with o2:
        ac_email_address = st.text_input("Email owner", value=str(D('ac_email_address','')))
    with o3:
        ac_send_email = st.checkbox("Invia email", value=_truthy_flag(D('ac_send_email')))
    with o4:
        ac_requestor = st.text_input("Requestor", value=str(D('ac_requestor','')))

    # Short subito sotto
    ac_short_description = st.text_input("Short description *", value=str(D('ac_short_description','')))
    ac_detailed_description = st.text_area("Descrizione dettagliata", value=str(D('ac_detailed_description','')), height=140)

    st.subheader("3) Classificazione / tipo")
    s1, s2, s3 = st.columns(3)
    with s1:
        ac_request_source = st.text_input("Request source", value=str(D('ac_request_source','')))
    with s2:
        ac_implementation_type = st.text_input("Implementation type", value=str(D('ac_implementation_type','')))
    with s3:
        ac_car_class = st.text_input("CAR class", value=str(D('ac_car_class','')))

    st.subheader("4) Costi / piattaforma")
    k1, k2 = st.columns(2)
    with k1:
        ac_cost_smry_internal = st.text_input("Costo interno (smry)", value=str(D('ac_cost_smry_internal','')))
    with k2:
        new_macro_piattaforma = st.text_input("New macro piattaforma", value=str(D('new_macro_piattaforma','')))

    st.subheader("5) Chiusura AC")
    ac_effective = st.text_area("Efficacia (AC_EFFECTIVE)", value=str(D('ac_effective','')), height=90)
    ac_evidence_verify = st.text_area("Evidenze (AC_EVIDENCE_VERIFY)", value=str(D('ac_evidence_verify','')), height=90)

    # autocorrezione email da owner
    owner_clean = ac_owner.strip()
    email_clean = str(ac_email_address or '').strip()
    if not email_clean and owner_clean:
        sug = suggest_email_from_name(owner_clean)
        if sug:
            email_clean = sug

    vals = {
        'ac_corrective_action_num': ac_num_val.strip() or None,
        'ac_request_status': (ac_request_status or '').strip() or None,
        'ac_request_priority': (ac_request_priority or '').strip() or None,

        'ac_date_required': (ac_date_required or '').strip() or None,
        'ac_end_date': (ac_end_date or '').strip() or None,
        'ac_follow_up_date': (ac_follow_up_date or '').strip() or None,

        'ac_requestor': (ac_requestor or '').strip() or None,
        'ac_owner': owner_clean or None,
        'ac_email_address': email_clean or None,
        'ac_send_email': 'Y' if ac_send_email else 'N',

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
            trigger_email_prompt(nc_id, 'Modifica dati NC')

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
            vals_ac = render_ac_form(defaults=ac_row.to_dict(), mode="edit")
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
                trigger_email_prompt(nc_id, f"Modifica AC {ac_row.get('ac_corrective_action_num')}")
    else:
        st.info('Nessuna AC collegata a questa NC.')

    st.markdown('---')
    st.subheader('➕ Aggiungi nuova AC per questa NC')

    df_ac_all = _ensure_unique_columns(load_ac_data())
    ac_code = get_next_ac_number(df_ac_all)

    with st.form(key='form_inserisci_ac_levels'):
        vals_new = render_ac_form(defaults={}, mode="create", proposed_code=ac_code)
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
            trigger_email_prompt(nc_id, f"Nuova AC {ac_code} creata")

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

        except Exception: pass
        st.error("L'app si è interrotta con un errore. Copia/incolla questo stacktrace in chat.")
        st.exception(e)
