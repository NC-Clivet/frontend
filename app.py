import os, re, json, unicodedata
from datetime import date, datetime
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

try:
    import google.generativeai as genai
except ImportError:
    genai = None

# ============================================================
# SECRETS / CONFIG
# ============================================================

from collections.abc import Mapping

def secret_any(*paths, default=""):
    # paths: tuple di chiavi, es: ("google","data_script_url") oppure ("DATA_SCRIPT_URL",)
    for path in paths:
        try:
            cur = st.secrets
            ok = True
            for p in path:
                if isinstance(cur, Mapping) and p in cur:
                    cur = cur[p]
                else:
                    ok = False
                    break
            if ok:
                val = str(cur).strip()
                if val:
                    return val
        except Exception:
            pass
    return default
DATA_SCRIPT_URL = secret_any(
    ("google", "data_script_url"),
    ("DATA_SCRIPT_URL",),
    ("data_script_url",),
).split("?")[0]

MAIL_SCRIPT_URL = secret_any(
    ("google", "mail_script_url"),
    ("MAIL_SCRIPT_URL",),
    ("mail_script_url",),
).split("?")[0]

DATA_KEY = secret_any(
    ("security", "data_api_key"),
    ("DATA_API_KEY",),
    ("data_api_key",),
)


DATA_SCRIPT_URL = secret_any(("google","data_script_url"), ("DATA_SCRIPT_URL",), ("data_script_url",), default="").split("?")[0]
MAIL_SCRIPT_URL = secret_any(("google","mail_script_url"), ("MAIL_SCRIPT_URL",), ("mail_script_url",), default="").split("?")[0]

GEMINI_API_KEY = secret_any(("gemini","api_key"), ("GEMINI_API_KEY",), ("gemini_api_key",), default="")
GEMINI_MODEL   = secret_any(("gemini","model"), ("GEMINI_MODEL",), ("gemini_model",), default="gemini-2.5-flash")

if not DATA_SCRIPT_URL:
    raise RuntimeError("Manca la URL Apps Script DATA: imposta secrets google.data_script_url (o DATA_SCRIPT_URL).")


# ============================================================
# HELPERS API
# ============================================================

def _api_get(op: str, **params):
    url = DATA_SCRIPT_URL
    q = {"op": op, **params}
    if DATA_KEY:
        q["key"] = DATA_KEY

    r = requests.get(url, params=q, timeout=(5, 30), headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(j.get("error", "API error"))
    return j.get("data")

def _api_post(op: str, **body):
    url = DATA_SCRIPT_URL
    payload = {"op": op, **body}
    if DATA_KEY:
        payload["key"] = DATA_KEY

    r = requests.post(url, json=payload, timeout=(5, 60), headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(j.get("error", "API error"))
    return j.get("data")
    
def get_status_options_for_insert():
    # per inserimento: Cancelled NON selezionabile
    return ["New", "Managed", "Close"]

def get_status_options_for_edit(existing_value: str | None = None):
    base = ["New", "Managed", "Close"]
    # Cancelled può esistere (solo se già presente)
    if existing_value and str(existing_value).strip().lower() == "cancelled":
        return base + ["Cancelled"]
    return base
    
def _json_safe(v):
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v

def _serialize_dict(d: dict) -> dict:
    return {k: _json_safe(v) for k, v in (d or {}).items()}

def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).replace("\u00a0", " ").strip() for c in df.columns]
    return df

def _rename_ci(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    """Rinomina colonne in modo case-insensitive, prendendo la prima alternativa trovata."""
    if df is None or df.empty:
        return df
    upper_map = {str(c).strip().upper(): c for c in df.columns}
    ren = {}
    for target, alts in mapping.items():
        for a in alts:
            k = str(a).strip().upper()
            if k in upper_map:
                ren[upper_map[k]] = target
                break
    return df.rename(columns=ren)


NC_COLMAP = {
    "nonconformance_number": ["nonconformance_number", "NONCONFORMANCE_NUMBER", "NC_NUMBER", "NC", "NONCONFORMANCE NO", "NONCONFORMANCE NUM"],
    "nonconformance_status": ["nonconformance_status", "NONCONFORMANCE_STATUS", "STATUS"],
    "date_opened": ["date_opened", "DATE_OPENED", "OPEN_DATE"],
    "date_closed": ["date_closed", "DATE_CLOSED", "CLOSE_DATE"],
    "piattaforma": ["piattaforma", "PIATTAFORMA", "PIATT.", "NEW_MACRO PIATTAFORMA", "MACRO PIATT.", "MACRO PIATTAF."],
    "short_description": ["short_description", "SHORT_DESCRIPTION"],
    "detailed_description": ["detailed_description", "DETAILED_DESCRIPTION"],
    "owner": ["owner", "OWNER"],
    "responsibility": ["responsibility", "RESPONSIBILITY"],
    "nc_parent_ref": ["nc_parent_ref", "NC_PARENT_REF", "NC_PARENT_Y_N", "nc_parent_ref"],
    "id": ["id", "ID"],
    # se nel foglio NC hai anche Make/Buy:
    "mob": ["mob", "MOB", "MAKE_OR_BUY", "MAKE BUY", "MAKE/BUY"],
}

AC_COLMAP = {
    "id": ["id","ID"],
    "nc_id": ["nc_id","NC_ID"],
    "ac_corrective_action_num": ["ac_corrective_action_num","AC_CORRECTIVE_ACTION_NUM"],
    "ac_request_status": ["ac_request_status","AC_REQUEST_STATUS"],
    "ac_request_priority": ["ac_request_priority","AC_REQUEST_PRIORITY"],
    "ac_date_opened": ["ac_date_opened","AC_DATE_OPENED"],
    "ac_date_required": ["ac_date_required","AC_DATE_REQUIRED"],
    "ac_end_date": ["ac_end_date","AC_END_DATE"],
    "ac_follow_up_date": ["ac_follow_up_date","AC_FOLLOW_UP_DATE"],
    "ac_owner": ["ac_owner","AC_OWNER"],
    "ac_email_address": ["ac_email_address","AC_EMAIL_ADDRESS"],
    "ac_short_description": ["ac_short_description","AC_SHORT_DESCRIPTION"],
    "ac_detailed_description": ["ac_detailed_description","AC_DETAILED_DESCRIPTION"],
}
@st.cache_data(show_spinner=False)
def load_nc_data() -> pd.DataFrame:
    data = _api_get("list_nc") or []
    df = pd.DataFrame(data)
    df = _clean_cols(df)
    if df.empty:
        return df

    # ✅ NORMALIZZA NOMI COLONNE (questa è la parte che manca)
    df = _rename_ci(df, NC_COLMAP)

    # date
    for c in ["date_opened", "date_closed", "created_at", "updated_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # id string
    if "id" in df.columns:
        df["id"] = df["id"].astype(str).str.strip()

    # se id manca o è vuoto, usa nonconformance_number
    if "nonconformance_number" in df.columns:
        df["nonconformance_number"] = df["nonconformance_number"].astype(str).str.strip()
        if "id" not in df.columns or df["id"].eq("").all():
            df["id"] = df["nonconformance_number"]

    return df

@st.cache_data(show_spinner=False)
def load_ac_data() -> pd.DataFrame:
    data = _api_get("list_ac") or []
    df = pd.DataFrame(data)
    df = _clean_cols(df)
    if df.empty:
        return pd.DataFrame(columns=["id","nc_id"])
    df = _rename_ci(df, AC_COLMAP)
    # date
    for c in ["ac_date_opened","ac_date_required","ac_end_date","ac_follow_up_date","created_at","updated_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ["id","nc_id","ac_corrective_action_num"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    # id fallback = ac_corrective_action_num
    if "id" not in df.columns and "ac_corrective_action_num" in df.columns:
        df["id"] = df["ac_corrective_action_num"].astype(str).str.strip()
    return df

@st.cache_data(show_spinner=False)

def load_platforms() -> list[str]:
    data = _api_get("list_platforms") or []
    out = [str(x).strip() for x in data if str(x).strip()]
    return sorted(list(dict.fromkeys(out)))

def clear_caches():
    load_nc_data.clear()
    load_ac_data.clear()
    load_platforms.clear()

# ============================================================
# EMAIL VIA IFRAME (mittente = utente Workspace)
# ============================================================

def send_mail_via_hidden_iframe(script_url: str, payload: dict, key: str = "sendmail"):
    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_js = json.dumps(payload_json).replace("</", "<\\/")

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
  const payload = {payload_js};
  function toB64Unicode(str) {{
    return btoa(unescape(encodeURIComponent(str)));
  }}
  const wrap = document.getElementById("{key}_wrap");
  wrap.innerHTML = '<div style="padding:6px 0;">📨 Invio email in corso...</div>';

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

  document.getElementById("{key}_payload_b64").value = toB64Unicode(payload);
  document.getElementById("{key}_form").submit();
}})();
</script>
"""
    components.html(html, height=60)

def _pick_first(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            return s
    return ""

def get_nc_details(nc_id: str) -> dict:
    data = _api_get("get_nc", id=str(nc_id))
    return data if isinstance(data, dict) else {}

def get_ac_details_for_nc(nc_id: str) -> list[dict]:
    data = _api_get("list_ac_for_nc", nc_id=str(nc_id))
    return data if isinstance(data, list) else []

def get_emails_for_nc(nc_id: str) -> list[str]:
    emails = set()
    nc = get_nc_details(nc_id)
    if isinstance(nc, dict):
        v = (nc.get("email_address") or "").strip()
        if v:
            emails.add(v)

    for ac in get_ac_details_for_nc(nc_id) or []:
        v = (ac.get("ac_email_address") or "").strip()
        if v:
            emails.add(v)
    return sorted(emails)

def _operation_to_action(operation: str) -> str:
    op = (operation or "").lower()
    if "nuova" in op or "crea" in op or "inser" in op:
        return "create_nc"
    return "update_nc"

def trigger_email_prompt(nc_id: str, operation: str):
    st.session_state["show_email_prompt"] = True
    st.session_state["email_nc_id"] = str(nc_id)
    st.session_state["email_operation"] = str(operation)

def render_email_prompt():
    if not st.session_state.get("show_email_prompt"):
        return

    nc_id = st.session_state.get("email_nc_id")
    operation = st.session_state.get("email_operation", "Aggiornamento NC")
    nc = get_nc_details(nc_id) if nc_id else {}
    nc_number = nc.get("nonconformance_number", nc_id)

    emails = get_emails_for_nc(nc_id) if nc_id else []

    safe_op = re.sub(r"[^A-Za-z0-9]+", "_", operation)
    ctx = f"{nc_id}_{safe_op}"

    st.markdown("---")
    st.subheader("Inviare le modifiche agli owner?")
    st.write(f"Vuoi inviare una mail per **NC {nc_number}**?")

    if emails:
        st.write("Destinatari:")
        for e in emails:
            st.write(f"- {e}")
    else:
        st.warning("Nessun indirizzo email trovato (NC/AC).")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("✉️ Sì, invia", key=f"mail_yes_{ctx}"):
            if not emails:
                st.warning("Nessun indirizzo email: impossibile inviare.")
            else:
                ac_list = get_ac_details_for_nc(nc_id)

                payload = {
                    "type": _operation_to_action(operation),
                    "to": ", ".join(emails),
                    "nc": {
                        "nonconformance_number": nc.get("nonconformance_number", ""),
                        "subject": _pick_first(nc, ["incident_type", "nonconformance_source", "subject"]),
                        "short_description": nc.get("short_description", ""),
                        "opened_by": nc.get("created_by", ""),
                        "owner": nc.get("owner", ""),
                        "mob": nc.get("mob",""),
                        "responsibility": nc.get("responsibility", ""),
                        "nonconformance_status": nc.get("nonconformance_status", ""),
                        "piattaforma": nc.get("piattaforma", ""),
                    },
                    "ac_list": [
                        {
                        "ac_corrective_action_num": a.get("AC_CORRECTIVE_ACTION_NUM","") or a.get("ac_corrective_action_num",""),
                        "ac_short_description": a.get("AC_SHORT_DESCRIPTION","") or a.get("ac_short_description",""),
                        "ac_owner": a.get("AC_OWNER","") or a.get("ac_owner",""),
                        "ac_request_status": a.get("AC_REQUEST_STATUS","") or a.get("ac_request_status",""),
                        }
                        for a in (ac_list or [])
                    ]
                }

                st.session_state[f"mail_payload_{ctx}"] = payload

        payload = st.session_state.get(f"mail_payload_{ctx}")
        if payload:
            send_mail_via_hidden_iframe(MAIL_SCRIPT_URL, payload, key=f"mail_{ctx}")
            if st.button("✅ Chiudi", key=f"mail_close_{ctx}"):
                st.session_state["show_email_prompt"] = False
                st.session_state.pop(f"mail_payload_{ctx}", None)
                st.rerun()

    with c2:
        if st.button("❌ No, non inviare", key=f"mail_no_{ctx}"):
            st.session_state["show_email_prompt"] = False
            st.rerun()

# ============================================================
# GEMINI
# ============================================================

def call_gemini(prompt: str) -> str:
    if genai is None:
        raise RuntimeError("Libreria google-generativeai non installata. Aggiungila in requirements.txt")
    if not GEMINI_API_KEY:
        raise RuntimeError("Chiave Gemini mancante in secrets: [gemini].api_key (o GEMINI_API_KEY).")
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)
    resp = model.generate_content(prompt)
    return (resp.text or "").strip()

def build_nc_ac_context(nc: dict, ac_list: list[dict]) -> str:
    lines = []
    lines.append(f"NC number: {nc.get('nonconformance_number')}")
    lines.append(f"Status: {nc.get('nonconformance_status')}")
    lines.append(f"Opened: {nc.get('date_opened')}")
    lines.append(f"Closed: {nc.get('date_closed')}")
    lines.append(f"Serie: {nc.get('serie')}")
    lines.append(f"Piattaforma: {nc.get('piattaforma')}")
    lines.append(f"Owner: {nc.get('owner')}")
    lines.append(f"Short: {nc.get('short_description')}")
    lines.append(f"Detail: {nc.get('detailed_description')}")
    lines.append("\nCorrective Actions (AC):")
    if ac_list:
        for a in ac_list:
            lines.append(f"- AC {a.get('ac_corrective_action_num')} | owner={a.get('ac_owner')} | status={a.get('ac_request_status')}")
            lines.append(f"  short={a.get('ac_short_description')}")
            lines.append(f"  detail={a.get('ac_detailed_description')}")
    else:
        lines.append("  (no AC linked)")
    return "\n".join(lines)

# ============================================================
# CRUD WRAPPERS
# ============================================================

def insert_nc(values: dict) -> str:
    out = _api_post("create_nc", _user="streamlit", payload=_serialize_dict(values))
    clear_caches()
    return str(out.get("id") if isinstance(out, dict) else "")

def update_nc(nc_id: str, patch: dict):
    _api_post("update_nc", _user="streamlit", id=str(nc_id), patch=_serialize_dict(patch))
    clear_caches()

def insert_ac(values: dict):
    _api_post("create_ac", _user="streamlit", payload=_serialize_dict(values))
    clear_caches()

def update_ac(ac_id: str, patch: dict):
    _api_post("update_ac", _user="streamlit", id=str(ac_id), patch=_serialize_dict(patch))
    clear_caches()

# ============================================================
# UI (MINIMA: Inserisci + Modifica + Consulta)
# ============================================================

def view_lista(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header("📋 Lista")
    tab = st.radio("Visualizza", ["NC", "AC"], horizontal=True)

    if tab == "NC":
        if df_nc.empty:
            st.info("Nessuna NC.")
            return
        cols = [c for c in [
            "nonconformance_number","nonconformance_status","date_opened","date_closed",
            "serie","piattaforma","owner","responsibility","short_description"
        ] if c in df_nc.columns]
        st.dataframe(df_nc[cols], width="stretch", hide_index=True)
    else:
        if df_ac.empty:
            st.info("Nessuna AC.")
            return
        cols = [c for c in [
            "nc_id","ac_corrective_action_num","ac_request_status","ac_owner","ac_short_description",
            "ac_date_opened","ac_date_required","ac_end_date"
        ] if c in df_ac.columns]
        st.dataframe(df_ac[cols], width="stretch", hide_index=True)

def view_inserisci_nc(df_nc: pd.DataFrame):
    st.header("➕ Nuova NC")
    today = date.today()

    # numero NC proposto
    def next_nc_number():
        if df_nc.empty or "nonconformance_number" not in df_nc.columns:
            return "NC-1-CVT"
        mx = 0
        for s in df_nc["nonconformance_number"].astype(str):
            m = re.match(r"NC-(\d+)-CVT", s.strip())
            if m:
                mx = max(mx, int(m.group(1)))
        return f"NC-{mx+1}-CVT"

    nc_num = next_nc_number()
    st.info(f"Numero NC proposto: **{nc_num}**")

    platforms = load_platforms()

    with st.form("form_new_nc"):
        serie = st.text_input("Serie *")
        piattaforma = st.selectbox("Piattaforma *", platforms) if platforms else st.text_input("Piattaforma *")
        short = st.text_input("Short description *")
        status = st.selectbox("Stato", ["New","Managed","Close","Cancelled"], index=0)
        RESP_OPTIONS = ["R&D", "Operation", "Supplier", "MKT", "Other", "Third party"]
        responsibility = st.selectbox("Responsabilità", RESP_OPTIONS, index=0)
        owner = st.text_input("Owner")
        email = st.text_input("Email owner")
        mob = st.selectbox("Make/Buy (MOB)", ["Make", "Buy"], index=0)
        mob = st.selectbox("Make/Buy", ["Make","Buy"], index=0)
        detailed = st.text_area("Detailed description")
        submitted = st.form_submit_button("💾 Crea NC")

    if submitted:
        if not serie.strip() or not str(piattaforma).strip() or not short.strip():
            st.error("Compila Serie, Piattaforma, Short description.")
            return

        vals = {
            "nonconformance_number": nc_num,
            "id": nc_num,
            "date_opened": today.isoformat(),
            "nonconformance_status": status,
            "serie": serie.strip(),
            "piattaforma": str(piattaforma).strip(),
            "short_description": short.strip(),
            "detailed_description": detailed or "",
            "owner": owner.strip(),
            "email_address": email.strip(),
            "responsibility": responsibility,
            "mob": mob
        }
        new_id = insert_nc(vals)
        st.success(f"Creata {nc_num}")
        trigger_email_prompt(new_id, "Nuova NC creata")

def view_modifica(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header("✏️ Modifica NC / AC")

    if df_nc.empty:
        st.info("Nessuna NC.")
        return

    nc_numbers = sorted(df_nc["nonconformance_number"].dropna().astype(str).tolist())
    selected = st.selectbox("Seleziona NC", nc_numbers)
    nc_row = df_nc[df_nc["nonconformance_number"].astype(str) == str(selected)].iloc[0]
    nc_id = str(nc_row["id"]).strip()

    st.subheader(f"NC {selected}")

    with st.form("form_edit_nc"):
        short = st.text_input("Short", value=str(nc_row.get("short_description","") or ""))
        status = st.selectbox("Stato", ["New","Managed","Close","Cancelled"],
                              index=max(0, ["New","Managed","Close","Cancelled"].index(str(nc_row.get("nonconformance_status","New")) if str(nc_row.get("nonconformance_status","New")) in ["New","Managed","Close","Cancelled"] else "New")))
        RESP_OPTIONS = ["R&D", "Operation", "Supplier", "MKT", "Other", "Third party"]
        responsibility = st.selectbox("Responsabilità", RESP_OPTIONS, index=0)
        owner = st.text_input("Owner", value=str(nc_row.get("owner","") or ""))
        email = st.text_input("Email", value=str(nc_row.get("email_address","") or ""))
        detailed = st.text_area("Detailed", value=str(nc_row.get("detailed_description","") or ""))
        mob_cur = (row.get("mob") or "").strip()
        mob = st.selectbox("Make/Buy (MOB)", ["Make","Buy"], index=0 if mob_cur!="Buy" else 1)
        save = st.form_submit_button("💾 Salva NC")

    if save:
        patch = {
            "short_description": short.strip(),
            "nonconformance_status": status,
            "responsibility": responsibility,
            "owner": owner.strip(),
            "email_address": email.strip(),
            "detailed_description": detailed,
            "mob": mob
        }
        update_nc(nc_id, patch)
        st.success("NC aggiornata.")
        trigger_email_prompt(nc_id, "Modifica dati NC")

    st.markdown("---")
    st.subheader("Azioni Correttive (AC)")

    df_ac_nc = df_ac[df_ac.get("nc_id", pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
    if df_ac_nc.empty:
        st.info("Nessuna AC collegata.")
    else:
        cols = [c for c in ["ac_corrective_action_num","ac_request_status","ac_owner","ac_short_description","ac_date_required","ac_end_date"] if c in df_ac_nc.columns]
        st.dataframe(df_ac_nc[cols], width="stretch", hide_index=True)

        labels = [
            f"{r.get('ac_corrective_action_num','')} - {r.get('ac_short_description','')}"
            for _, r in df_ac_nc.iterrows()
        ]
        pick = st.selectbox("Seleziona AC", labels)
        ac_row = df_ac_nc.iloc[labels.index(pick)]
        ac_id = str(ac_row.get("id") or ac_row.get("ac_corrective_action_num")).strip()

        with st.form("form_edit_ac"):
            ac_status = st.text_input("Stato AC", value=str(ac_row.get("ac_request_status","") or ""))
            ac_owner = st.text_input("Owner AC", value=str(ac_row.get("ac_owner","") or ""))
            ac_mail = st.text_input("Email AC", value=str(ac_row.get("ac_email_address","") or ""))
            ac_short = st.text_input("Short AC", value=str(ac_row.get("ac_short_description","") or ""))
            ac_det = st.text_area("Detail AC", value=str(ac_row.get("ac_detailed_description","") or ""))
            upd = st.form_submit_button("💾 Salva AC")
        if upd:
            patch = {
                "ac_request_status": ac_status,
                "ac_owner": ac_owner,
                "ac_email_address": ac_mail,
                "ac_short_description": ac_short,
                "ac_detailed_description": ac_det
            }
            update_ac(ac_id, patch)
            st.success("AC aggiornata.")
            trigger_email_prompt(nc_id, f"Modifica AC {ac_row.get('ac_corrective_action_num','')}")

    st.markdown("---")
    st.subheader("➕ Nuova AC")

    with st.form("form_new_ac"):
        ac_num = st.text_input("AC corrective action num * (alfanumerico)")
        ac_short = st.text_input("Short AC *")
        ac_owner = st.text_input("Owner AC")
        ac_mail = st.text_input("Email AC")
        ac_status = st.text_input("Stato AC", value="OPEN")
        ac_priority = st.text_input("Priorità AC")
        ac_required = st.date_input("Data richiesta", value=date.today())
        create = st.form_submit_button("💾 Crea AC")

    if create:
        if not ac_num.strip() or not ac_short.strip():
            st.error("Compila AC num e Short AC.")
        else:
            vals = {
                "nc_id": nc_id,  # col A = NC id
                "id": ac_num.strip(),
                "ac_corrective_action_num": ac_num.strip(),
                "ac_short_description": ac_short.strip(),
                "ac_owner": ac_owner.strip(),
                "ac_email_address": ac_mail.strip(),
                "ac_request_status": ac_status.strip(),
                "ac_request_priority": ac_priority.strip(),
                "ac_date_required": ac_required.isoformat(),
            }
            insert_ac(vals)
            st.success("AC creata.")
            trigger_email_prompt(nc_id, f"Nuova AC {ac_num.strip()} creata")

def view_consulta(df_nc: pd.DataFrame, df_ac: pd.DataFrame):
    st.header("🔍 Consulta NC")
    if df_nc.empty:
        st.info("Nessuna NC.")
        return
    nc_numbers = sorted(df_nc["nonconformance_number"].dropna().astype(str).tolist())
    selected = st.selectbox("Seleziona NC", nc_numbers)
    nc_row = df_nc[df_nc["nonconformance_number"].astype(str) == str(selected)].iloc[0]
    nc_id = str(nc_row["id"]).strip()

    st.subheader(f"NC {selected}")
    st.write(nc_row.to_dict())

    df_ac_nc = df_ac[df_ac.get("nc_id", pd.Series(dtype=str)).astype(str).str.strip() == str(nc_id)].copy()
    st.markdown("### AC collegate")
    if df_ac_nc.empty:
        st.info("Nessuna AC.")
    else:
        st.dataframe(df_ac_nc, width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader("🤖 Gemini")
    if st.button("Verifica NC con Gemini"):
        nc = get_nc_details(nc_id)
        ac_list = get_ac_details_for_nc(nc_id)
        ctx = build_nc_ac_context(nc, ac_list)
        prompt = f"""Sei un esperto di gestione Non Conformità industriali.

Analizza NC + AC e dammi suggerimenti pratici (bullet points):
- completezza
- contenimenti
- azioni correttive
- azioni su magazzino/fornitore/cliente
- retrofit/comunicazioni utili

DATI:
{ctx}
"""
        try:
            st.write(call_gemini(prompt))
        except Exception as e:
            st.error(str(e))

# ============================================================
# MAIN
# ============================================================

def main():
    st.set_page_config(page_title="NC Management", layout="wide")
    st.sidebar.title("Menu")
    choice = st.sidebar.radio("Sezione", ["Lista", "Consulta", "Modifica", "Nuova NC", "Piattaforme"])

    try:
        df_nc = load_nc_data()
        df_ac = load_ac_data()
    except Exception as e:
        st.error("Errore caricamento backend")
        st.exception(e)
        st.stop()

    with st.sidebar.expander("Healthcheck", expanded=False):
        st.write("DATA:", DATA_SCRIPT_URL)
        st.write("MAIL:", MAIL_SCRIPT_URL)
        st.write("NC:", len(df_nc))
        st.write("AC:", len(df_ac))

    if choice == "Lista":
        view_lista(df_nc, df_ac)
    elif choice == "Consulta":
        view_consulta(df_nc, df_ac)
    elif choice == "Modifica":
        view_modifica(df_nc, df_ac)
    elif choice == "Nuova NC":
        view_inserisci_nc(df_nc)
    elif choice == "Piattaforme":
        st.header("🧩 Piattaforme")
        plats = load_platforms()
        st.dataframe(pd.DataFrame({"Piattaforma": plats}), width="stretch", hide_index=True)

        newp = st.text_input("Nuova piattaforma")
        if st.button("➕ Aggiungi"):
            if not newp.strip():
                st.error("Inserisci un nome.")
            else:
                _api_post("add_platform", _user="streamlit", name=newp.strip())
                clear_caches()
                st.success("Aggiunta.")
                st.rerun()

    # PROMPT EMAIL SEMPRE IN FONDO
    render_email_prompt()

if __name__ == "__main__":
    main()

