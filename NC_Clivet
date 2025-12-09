import re
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st
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

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_API_KEY = "AIzaSyDC9sm2ul7gBmsg010PWtFcZNbRjVif6oQ"




# ============================================================
# CONFIG
# ============================================================

DB_PATH = r"P:\QA\007 Validazione Prodotti\11 Non conformità\nc_system.db"
TREND_PATH = r"P:\QA\007 Validazione Prodotti\11 Non conformità\Trend _NC Quality_.xlsx"

# Email / SMTP - DA PERSONALIZZARE
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "nc.clivet@gmail.com"   # TODO: sostituisci
SMTP_PASSWORD = "omenkbnewgqmkbmr"       # TODO: sostituisci


# ============================================================
# DB HELPERS
# ============================================================

def get_connection():
    return sqlite3.connect(DB_PATH)


@st.cache_data(show_spinner=False)
def load_nc_data() -> pd.DataFrame:
    """Carica tutte le NC dal database in un DataFrame (con tutte le colonne)."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM nonconformances", conn)
    conn.close()

    # conversione date (in oggetti date)
    for col in ["date_opened", "date_closed"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


@st.cache_data(show_spinner=False)
def load_ac_data() -> pd.DataFrame:
    """Carica tutte le AC dal database in un DataFrame (con tutte le colonne)."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM corrective_actions", conn)
    conn.close()

    # conversione date (in oggetti date)
    for col in ["ac_date_opened", "ac_date_required", "ac_end_date", "ac_follow_up_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


def clear_caches():
    load_nc_data.clear()
    load_ac_data.clear()


def get_status_options(df_nc: pd.DataFrame):
    """Lista di stati possibili per la NC."""
    if df_nc is None or df_nc.empty:
        return ["OPEN", "CLOSED", "CANCELLED"]
    vals = sorted(
        {
            str(v).strip()
            for v in df_nc["nonconformance_status"].dropna().tolist()
            if str(v).strip()
        }
    )
    base = ["OPEN", "CLOSED", "CANCELLED"]
    for v in vals:
        if v not in base:
            base.append(v)
    return base


def get_next_nc_number(conn) -> str:
    """
    Genera il prossimo numero NC nel formato NC-<n>-CVT
    leggendo il massimo esistente.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT nonconformance_number FROM nonconformances "
        "WHERE nonconformance_number LIKE 'NC-%-CVT'"
    )
    values = cur.fetchall()
    max_n = 0
    for (val,) in values:
        m = re.match(r"NC-(\d+)-CVT", str(val))
        if m:
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except ValueError:
                continue
    return f"NC-{max_n + 1}-CVT"


def get_next_ac_number(conn) -> int:
    """Restituisce il prossimo numero AC progressivo per l'intero DB."""
    cur = conn.cursor()
    cur.execute("SELECT ac_corrective_action_num FROM corrective_actions")
    nums = []
    for (val,) in cur.fetchall():
        try:
            nums.append(int(val))
        except (TypeError, ValueError):
            continue
    return (max(nums) + 1) if nums else 1


def update_nc_in_db(nc_id: int, values: dict):
    conn = get_connection()
    cur = conn.cursor()
    cols = ", ".join([f"{k} = ?" for k in values.keys()])
    sql = f"UPDATE nonconformances SET {cols} WHERE id = ?"
    params = list(values.values()) + [nc_id]
    cur.execute(sql, params)
    conn.commit()
    conn.close()
    clear_caches()


def insert_nc_in_db(values: dict) -> int:
    """Inserisce una NC e restituisce l'id generato."""
    conn = get_connection()
    cur = conn.cursor()
    columns = ", ".join(values.keys())
    placeholders = ", ".join(["?"] * len(values))
    sql = f"INSERT INTO nonconformances ({columns}) VALUES ({placeholders})"
    cur.execute(sql, list(values.values()))
    nc_id = cur.lastrowid
    conn.commit()
    conn.close()
    clear_caches()
    return nc_id


def update_ac_in_db(ac_id: int, values: dict):
    conn = get_connection()
    cur = conn.cursor()
    cols = ", ".join([f"{k} = ?" for k in values.keys()])
    sql = f"UPDATE corrective_actions SET {cols} WHERE id = ?"
    params = list(values.values()) + [ac_id]
    cur.execute(sql, params)
    conn.commit()
    conn.close()
    clear_caches()


def insert_ac_in_db(nc_id: int, values: dict):
    conn = get_connection()
    cur = conn.cursor()
    values_all = {"nc_id": nc_id}
    values_all.update(values)
    columns = ", ".join(values_all.keys())
    placeholders = ", ".join(["?"] * len(values_all))
    sql = f"INSERT INTO corrective_actions ({columns}) VALUES ({placeholders})"
    cur.execute(sql, list(values_all.values()))
    conn.commit()
    conn.close()
    clear_caches()

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

def ensure_platform_table():
    """Crea la tabella platforms se non esiste e la popola dalle NC esistenti."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS platforms (
            name TEXT PRIMARY KEY
        )
        """
    )
    # popola con le piattaforme già presenti nelle NC
    cur.execute(
        "SELECT DISTINCT piattaforma FROM nonconformances "
        "WHERE piattaforma IS NOT NULL AND TRIM(piattaforma) <> ''"
    )
    for (name,) in cur.fetchall():
        cur.execute(
            "INSERT OR IGNORE INTO platforms (name) VALUES (?)",
            (name.strip(),),
        )
    conn.commit()
    conn.close()


@st.cache_data(show_spinner=False)
def load_platforms() -> list[str]:
    """Ritorna l'elenco delle piattaforme gestite."""
    ensure_platform_table()
    conn = get_connection()
    df = pd.read_sql_query("SELECT name FROM platforms ORDER BY name", conn)
    conn.close()
    return df["name"].tolist()


def add_platform(name: str):
    """Aggiunge una nuova piattaforma."""
    name = (name or "").strip()
    if not name:
        return
    ensure_platform_table()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO platforms (name) VALUES (?)", (name,))
    conn.commit()
    conn.close()
    load_platforms.clear()


# ============================================================
# EMAIL HELPERS
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


def get_nc_number_by_id(nc_id: int) -> str:
    """Ritorna il numero NC a partire dall'id interno."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT nonconformance_number FROM nonconformances WHERE id = ?",
        (nc_id,),
    )
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        return str(row[0])
    return f"ID {nc_id}"


def get_emails_for_nc(nc_id: int) -> list[str]:
    """
    Ricava gli indirizzi email associati a una NC.
    Usa i campi email_address (NC) e ac_email_address (AC).
    """
    emails = set()
    conn = get_connection()
    cur = conn.cursor()

    # email principale sulla NC
    try:
        cur.execute(
            "SELECT email_address FROM nonconformances WHERE id = ?",
            (nc_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            emails.add(row[0])
    except Exception:
        pass

    # email dalle AC collegate
    try:
        cur.execute(
            "SELECT DISTINCT ac_email_address FROM corrective_actions WHERE nc_id = ?",
            (nc_id,),
        )
        for (mail,) in cur.fetchall():
            if mail:
                emails.add(mail)
    except Exception:
        pass

    conn.close()
    return sorted(emails)


def trigger_email_prompt(nc_id: int, operation: str):
    """Imposta lo stato per mostrare il box di invio email."""
    st.session_state["email_nc_id"] = nc_id
    st.session_state["email_operation"] = operation
    st.session_state["show_email_prompt"] = True


def render_email_prompt():
    """Mostra, se necessario, il box per inviare le modifiche agli owner."""
    if not st.session_state.get("show_email_prompt"):
        return

    nc_id = st.session_state.get("email_nc_id")
    operation = st.session_state.get("email_operation", "Aggiornamento NC")
    nc_number = get_nc_number_by_id(nc_id) if nc_id is not None else "n/d"
    emails = get_emails_for_nc(nc_id) if nc_id is not None else []

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
            if st.button("✉️ Sì, invia", key="email_send_yes"):
                if emails:
                    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
                    subject = f"[NC {nc_number}] {operation}"
                    body = (
                        "Gentile Owner,\n\n"
                        f"Sulla Non Conformità n. {nc_number} è stata registrata la seguente attività:\n\n"
                        f"• Operazione: {operation}\n"
                        f"• Data e ora: {now_str}\n\n"
                        "Puoi consultare i dettagli completi nell’applicativo NC Management.\n\n"
                        "Questa è una comunicazione automatica: si prega di non rispondere."
                    )
                    send_email(emails, subject, body)
                    st.success("Email inviata agli owner.")
                st.session_state["show_email_prompt"] = False
        with col2:
            if st.button("❌ No, non inviare", key="email_send_no"):
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

    status_list = sorted(df["nonconformance_status"].dropna().unique().tolist())
    status_selected = st.multiselect("Stato NC", status_list, default=[])
    if status_selected:
        df = df[df["nonconformance_status"].isin(status_selected)]

    resp_list = sorted(df["responsibility"].dropna().unique().tolist())
    responsibility_selected = st.multiselect("Responsabilità", resp_list, default=[])
    if responsibility_selected:
        df = df[df["responsibility"].isin(responsibility_selected)]

    owner_list = sorted(df["owner"].dropna().unique().tolist())
    owner_selected = st.multiselect("Owner", owner_list, default=[])
    if owner_selected:
        df = df[df["owner"].isin(owner_selected)]

    return df


def style_ac_table(df_ac: pd.DataFrame) -> "pd.io.formats.style.Styler":
    """Stile base per la tabella AC."""
    return df_ac.style.set_properties(
        **{"white-space": "nowrap", "text-overflow": "ellipsis", "max-width": "300px"}
    )

def get_display_status(row: pd.Series) -> str:
    """Restituisce lo stato 'visuale' della NC (considerando eventuale parent)."""
    raw = (row.get("nonconformance_status") or "").upper().strip()
    parent_ref = str(row.get("nc_parent_ref") or "").strip()
    if parent_ref:
        return "MANAGED"
    return raw or "NEW"


def status_to_color(status: str) -> str:
    s = (status or "").upper()
    if s in ("NEW", "OPEN"):
        return "#cc0000"  # rosso
    if s == "MANAGED":
        return "#ff8800"  # arancione
    if s in ("CLOSED", "CLOSE", "CLOSED/VERIFIED", "CANCELLED", "CANCELED", "CHIUSA"):
        return "#008000"  # verde
    return "#555555"      # grigio


def render_status_html(status: str) -> str:
    color = status_to_color(status)
    return f"<span style='color:{color}; font-weight:bold'>{status}</span>"


def safe_date_for_input(val):
    """Converte valori vari (stringhe, date, Timestamp, NaT) in date o None."""
    if val is None or val == "":
        return None
    try:
        import pandas as pd  # type: ignore
        if isinstance(val, pd.Timestamp):
            if pd.isna(val):
                return None
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

        df = df_ac.copy()
        df = df.merge(
            df_nc[["id", "nonconformance_number"]].rename(columns={"id": "nc_id"}),
            on="nc_id",
            how="left",
        )

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
        nc_id = int(row["id"])
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

        df_ac_nc = df_ac[df_ac["nc_id"] == nc_id].copy()
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

    # df_list esiste sempre e non causa errori
    if not df_list.empty:
        counts = df_list["display_status"].value_counts()
        recap_parts = [f"{cnt} in stato {stato}" for stato, cnt in counts.items()]
        st.caption(" | ".join(recap_parts))
    else:
        st.caption("Nessuna NC selezionata o disponibile.")

    df_list = df_nc.copy()
    # stato visualizzato (NEW/OPEN/MANAGED/CLOSED...)
    df_list["display_status"] = df_list.apply(get_display_status, axis=1)
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

    for _, r in df_list.iterrows():
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

        if c7.button("Dettaglio", key=f"det_nc_{r['id']}"):
            st.session_state["consulta_mode"] = "detail"
            st.session_state["consulta_nc_id"] = int(r["id"])
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
    nc_id = int(row["id"])

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

        current_status = row.get("nonconformance_status") or "OPEN"
        if current_status not in status_options:
            status_options = [current_status] + status_options
        status = st.selectbox(
            "Stato NC", options=status_options, index=status_options.index(current_status)
        )

        nonconform_priority = st.text_input(
            "Priorità NC", value=row.get("nonconform_priority") or ""
        )
        responsibility = st.text_input(
            "Responsabilità", value=row.get("responsibility") or ""
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
            date_closed = st.date_input("Data chiusura", value=date_closed_val)

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

    df_ac_nc = df_ac[df_ac["nc_id"] == nc_id].copy()

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
        ac_id = int(ac_row["id"])

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
                update_ac_in_db(ac_id, vals_ac)
                st.success("AC aggiornata con successo.")
                trigger_email_prompt(nc_id, f"Modifica AC {ac_row['ac_corrective_action_num']}")

    st.markdown("---")
    st.subheader("➕ Aggiungi nuova AC per questa NC")

    conn = get_connection()
    with conn:
        next_ac_number = get_next_ac_number(conn)
    conn.close()
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

    status_options = get_status_options(df_nc) if not df_nc.empty else [
        "OPEN",
        "CLOSED",
        "CANCELLED",
    ]

    conn = get_connection()
    with conn:
        new_nc_number = get_next_nc_number(conn)
    conn.close()

    st.info(f"Il nuovo numero NC proposto è: **{new_nc_number}**")

    today = date.today()

    with st.form(key="form_inserisci_nc"):
        st.text_input("Numero NC", value=new_nc_number, disabled=True)

        serie = st.text_input("Serie *")
        platforms = load_platforms()
        current_plat = row.get("piattaforma") or ""
        if platforms:
            if current_plat and current_plat not in platforms:
                platforms = [current_plat] + [p for p in platforms if p != current_plat]
            piattaforma = st.selectbox(
                "Piattaforma",
                options=platforms,
                index=platforms.index(current_plat) if current_plat in platforms else 0,
            )
        else:
            piattaforma = st.text_input("Piattaforma", value=current_plat)

        short_description = st.text_input("Short description *")

        status = st.selectbox(
            "Stato NC",
            options=status_options,
            index=status_options.index("OPEN") if "OPEN" in status_options else 0,
        )

        nonconform_priority = st.text_input("Priorità NC")
        responsibility = st.text_input("Responsabilità")
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
                # auto-suggerimento email se campo lasciato vuoto
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
    st.header("📈 Trend NC Quality")

    trend_df = load_trend_data()
    if trend_df.empty:
        st.warning("Nessun dato trend disponibile.")
        return

    trend_df = trend_df.dropna(subset=["data_pubblicazione"])
    if trend_df.empty:
        st.warning("Nessun dato trend con data valida.")
        return

    min_date = trend_df["data_pubblicazione"].min().date()
    max_date = trend_df["data_pubblicazione"].max().date()

    st.caption(f"Dati disponibili da {min_date} a {max_date}")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Da data", value=min_date, min_value=min_date, max_value=max_date
        )
    with col2:
        end_date = st.date_input(
            "A data", value=max_date, min_value=min_date, max_value=max_date
        )

    mask = (trend_df["data_pubblicazione"].dt.date >= start_date) & (
        trend_df["data_pubblicazione"].dt.date <= end_date
    )
    trend_filt = trend_df[mask].copy()

    if trend_filt.empty:
        st.warning("Nessun dato nel range selezionato.")
        return

    value_cols = [
        col for col in trend_filt.columns if col not in ["data_pubblicazione", "year_week"]
    ]
    trend_melt = trend_filt.melt(
        id_vars=["data_pubblicazione"],
        value_vars=value_cols,
        var_name="metrica",
        value_name="valore",
    )

    chart = (
        alt.Chart(trend_melt)
        .mark_line(point=True)
        .encode(
            x="data_pubblicazione:T",
            y="valore:Q",
            color="metrica:N",
            tooltip=["data_pubblicazione:T", "metrica:N", "valore:Q"],
        )
        .properties(width="container", height=400)
    )

    st.altair_chart(chart, use_container_width=True)


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

    st.caption(f"Database: `{DB_PATH}`")

    df_nc = load_nc_data()
    df_ac = load_ac_data()

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

    render_email_prompt()

    # Box di invio email, se richiesto da una delle view
    render_email_prompt()


if __name__ == "__main__":
    main()
