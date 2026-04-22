
# =============================================================================
# PLANNING LOGISTIQUE — Application Streamlit (version SQLite)
# =============================================================================
# Lancement : python -m streamlit run planning_sqlite.py
#
# FICHIERS GÉNÉRÉS :
#   - planning.db         : base SQLite (utilisateurs, logs, configuration)
#
# RÔLES UTILISATEURS :
#   - "admin"          : accès complet (configuration sidebar, gestion comptes, planning)
#   - "planificateur"  : modifie le planning (présences, tableaux, semaine), pas la gestion des comptes
#   - "lecture seule"  : consultation uniquement
# =============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta, datetime
import os
import random
import hashlib
import io
import sqlite3
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
import streamlit.components.v1 as components
import bcrypt
import secrets

MAX_ATTEMPTS = 5
LOCKOUT_MINUTES = 15
SESSION_DAYS = 30

TEST_MODE = True  # Mettre False pour activer la vraie connexion

st.set_page_config(page_title="Planning logistique", layout="wide")

# =============================================================================
# CONFIG SQLITE
# =============================================================================
DB_FILE = "planning.db"
#DB_FILE = "/tmp/planning.db"  # Utilisation d'un fichier temporaire pour éviter les problèmes de permissions

# DEFAULT_USERS = {
#     "admin": {"password": "admin", "role": "admin",         "name": "Administrateur"},
#     "chef":  {"password": "chef",  "role": "planificateur", "name": "Chef d'équipe"},
#     "op":    {"password": "op",    "role": "lecture seule", "name": "Opérateur 1"},
# }

# =============================================================================
# OUTILS BASE DE DONNÉES
# =============================================================================
def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS connection_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                login_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS planning_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                start_date TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS team_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fixed_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_name TEXT NOT NULL,
                assigned_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS rotation_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_name TEXT NOT NULL,
                required_count INTEGER NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS post_restrictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_name TEXT NOT NULL,
                person_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS post_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_name TEXT NOT NULL,
                max_count INTEGER NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_restrictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                person_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_name TEXT NOT NULL,
                day_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                UNIQUE(person_name, day_index)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS planning_main (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                post_name TEXT NOT NULL,
                assigned_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS planning_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                task_name TEXT NOT NULL,
                assigned_name TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                username TEXT PRIMARY KEY,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                locked_until TEXT
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_login_at ON connection_logs(login_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_main_week ON planning_main(week_start)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_tasks_week ON planning_tasks(week_start)")

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, stored_hash: str) -> bool:
    if stored_hash.startswith("$2"):
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    return stored_hash == hashlib.sha256(password.encode()).hexdigest()

def seed_default_users():
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        if row["c"] == 0:
            now = datetime.now().isoformat()
            for username, user in DEFAULT_USERS.items():
                conn.execute("""
                    INSERT INTO users (username, password_hash, role, name, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (username, hash_password(user["password"]), user["role"], user["name"], now))

def init_meta():
    with get_conn() as conn:
        row = conn.execute("SELECT start_date FROM planning_meta WHERE id = 1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO planning_meta (id, start_date) VALUES (1, ?)",
                (date.today().strftime("%Y-%m-%d"),)
            )

init_db()
seed_default_users()
init_meta()

def _get_cookie(name: str):
    cookie_str = st.context.headers.get("Cookie", "")
    for part in cookie_str.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == name:
            return v.strip()
    return None

def _set_auth_cookies(token: str):
    is_https = st.context.headers.get("X-Forwarded-Proto", "http") == "https"
    secure = "; Secure" if is_https else ""
    exp = "Fri, 31 Dec 2099 23:59:59 GMT"
    components.html(f"""
        <script>
        document.cookie = "session_token={token}; path=/; expires={exp}; SameSite=Strict{secure}";
        window.parent.location.reload();
        </script>
    """, height=0)

def _clear_auth_cookies():
    components.html("""
        <script>
        document.cookie = "session_token=; path=/; max-age=0; SameSite=Strict";
        window.parent.location.reload();
        </script>
    """, height=0)

# =============================================================================
# AUTHENTIFICATION
# =============================================================================
def check_rate_limit(username: str) -> tuple:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT attempt_count, locked_until FROM login_attempts WHERE username=?",
            (username,)
        ).fetchone()
    if row is None:
        return True, MAX_ATTEMPTS
    if row["locked_until"]:
        locked_until = datetime.fromisoformat(row["locked_until"])
        if datetime.now() < locked_until:
            return False, 0
        with get_conn() as conn:
            conn.execute("DELETE FROM login_attempts WHERE username=?", (username,))
        return True, MAX_ATTEMPTS
    remaining = MAX_ATTEMPTS - row["attempt_count"]
    return remaining > 0, max(0, remaining)

def record_failed_attempt(username: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT attempt_count FROM login_attempts WHERE username=?", (username,)
        ).fetchone()
        count = (row["attempt_count"] + 1) if row else 1
        conn.execute("""
            INSERT INTO login_attempts (username, attempt_count) VALUES (?, ?)
            ON CONFLICT(username) DO UPDATE SET attempt_count=excluded.attempt_count
        """, (username, count))
        if count >= MAX_ATTEMPTS:
            locked_until = (datetime.now() + timedelta(minutes=LOCKOUT_MINUTES)).isoformat()
            conn.execute(
                "UPDATE login_attempts SET locked_until=? WHERE username=?",
                (locked_until, username)
            )

def reset_attempts(username: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM login_attempts WHERE username=?", (username,))

def create_session(username: str, name: str, role: str) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(days=SESSION_DAYS)).isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO sessions (token, username, name, role, expires_at)
            VALUES (?, ?, ?, ?, ?)
        """, (token, username, name, role, expires_at))
    return token

def get_session(token: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT username, name, role, expires_at FROM sessions WHERE token=?",
            (token,)
        ).fetchone()
    if row is None:
        return None
    if datetime.fromisoformat(row["expires_at"]) < datetime.now():
        delete_session(token)
        return None
    return {"username": row["username"], "name": row["name"], "role": row["role"]}

def delete_session(token: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))

def has_default_passwords() -> bool:
    with get_conn() as conn:
        rows = conn.execute("SELECT username, password_hash FROM users").fetchall()
    defaults = {"admin": "admin", "chef": "chef", "op": "op"}
    for row in rows:
        default_pwd = defaults.get(row["username"])
        if default_pwd and verify_password(default_pwd, row["password_hash"]):
            return True
    return False

def load_users():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT username, password_hash, role, name
            FROM users
            ORDER BY username
        """).fetchall()

    return {
        row["username"]: {
            "password": row["password_hash"],
            "role": row["role"],
            "name": row["name"],
        }
        for row in rows
    }

def create_user(username, name, password, role):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO users (username, password_hash, role, name, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            username,
            hash_password(password),
            role,
            name,
            datetime.now().isoformat()
        ))

def update_user_password(username, password):
    with get_conn() as conn:
        conn.execute("""
            UPDATE users
            SET password_hash = ?
            WHERE username = ?
        """, (hash_password(password), username))

def delete_user(username):
    with get_conn() as conn:
        conn.execute("DELETE FROM users WHERE username = ?", (username,))

def check_login(username, password):
    allowed, remaining = check_rate_limit(username)
    if not allowed:
        return None, "locked"
    with get_conn() as conn:
        row = conn.execute("""
            SELECT username, password_hash, role, name
            FROM users WHERE username = ?
        """, (username,)).fetchone()
    if row and verify_password(password, row["password_hash"]):
        if not row["password_hash"].startswith("$2"):
            with get_conn() as conn:
                conn.execute("UPDATE users SET password_hash=? WHERE username=?",
                             (hash_password(password), row["username"]))
        reset_attempts(username)
        return {"role": row["role"], "name": row["name"]}, None
    record_failed_attempt(username)
    _, remaining = check_rate_limit(username)
    return None, remaining

def log_connexion(username, name, role):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO connection_logs (username, name, role, login_at)
            VALUES (?, ?, ?, ?)
        """, (
            username,
            name,
            role,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

def load_logs():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT username, name, role, login_at
            FROM connection_logs
            ORDER BY id DESC
        """).fetchall()

    return [
        {
            "datetime": row["login_at"],
            "username": row["username"],
            "name": row["name"],
            "role": row["role"],
        }
        for row in rows
    ]

def clear_logs():
    with get_conn() as conn:
        conn.execute("DELETE FROM connection_logs")

def show_login():
    st.markdown("""
        <style>
        [data-testid="stSidebar"]       { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        .block-container {
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }
        .stApp {
            background: #f5f7fa;
        }
        [data-testid="stForm"] {
            background: #ffffff;
            border: 1px solid #e0e0e0 !important;
            border-radius: 16px !important;
            padding: 2rem 2rem 1.5rem 2rem !important;
            box-shadow: 0 4px 24px rgba(0,0,0,0.07);
        }
        </style>
    """, unsafe_allow_html=True)

    # Centrage vertical
    top, mid, bot = st.columns([1, 1, 1])
    st.markdown("<div style='height:20vh'></div>", unsafe_allow_html=True)

    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        st.markdown("""
            <div style="text-align:center; margin-bottom:1.5rem;">
                <div style="font-size:2.5rem; line-height:1;">📋</div>
                <div style="font-size:1.4rem; font-weight:700; color:#1a1a2e; margin-top:0.4rem;">Planning logistique</div>
                <div style="font-size:0.85rem; color:#888; margin-top:0.25rem;">Connectez-vous pour accéder à votre espace</div>
            </div>
        """, unsafe_allow_html=True)

        with st.form("form_login"):
            username = st.text_input("Identifiant", placeholder="Votre identifiant", key="login_username")
            password = st.text_input("Mot de passe", type="password", placeholder="••••••••", key="login_password")
            submitted = st.form_submit_button("Se connecter", use_container_width=True, type="primary")

        if submitted:
            user, info = check_login(username, password)
            if user:
                token = create_session(username, user["name"], user["role"])
                st.session_state["logged_in"] = True
                st.session_state["username"] = username
                st.session_state["user_role"] = user["role"]
                st.session_state["user_name"] = user["name"]
                st.session_state["session_token"] = token
                log_connexion(username, user["name"], user["role"])
                _set_auth_cookies(token)
                st.rerun()
            elif info == "locked":
                st.error(f"Compte verrouillé suite à trop de tentatives. Réessayez dans {LOCKOUT_MINUTES} min.")
            else:
                remaining = info
                if remaining > 0:
                    st.error(f"Identifiant ou mot de passe incorrect — {remaining} tentative(s) restante(s).")
                else:
                    st.error(f"Compte verrouillé pour {LOCKOUT_MINUTES} minutes.")

if TEST_MODE:
    st.session_state["logged_in"] = True
    st.session_state["username"] = "admin"
    st.session_state["user_role"] = "admin"
    st.session_state["user_name"] = "Administrateur"
    st.session_state.setdefault("session_token", "test")
else:
    if "logged_in" not in st.session_state:
        token = _get_cookie("session_token")
        if token:
            session = get_session(token)
            if session:
                st.session_state["logged_in"] = True
                st.session_state["username"] = session["username"]
                st.session_state["user_role"] = session["role"]
                st.session_state["user_name"] = session["name"]
                st.session_state["session_token"] = token
            else:
                st.session_state["logged_in"] = False
        else:
            st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        show_login()
        st.stop()

# =============================================================================
# EN-TÊTE
# =============================================================================
col_title, col_user = st.columns([4, 1])

with col_title:
    st.title("Planning équipe logistique")

with col_user:
    st.markdown(f"👤 **{st.session_state['user_name']}**")
    role_labels = {"admin": "Administrateur", "planificateur": "Planificateur", "lecture seule": "Lecture seule"}
    st.caption(f"Rôle : {role_labels.get(st.session_state['user_role'], st.session_state['user_role'])}")
    if st.button("Déconnexion"):
        token = st.session_state.get("session_token")
        if token:
            delete_session(token)
        for key in ["logged_in", "username", "user_role", "user_name", "session_token"]:
            if key in st.session_state:
                del st.session_state[key]
        _clear_auth_cookies()

is_admin = st.session_state["user_role"] == "admin"
can_plan = st.session_state["user_role"] in ("admin", "planificateur")

if is_admin and has_default_passwords():
    st.warning("⚠️ Des comptes utilisent encore leur mot de passe par défaut. Changez-les dans **Gestion des utilisateurs** avant la mise en ligne.")

if not can_plan:
    st.info("👁️ Vous êtes en mode lecture seule. Contactez un administrateur pour modifier le planning.")

# =============================================================================
# NAVIGATION
# =============================================================================
if st.session_state["user_role"] == "lecture seule":
    st.markdown("""
        <style>
        [data-testid="stSidebar"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        </style>
    """, unsafe_allow_html=True)
    page = "Planning"
elif is_admin:
    page = st.sidebar.radio("Navigation", ["Planning", "Gestion des utilisateurs", "Journal des connexions"], index=0)
else:
    page = "Planning"

# =============================================================================
# GESTION DES UTILISATEURS
# =============================================================================
if page == "Gestion des utilisateurs":
    st.header("Gestion des utilisateurs")
    users = load_users()

    st.subheader("Comptes existants")
    role_labels = {"admin": "Administrateur", "planificateur": "Planificateur", "lecture seule": "Lecture seule"}

    for uname, udata in users.items():
        col_name, col_role, col_pwd, col_del = st.columns([3, 2, 2, 1])

        with col_name:
            st.write(f"**{uname}** — {udata['name']}")

        with col_role:
            st.write(role_labels.get(udata["role"], udata["role"]))

        with col_pwd:
            if st.button("Changer mot de passe", key=f"pwd_btn_{uname}"):
                st.session_state[f"show_pwd_{uname}"] = not st.session_state.get(f"show_pwd_{uname}", False)

        with col_del:
            if uname != st.session_state["username"]:
                if st.button("Supprimer", key=f"del_{uname}"):
                    delete_user(uname)
                    st.success(f"Compte «{uname}» supprimé")
                    st.rerun()
            else:
                st.caption("(vous)")

        if st.session_state.get(f"show_pwd_{uname}", False):
            with st.form(key=f"form_pwd_{uname}"):
                new_pwd = st.text_input("Nouveau mot de passe", type="password", key=f"pwd1_{uname}")
                new_pwd2 = st.text_input("Confirmer le mot de passe", type="password", key=f"pwd2_{uname}")
                if st.form_submit_button("Valider"):
                    if not new_pwd:
                        st.error("Le mot de passe ne peut pas être vide.")
                    elif new_pwd != new_pwd2:
                        st.error("Les mots de passe ne correspondent pas.")
                    else:
                        update_user_password(uname, new_pwd)
                        st.success(f"Mot de passe de «{uname}» modifié.")
                        st.session_state.pop(f"show_pwd_{uname}", None)
                        st.rerun()

    st.markdown("---")

    st.subheader("Ajouter un compte")
    with st.form("form_add_user"):
        new_username = st.text_input("Identifiant")
        new_name = st.text_input("Nom affiché")
        new_password = st.text_input("Mot de passe (8 caractères min.)", type="password")
        new_role = st.selectbox(
            "Rôle",
            ["lecture seule", "planificateur", "admin"],
            format_func=lambda x: {"admin": "Administrateur", "planificateur": "Planificateur", "lecture seule": "Lecture seule"}[x]
        )
        submitted = st.form_submit_button("Créer le compte")
        if submitted:
            username_clean = new_username.strip()
            if not username_clean or not new_name or not new_password:
                st.error("Tous les champs sont obligatoires.")
            elif " " in username_clean:
                st.error("L'identifiant ne doit pas contenir d'espaces.")
            elif username_clean != new_username:
                st.error("L'identifiant ne doit pas commencer ou finir par un espace.")
            elif len(new_password) < 8:
                st.error("Le mot de passe doit contenir au moins 8 caractères.")
            elif username_clean in users:
                st.error(f"L'identifiant «{username_clean}» existe déjà.")
            else:
                create_user(username_clean, new_name, new_password, new_role)
                st.success(f"Compte «{username_clean}» créé avec succès.")
                st.rerun()
    st.stop()

# =============================================================================
# JOURNAL DES CONNEXIONS
# =============================================================================
if page == "Journal des connexions":
    st.header("Journal des connexions")
    logs = load_logs()
    if logs:
        df_logs = pd.DataFrame(logs)
        df_logs.columns = ["Date/Heure", "Identifiant", "Nom", "Rôle"]
        df_logs["Rôle"] = df_logs["Rôle"].map({
            "admin": "Administrateur",
            "planificateur": "Planificateur",
            "lecture seule": "Lecture seule"
        }).fillna(df_logs["Rôle"])
        st.dataframe(df_logs, use_container_width=True)

        if st.button("Effacer le journal"):
            clear_logs()
            st.success("Journal effacé")
            st.rerun()
    else:
        st.info("Aucune connexion enregistrée.")
    st.stop()

# =============================================================================
# SESSION / ÉTAT
# =============================================================================
def init_state():
    defaults = {
        "team": [],
        "start_date": date.today(),
        "fixed_posts": {},
        "rotation_posts": {},
        "post_restrictions": {},
        "post_limit": {},
        "tasks": [],
        "task_restrictions": {},
        "daily_status": {},
        "df_main": pd.DataFrame(),
        "df_tasks": pd.DataFrame(),
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

# =============================================================================
# SAUVEGARDE / CHARGEMENT SQLITE
# =============================================================================
def save_current_planning_tables():
    week_start = st.session_state.start_date.strftime("%Y-%m-%d")
    with get_conn() as conn:
        conn.execute("DELETE FROM planning_main WHERE week_start = ?", (week_start,))
        conn.execute("DELETE FROM planning_tasks WHERE week_start = ?", (week_start,))

        for _, row in st.session_state.df_main.iterrows():
            conn.execute("""
                INSERT INTO planning_main (week_start, post_name, assigned_name)
                VALUES (?, ?, ?)
            """, (week_start, str(row["Poste"]), str(row["Assigné"])))

        for _, row in st.session_state.df_tasks.iterrows():
            conn.execute("""
                INSERT INTO planning_tasks (week_start, task_name, assigned_name)
                VALUES (?, ?, ?)
            """, (week_start, str(row["Tâche"]), str(row["Assigné"])))

def load_current_planning_tables():
    week_start = st.session_state.start_date.strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows_main = conn.execute("""
            SELECT post_name, assigned_name
            FROM planning_main
            WHERE week_start = ?
            ORDER BY id
        """, (week_start,)).fetchall()

        rows_tasks = conn.execute("""
            SELECT task_name, assigned_name
            FROM planning_tasks
            WHERE week_start = ?
            ORDER BY id
        """, (week_start,)).fetchall()

    if rows_main:
        st.session_state.df_main = pd.DataFrame(
            [{"Poste": r["post_name"], "Assigné": r["assigned_name"]} for r in rows_main]
        )
    else:
        st.session_state.df_main = pd.DataFrame()

    if rows_tasks:
        st.session_state.df_tasks = pd.DataFrame(
            [{"Tâche": r["task_name"], "Assigné": r["assigned_name"]} for r in rows_tasks]
        )
    else:
        st.session_state.df_tasks = pd.DataFrame()

def save_data():
    with get_conn() as conn:
        conn.execute("DELETE FROM planning_meta")
        conn.execute(
            "INSERT INTO planning_meta (id, start_date) VALUES (1, ?)",
            (st.session_state.start_date.strftime("%Y-%m-%d"),)
        )

        conn.execute("DELETE FROM team_members")
        for name in st.session_state.team:
            conn.execute("INSERT INTO team_members (name) VALUES (?)", (name,))

        conn.execute("DELETE FROM fixed_posts")
        for post, person in st.session_state.fixed_posts.items():
            conn.execute("""
                INSERT INTO fixed_posts (post_name, assigned_name)
                VALUES (?, ?)
            """, (post, person))

        conn.execute("DELETE FROM rotation_posts")
        for post, count in st.session_state.rotation_posts.items():
            conn.execute("""
                INSERT INTO rotation_posts (post_name, required_count)
                VALUES (?, ?)
            """, (post, count))

        conn.execute("DELETE FROM post_restrictions")
        for post, people in st.session_state.post_restrictions.items():
            for person in people:
                conn.execute("""
                    INSERT INTO post_restrictions (post_name, person_name)
                    VALUES (?, ?)
                """, (post, person))

        conn.execute("DELETE FROM post_limits")
        for post, max_count in st.session_state.post_limit.items():
            conn.execute("""
                INSERT INTO post_limits (post_name, max_count)
                VALUES (?, ?)
            """, (post, max_count))

        conn.execute("DELETE FROM tasks")
        for task in st.session_state.tasks:
            conn.execute("INSERT INTO tasks (task_name) VALUES (?)", (task,))

        conn.execute("DELETE FROM task_restrictions")
        for task, people in st.session_state.task_restrictions.items():
            for person in people:
                conn.execute("""
                    INSERT INTO task_restrictions (task_name, person_name)
                    VALUES (?, ?)
                """, (task, person))

        conn.execute("DELETE FROM daily_status")
        for key, status in st.session_state.daily_status.items():
            person_name, day_index = key.rsplit("_", 1)
            conn.execute("""
                INSERT INTO daily_status (person_name, day_index, status)
                VALUES (?, ?, ?)
            """, (person_name, int(day_index), status))

    save_current_planning_tables()

def load_data():
    with get_conn() as conn:
        row = conn.execute("SELECT start_date FROM planning_meta WHERE id = 1").fetchone()
        if row and row["start_date"]:
            st.session_state.start_date = date.fromisoformat(row["start_date"])

        st.session_state.team = [
            r["name"] for r in conn.execute("SELECT name FROM team_members ORDER BY id").fetchall()
        ]

        st.session_state.fixed_posts = {
            r["post_name"]: r["assigned_name"]
            for r in conn.execute("SELECT post_name, assigned_name FROM fixed_posts ORDER BY id").fetchall()
        }

        st.session_state.rotation_posts = {
            r["post_name"]: r["required_count"]
            for r in conn.execute("SELECT post_name, required_count FROM rotation_posts ORDER BY id").fetchall()
        }

        st.session_state.post_restrictions = {}
        for r in conn.execute("SELECT post_name, person_name FROM post_restrictions ORDER BY id").fetchall():
            st.session_state.post_restrictions.setdefault(r["post_name"], []).append(r["person_name"])

        st.session_state.post_limit = {
            r["post_name"]: r["max_count"]
            for r in conn.execute("SELECT post_name, max_count FROM post_limits ORDER BY id").fetchall()
        }

        st.session_state.tasks = [
            r["task_name"] for r in conn.execute("SELECT task_name FROM tasks ORDER BY id").fetchall()
        ]

        st.session_state.task_restrictions = {}
        for r in conn.execute("SELECT task_name, person_name FROM task_restrictions ORDER BY id").fetchall():
            st.session_state.task_restrictions.setdefault(r["task_name"], []).append(r["person_name"])

        st.session_state.daily_status = {}
        for r in conn.execute("SELECT person_name, day_index, status FROM daily_status ORDER BY id").fetchall():
            st.session_state.daily_status[f"{r['person_name']}_{r['day_index']}"] = r["status"]

    load_current_planning_tables()

load_data()

# =============================================================================
# SIDEBAR — CONFIGURATION
# =============================================================================
with st.sidebar:
    st.header("Configuration")

    if not can_plan:
        st.warning("⛔ Configuration réservée aux administrateurs et planificateurs")
    else:
        team_input = st.text_input(
            "Equipe (noms séparés par des virgules)",
            value=",".join(st.session_state.team)
        )

        fixed_posts_input = st.text_area(
            "Postes fixes (poste:Nom)",
            value="\n".join([f"{k}:{v}" for k, v in st.session_state.fixed_posts.items()])
        )

        rotation_posts_input = st.text_area(
            "Postes rotation (poste:nombre de personnes)",
            value="\n".join([f"{k}:{v}" for k, v in st.session_state.rotation_posts.items()])
        )

        post_restrictions_input = st.text_area(
            "Restrictions poste (poste:Nom1,Nom2 — personnes exclues)",
            value="\n".join([f"{k}:{','.join(v)}" for k, v in st.session_state.post_restrictions.items()])
        )

        post_limit_input = st.text_area(
            "Limite cumul poste (poste:nb max par personne)",
            value="\n".join([f"{k}:{v}" for k, v in st.session_state.post_limit.items()])
        )

        tasks_input = st.text_area(
            "Tâches annexes (une par ligne)",
            value="\n".join(st.session_state.tasks)
        )

        task_restrictions_input = st.text_area(
            "Restrictions tâches (tache:Nom1,Nom2 — personnes exclues)",
            value="\n".join([f"{k}:{','.join(v)}" for k, v in st.session_state.task_restrictions.items()])
        )

        start_date = st.date_input("Début de semaine (lundi)", value=st.session_state.start_date)

        if st.button("Valider configuration"):
            try:
                st.session_state.team = [x.strip() for x in team_input.split(",") if x.strip()]

                st.session_state.fixed_posts = {}
                for l in fixed_posts_input.split("\n"):
                    if ":" in l:
                        p, n = l.split(":", 1)
                        st.session_state.fixed_posts[p.strip()] = n.strip()

                st.session_state.rotation_posts = {}
                for l in rotation_posts_input.split("\n"):
                    if ":" in l:
                        p, n = l.split(":", 1)
                        st.session_state.rotation_posts[p.strip()] = int(n.strip())

                st.session_state.post_restrictions = {}
                for l in post_restrictions_input.split("\n"):
                    if ":" in l:
                        p, n = l.split(":", 1)
                        st.session_state.post_restrictions[p.strip()] = [x.strip() for x in n.split(",") if x.strip()]

                st.session_state.post_limit = {}
                for l in post_limit_input.split("\n"):
                    if ":" in l:
                        p, n = l.split(":", 1)
                        st.session_state.post_limit[p.strip()] = int(n.strip())

                st.session_state.tasks = [x.strip() for x in tasks_input.split("\n") if x.strip()]

                st.session_state.task_restrictions = {}
                for l in task_restrictions_input.split("\n"):
                    if ":" in l:
                        p, n = l.split(":", 1)
                        st.session_state.task_restrictions[p.strip()] = [x.strip() for x in n.split(",") if x.strip()]

                st.session_state.start_date = start_date
                st.session_state.df_main = pd.DataFrame()
                st.session_state.df_tasks = pd.DataFrame()

                save_data()
                st.success("Configuration sauvegardée")
                st.rerun()
            except ValueError:
                st.error("Vérifie les nombres saisis dans les postes de rotation et les limites de cumul.")

# =============================================================================
# CALCUL DES DATES DE LA SEMAINE
# =============================================================================
start = st.session_state.start_date
end = start + timedelta(days=4)
week_label = f"{start.strftime('%d/%m/%Y')} au {end.strftime('%d/%m/%Y')}"
days = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi"]

# =============================================================================
# PRÉSENCE
# =============================================================================
st.header("Présence équipe")
abs_people = []

for p in st.session_state.team:
    st.subheader(p)
    cols = st.columns(5)

    for i, d in enumerate(days):
        key = f"{p}_{i}"
        if key not in st.session_state.daily_status:
            st.session_state.daily_status[key] = "Présent"

        with cols[i]:
            status = st.selectbox(
                d,
                ["Présent", "ABS", "Congé"],
                index=["Présent", "ABS", "Congé"].index(st.session_state.daily_status[key]),
                key=key,
                disabled=not can_plan,
            )
            st.session_state.daily_status[key] = status
            if status != "Présent":
                abs_people.append(p)

# =============================================================================
# GÉNÉRATION PLANNING
# =============================================================================
def generate_planning():
    assigned = {p: 0 for p in st.session_state.team}
    main = []
    tasks = []

    for post, person in st.session_state.fixed_posts.items():
        main.append([post, person])
        if person in assigned:
            assigned[person] += 1

    for post, n in st.session_state.rotation_posts.items():
        limit = st.session_state.post_limit.get(post, 999)

        for _ in range(n):
            valid = [
                p for p in st.session_state.team
                if p not in st.session_state.post_restrictions.get(post, [])
                and assigned[p] < limit
            ]

            if valid:
                min_load = min(assigned[p] for p in valid)
                candidates = [p for p in valid if assigned[p] == min_load]
                person = random.choice(candidates)
            else:
                person = "VACANT"

            main.append([post, person])

            if person in assigned:
                assigned[person] += 1

    for t in st.session_state.tasks:
        valid = [
            p for p in st.session_state.team
            if p not in st.session_state.task_restrictions.get(t, [])
        ]

        if valid:
            min_load = min(assigned[p] for p in valid)
            candidates = [p for p in valid if assigned[p] == min_load]
            person = random.choice(candidates)
        else:
            person = "VACANT"

        tasks.append([t, person])

        if person in assigned:
            assigned[person] += 1

    df_main = pd.DataFrame(main, columns=["Poste", "Assigné"])
    df_tasks = pd.DataFrame(tasks, columns=["Tâche", "Assigné"])
    return df_main, df_tasks

if st.session_state.df_main.empty and st.session_state.df_tasks.empty:
    st.session_state.df_main, st.session_state.df_tasks = generate_planning()
    save_current_planning_tables()

df_main = st.session_state.df_main
df_tasks = st.session_state.df_tasks

# =============================================================================
# EDITION
# =============================================================================
st.subheader(f"Postes principaux — semaine du {week_label}")
df_main = st.data_editor(df_main, use_container_width=True, key="editor_main", disabled=not can_plan)
st.session_state.df_main = df_main

st.subheader(f"Tâches annexes — semaine du {week_label}")
df_tasks = st.data_editor(df_tasks, use_container_width=True, key="editor_tasks", disabled=not can_plan)
st.session_state.df_tasks = df_tasks

# =============================================================================
# CHARGE
# =============================================================================
charge = {p: 0 for p in st.session_state.team}

if "Assigné" in df_main.columns:
    for p in df_main["Assigné"]:
        if p in charge:
            charge[p] += 1

if "Assigné" in df_tasks.columns:
    for p in df_tasks["Assigné"]:
        if p in charge:
            charge[p] += 1

charge_df = pd.DataFrame({
    "Opérateur": list(charge.keys()),
    "Charge": list(charge.values())
})

fig = px.bar(
    charge_df,
    x="Opérateur",
    y="Charge",
    color="Charge",
    color_continuous_scale=["green", "orange", "red"]
)

st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# REDISTRIBUTION ABSENTS
# =============================================================================
if abs_people:
    st.header("Redistribution absents")

    main_r = df_main.copy()
    tasks_r = df_tasks.copy()

    for p in abs_people:
        main_r.loc[main_r["Assigné"] == p, "Assigné"] = "A redistribuer"
        tasks_r.loc[tasks_r["Assigné"] == p, "Assigné"] = "A redistribuer"

    st.dataframe(main_r, use_container_width=True)
    st.dataframe(tasks_r, use_container_width=True)

# =============================================================================
# EXPORT EXCEL
# =============================================================================
def export_excel_bytes():
    wb = Workbook()
    ws = wb.active
    ws.title = "Planning"

    ws.append([f"Planning semaine du {week_label}"])
    ws.append([])

    ws.append(["Postes principaux"])
    for r in dataframe_to_rows(df_main, index=False, header=True):
        ws.append(r)

    ws.append([])

    ws.append(["Tâches annexes"])
    for r in dataframe_to_rows(df_tasks, index=False, header=True):
        ws.append(r)

    for cell in ws[1]:
        cell.font = Font(bold=True)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# =============================================================================
# BOUTONS
# =============================================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    if can_plan and st.button("Sauvegarder"):
        save_data()
        st.success("Données sauvegardées dans SQLite")

with col2:
    st.download_button(
        label="Exporter Excel",
        data=export_excel_bytes(),
        file_name=f"planning_{start.strftime('%Y-%m-%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with col3:
    if can_plan and st.button("Semaine suivante"):
        save_current_planning_tables()
        st.session_state.start_date += timedelta(days=7)
        save_data()
        st.session_state.df_main = pd.DataFrame()
        st.session_state.df_tasks = pd.DataFrame()
        load_current_planning_tables()
        if st.session_state.df_main.empty and st.session_state.df_tasks.empty:
            st.session_state.df_main, st.session_state.df_tasks = generate_planning()
            save_current_planning_tables()
        st.rerun()

with col4:
    if can_plan and st.button("Regénérer planning"):
        st.session_state.df_main, st.session_state.df_tasks = generate_planning()
        save_current_planning_tables()
        st.rerun()
