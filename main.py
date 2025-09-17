import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional

try:
    import pyodbc
    USE_PYODBC = True
except ImportError:
    import pymssql
    USE_PYODBC = False
import pandas as pd
import streamlit as st

# ================================
# Paths/Constants
# ================================
CONFIG_PATH = Path("config.json")     # มี old_db/new_db/driver/encrypt/trust_server_cert
TABLES_PATH = Path("tables.json")     # {"master":[...], "transaction":[...]}

# ================================
# Base Utils
# ================================
def quote_ident(name: str) -> str:
    """ป้องกันชื่อ object ที่มีอักขระพิเศษ"""
    return f"[{name.replace(']', ']]')}]"

def load_json(path: Path, default) -> dict:
    if not path.exists():
        path.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        st.error(f"โหลดไฟล์ {path.name} ไม่สำเร็จ: {e}")
        return default

def save_json(path: Path, data: dict) -> bool:
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception as e:
        st.error(f"บันทึก {path.name} ไม่สำเร็จ: {e}")
        return False

# ================================
# Config / Tables
# ================================
def load_config() -> dict:
    default_cfg = {
        "old_db": {"server": "", "database": "", "uid": "", "pwd": ""},
        "new_db": {"server": "", "database": "", "uid": "", "pwd": ""},
        # fallback: ใช้ driver 17 ถ้าไม่มีให้ใช้ 'SQL Server'
        "driver": "ODBC Driver 17 for SQL Server",
        "encrypt": True,
        "trust_server_cert": True,
    }
    return load_json(CONFIG_PATH, default_cfg)

def load_tables() -> dict:
    default_tables = {
        "master": ["PNM_Zone", "PNM_Province", "COM_Company", "DOC_DocumentName"],
        "transaction": ["DOC_Header", "DOC_Detail", "PNM_Position_His"],
    }
    return load_json(TABLES_PATH, default_tables)

def build_conn_str(cfg: dict, which: str) -> str:
    """
    which: 'old_db' | 'new_db'
    """
    part = cfg.get(which, {})
    server = part.get("server", "")
    database = part.get("database", "")
    uid = part.get("uid", "")
    pwd = part.get("pwd", "")
    driver = cfg.get("driver") or "ODBC Driver 17 for SQL Server"
    encrypt = "yes" if cfg.get("encrypt", True) else "no"
    trust = "yes" if cfg.get("trust_server_cert", True) else "no"

    if USE_PYODBC:
        import platform
        if driver not in ["ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server", "SQL Server"]:
            driver = "ODBC Driver 17 for SQL Server"
        if platform.system() == "Linux" and driver == "ODBC Driver 17 for SQL Server":
            driver = "SQL Server"
        return (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"UID={uid};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={trust}"
        )
    else:
        # pymssql ไม่ใช้ driver string, ไม่รองรับ encrypt/trust
        return (server, uid, pwd, database)

def open_conn(conn_str):
    if USE_PYODBC:
        return pyodbc.connect(conn_str, timeout=10)
    else:
        server, user, pwd, db = conn_str
        return pymssql.connect(server=server, user=user, password=pwd, database=db, login_timeout=10)

# ================================
# DB Metadata / Quick Checks
# ================================
def q_columns(conn, table_name: str) -> List[Tuple[str, int]]:
    """
    คืน [(column_name, column_id)] เรียงตามลำดับคอลัมน์ในตาราง
    """
    sql = """
    SELECT c.name AS col_name, c.column_id
    FROM sys.columns c
    INNER JOIN sys.objects o ON c.object_id = o.object_id
    WHERE o.type IN ('U') AND o.name = ?
    ORDER BY c.column_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name,))
        return [(r[0], int(r[1])) for r in cur.fetchall()]

def q_rowcount(conn, table_name: str) -> int:
    sql = f"SELECT COUNT_BIG(1) FROM {quote_ident(table_name)} WITH (NOLOCK)"
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])

def q_checksum(conn, table_name: str) -> int:
    """
    SUM(BINARY_CHECKSUM(*)) — เร็วและพอจับต่างได้ (ไม่ใช่ 100% เท่าการเทียบทุกแถวทุกคอลัมน์)
    """
    sql = f"SELECT ISNULL(SUM(BINARY_CHECKSUM(*)), 0) FROM {quote_ident(table_name)} WITH (NOLOCK)"
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])

def common_columns(conn_old, conn_new, table_name: str) -> List[str]:
    cols_old = [c for c, _ in q_columns(conn_old, table_name)]
    cols_new = [c for c, _ in q_columns(conn_new, table_name)]
    return [c for c in cols_old if c in cols_new]  # รักษาลำดับตาม OLD

# ================================
# Compare Logic
# ================================
def compare_table(conn_old, conn_new, table_name: str) -> dict:
    """
    เปรียบเทียบ schema (แค่ชื่อคอลัมน์/ลำดับ), row count, checksum
    ถ้าต่าง -> ดึงตัวอย่างแถวที่ต่าง (จากชุดคอลัมน์ร่วม) ด้วยการเทียบใน Python
    """
    res = {
        "table": table_name,
        "schema_equal": True,
        "rowcount_old": None,
        "rowcount_new": None,
        "checksum_old": None,
        "checksum_new": None,
        "ok": True,                 # true = ไม่มีความต่างด้านข้อมูล (rowcount/checksum เท่ากัน)
        "messages": [],
        "only_in_old": [],
        "only_in_new": [],
        "columns_used": [],
    }

    try:
        cols_old = [c for c, _ in q_columns(conn_old, table_name)]
        cols_new = [c for c, _ in q_columns(conn_new, table_name)]
        if cols_old != cols_new:
            res["schema_equal"] = False
            miss_new = [c for c in cols_old if c not in cols_new]
            miss_old = [c for c in cols_new if c not in cols_old]
            if miss_new:
                res["messages"].append(f"คอลัมน์ใน OLD ที่ไม่มีใน NEW: {', '.join(miss_new)}")
            if miss_old:
                res["messages"].append(f"คอลัมน์ใน NEW ที่ไม่มีใน OLD: {', '.join(miss_old)}")

        res["rowcount_old"] = q_rowcount(conn_old, table_name)
        res["rowcount_new"] = q_rowcount(conn_new, table_name)
        if res["rowcount_old"] != res["rowcount_new"]:
            res["ok"] = False
            res["messages"].append(f"Row count ต่างกัน (OLD={res['rowcount_old']}, NEW={res['rowcount_new']})")

        res["checksum_old"] = q_checksum(conn_old, table_name)
        res["checksum_new"] = q_checksum(conn_new, table_name)
        if res["checksum_old"] != res["checksum_new"]:
            res["ok"] = False
            res["messages"].append("Checksum ต่างกัน")

        if not res["ok"]:
            only_old, only_new, cols_used = sample_row_diffs(conn_old, conn_new, table_name, limit=100)
            res["only_in_old"] = only_old
            res["only_in_new"] = only_new
            res["columns_used"] = cols_used

        return res
    except Exception as e:
        res["ok"] = False
        res["messages"].append(f"เกิดข้อผิดพลาด: {e}")
        return res

def sample_row_diffs(conn_old, conn_new, table: str, limit: int = 100) -> Tuple[List[Dict], List[Dict], List[str]]:
    """
    ดึง sample สองชุดจาก OLD/NEW และหาค่าที่ต่างกันเชิงค่า (เฉพาะคอลัมน์ร่วม)
    """
    cols = common_columns(conn_old, conn_new, table)
    if not cols:
        return [], [], []

    df_old = fetch_table_sample(conn_old, table, columns=cols, top=limit)
    df_new = fetch_table_sample(conn_new, table, columns=cols, top=limit)

    cols_use = [c for c in cols if c in df_old.columns and c in df_new.columns]
    set_old = {tuple(str(x) for x in row) for row in df_old[cols_use].itertuples(index=False, name=None)}
    set_new = {tuple(str(x) for x in row) for row in df_new[cols_use].itertuples(index=False, name=None)}

    only_old = list(set_old - set_new)
    only_new = list(set_new - set_old)

    def to_dicts(tuples_list):
        return [dict(zip(cols_use, t)) for t in tuples_list][:limit]

    return to_dicts(only_old), to_dicts(only_new), cols_use

# ================================
# Data Preview
# ================================
def fetch_table_sample(conn, table: str,
                       columns: Optional[List[str]] = None,
                       where: Optional[str] = None,
                       order_by: Optional[str] = None,
                       top: int = 200) -> pd.DataFrame:
    cols_meta = q_columns(conn, table)
    all_cols = [c for c, _ in cols_meta]
    if not all_cols:
        return pd.DataFrame()

    use_cols = [c for c in (columns or all_cols) if c in all_cols] or all_cols
    col_sql = ", ".join(quote_ident(c) for c in use_cols)
    where_sql = f" WHERE {where} " if where and where.strip() else ""
    order_sql = f" ORDER BY {order_by} " if order_by and order_by.strip() else ""

    sql = f"SELECT TOP ({top}) {col_sql} FROM {quote_ident(table)} WITH (NOLOCK){where_sql}{order_sql}"

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        df = pd.DataFrame.from_records(rows, columns=use_cols)
    return df

# ================================
# UI: Config Editor (Popup / Expander)
# ================================
def render_config_form(cfg: dict) -> dict:
    cfg_editor = {}
    for db_key, db_label in [("old_db", "ฐานเก่า (OLD)"), ("new_db", "ฐานใหม่ (NEW)")]:
        st.subheader(db_label)
        c1, c2 = st.columns(2)
        with c1:
            server = st.text_input(f"{db_label} - Server", value=cfg[db_key].get("server", ""), key=f"cfg_{db_key}_server")
            database = st.text_input(f"{db_label} - Database", value=cfg[db_key].get("database", ""), key=f"cfg_{db_key}_database")
        with c2:
            uid = st.text_input(f"{db_label} - User", value=cfg[db_key].get("uid", ""), key=f"cfg_{db_key}_uid")
            pwd = st.text_input(f"{db_label} - Password", value=cfg[db_key].get("pwd", ""), type="password", key=f"cfg_{db_key}_pwd")

        cfg_editor[db_key] = {"server": server, "database": database, "uid": uid, "pwd": pwd}

    st.subheader("🧩 ตัวเลือกเพิ่มเติม")
    driver = st.text_input("ODBC Driver", value=cfg.get("driver", "ODBC Driver 17 for SQL Server"), key="cfg_driver_txt")
    encrypt = st.checkbox("Encrypt", value=cfg.get("encrypt", True), key="cfg_encrypt_chk")
    trust = st.checkbox("Trust Server Certificate", value=cfg.get("trust_server_cert", True), key="cfg_trust_chk")

    cfg_new = {**cfg, **cfg_editor, "driver": driver, "encrypt": encrypt, "trust_server_cert": trust}
    return cfg_new

def config_editor_ui(cfg: dict):
    # ปุ่มเปิดตั้งค่า
    colA, colB = st.columns([1, 3])
    with colA:
        if "show_config" not in st.session_state:
            st.session_state["show_config"] = False
        if st.button("⚙️ ตั้งค่าเชื่อมต่อฐานข้อมูล", key="btn_open_cfg"):
            st.session_state["show_config"] = True
    with colB:
        st.caption("กดเพื่อแก้ไข config.json โดยไม่ต้องเปิดไฟล์เอง")

    use_modal = hasattr(st, "modal")

    if use_modal and st.session_state.get("show_config", False):
        with st.modal("ตั้งค่าเชื่อมต่อฐานข้อมูล", key="modal_cfg"):
            cfg_new = render_config_form(cfg)
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🔌 ทดสอบเชื่อมต่อ OLD", key="btn_test_old_modal"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "old_db")):
                            st.success("OLD: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")
            with col2:
                if st.button("🔌 ทดสอบเชื่อมต่อ NEW", key="btn_test_new_modal"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "new_db")):
                            st.success("NEW: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")
            with col3:
                st.write("")

            colS, colC = st.columns(2)
            with colS:
                if st.button("💾 บันทึก", type="primary", key="btn_save_cfg_modal"):
                    if save_json(CONFIG_PATH, cfg_new):
                        st.success("บันทึก config.json สำเร็จ")
                        st.session_state["show_config"] = False
                        st.rerun()
            with colC:
                if st.button("❌ ยกเลิก", key="btn_cancel_cfg_modal"):
                    st.session_state["show_config"] = False

    if not use_modal:
        with st.expander("⚙️ ตั้งค่าเชื่อมต่อฐานข้อมูล (Expander)", expanded=False):
            cfg_new = render_config_form(cfg)
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("🔌 ทดสอบ OLD", key="btn_test_old_exp"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "old_db")):
                            st.success("OLD: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")
            with c2:
                if st.button("🔌 ทดสอบ NEW", key="btn_test_new_exp"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "new_db")):
                            st.success("NEW: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")
            with c3:
                st.write("")

            if st.button("💾 บันทึกการตั้งค่า", type="primary", key="btn_save_cfg_exp"):
                if save_json(CONFIG_PATH, cfg_new):
                    st.success("บันทึก config.json สำเร็จ")
                    st.rerun()

# ================================
# Streamlit UI
# ================================
st.set_page_config(page_title="DB Compare (Old vs New)", page_icon="🧪", layout="wide")
st.title("🧪 เปรียบเทียบข้อมูล: ฐานเก่า vs ฐานใหม่")

cfg = load_config()
tables = load_tables()

# ---- CONFIG POPUP / EXPANDER ----
config_editor_ui(cfg)

st.divider()

# ---- Connection Status
st.subheader("สถานะการเชื่อมต่อ")
conn_str_old = build_conn_str(cfg, "old_db")
conn_str_new = build_conn_str(cfg, "new_db")

col_status, col_edit_tables = st.columns([1, 1])
with col_status:
    ok_old = ok_new = False
    try:
        with open_conn(conn_str_old):
            st.success("OLD: เชื่อมต่อได้")
            ok_old = True
    except Exception as e:
        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")

    try:
        with open_conn(conn_str_new):
            st.success("NEW: เชื่อมต่อได้")
            ok_new = True
    except Exception as e:
        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")

with col_edit_tables:
    st.subheader("จัดการ tables.json")
    tables_editor = st.text_area("แก้ไขรายการตาราง", value=json.dumps(tables, ensure_ascii=False, indent=2), height=200, key="tables_editor")
    if st.button("💾 บันทึก tables.json", key="btn_save_tables"):
        try:
            new_tbls = json.loads(tables_editor)
            if save_json(TABLES_PATH, new_tbls):
                st.success("บันทึกสำเร็จ")
                st.experimental_rerun()
        except Exception as e:
            st.error(f"รูปแบบ JSON ไม่ถูกต้อง: {e}")

st.divider()

# ---- Compare Section
st.header("🔍 Compare (Schema/Rows/Checksum)")

tab_choice = st.radio("เลือกหมวด", options=["master", "transaction"], horizontal=True, key="cmp_cat")
options = tables.get(tab_choice, [])
selected = st.multiselect("เลือกตารางที่ต้องการเปรียบเทียบ", options=options, default=options, key="cmp_tables")

if st.button("เริ่มเปรียบเทียบ", disabled=not (ok_old and ok_new), key="btn_compare"):
    if not selected:
        st.info("กรุณาเลือกอย่างน้อย 1 ตาราง")
    else:
        with open_conn(conn_str_old) as conn_old, open_conn(conn_str_new) as conn_new:
            for tname in selected:
                st.markdown(f"### 📄 ตาราง: `{tname}`")
                res = compare_table(conn_old, conn_new, tname)

                if res["ok"] and res["schema_equal"]:
                    status = "✅ เหมือนกันทั้งหมด"
                elif res["ok"] and not res["schema_equal"]:
                    status = "🟡 โครงสร้างต่างกันเล็กน้อย แต่ข้อมูลอาจเหมือน"
                else:
                    status = "❌ พบความแตกต่าง"

                st.write(f"ผลการเปรียบเทียบ: **{status}**")
                st.write(
                    f"- Schema equal: **{res['schema_equal']}**  \n"
                    f"- RowCount: OLD = **{res['rowcount_old']}**, NEW = **{res['rowcount_new']}**  \n"
                    f"- Checksum: OLD = **{res['checksum_old']}**, NEW = **{res['checksum_new']}**"
                )
                if res["messages"]:
                    with st.expander("รายละเอียด / คำเตือน", expanded=False, key=f"warn_{tname}"):
                        for m in res["messages"]:
                            st.write(f"- {m}")

                if not res["ok"]:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.subheader("🔻 อยู่ใน OLD แต่ไม่อยู่ใน NEW (sample)")
                        if res["only_in_old"]:
                            df_only_old = pd.DataFrame(res["only_in_old"])
                            st.dataframe(df_only_old, use_container_width=True, key=f"df_only_old_{tname}")
                            csv1 = df_only_old.to_csv(index=False).encode("utf-8-sig")
                            st.download_button("⬇️ CSV (Only in OLD - sample)", data=csv1,
                                               file_name=f"{tname}_only_in_OLD_sample.csv", mime="text/csv",
                                               key=f"dl_only_old_{tname}")
                        else:
                            st.caption("— ไม่มีตัวอย่าง —")
                    with c2:
                        st.subheader("🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (sample)")
                        if res["only_in_new"]:
                            df_only_new = pd.DataFrame(res["only_in_new"])
                            st.dataframe(df_only_new, use_container_width=True, key=f"df_only_new_{tname}")
                            csv2 = df_only_new.to_csv(index=False).encode("utf-8-sig")
                            st.download_button("⬇️ CSV (Only in NEW - sample)", data=csv2,
                                               file_name=f"{tname}_only_in_NEW_sample.csv", mime="text/csv",
                                               key=f"dl_only_new_{tname}")
                        else:
                            st.caption("— ไม่มีตัวอย่าง —")

                # ===== ตัวอย่างข้อมูลแบบเคียงข้าง (OLD / NEW) =====
                with st.expander("👀 ตัวอย่างข้อมูล (OLD / NEW)", expanded=False, key=f"sample_{tname}"):
                    top_sample = st.number_input(
                        f"จำนวนแถวตัวอย่างสำหรับ {tname}",
                        min_value=1, max_value=10000, value=50, step=50,
                        key=f"top_sample_{tname}"
                    )
                    try:
                        cols_old = [c for c, _ in q_columns(conn_old, tname)]
                        cols_new = [c for c, _ in q_columns(conn_new, tname)]
                        cols_common = [c for c in cols_old if c in cols_new]
                    except Exception as e:
                        cols_common = []
                        st.error(f"ดึงคอลัมน์ไม่สำเร็จ: {e}")

                    if not cols_common:
                        st.warning("ไม่พบคอลัมน์ร่วมระหว่าง OLD/NEW — ไม่สามารถแสดงตัวอย่างข้อมูลได้")
                    else:
                        cfl, cfr = st.columns([2, 1])
                        with cfl:
                            where_quick = st.text_input(
                                "WHERE (ไม่ต้องพิมพ์คำว่า WHERE)",
                                placeholder="เช่น IsActive = 1 AND Code LIKE 'TH%'",
                                key=f"where_sample_{tname}"
                            )
                            order_quick = st.text_input(
                                "ORDER BY",
                                placeholder="เช่น Code, Name",
                                key=f"order_sample_{tname}"
                            )
                        with cfr:
                            st.caption("TIP: ปล่อยว่างได้เพื่อความเร็ว")

                        col_old_prev, col_new_prev = st.columns(2)
                        with col_old_prev:
                            st.write("**OLD**")
                            try:
                                df_old_prev = fetch_table_sample(
                                    conn_old, tname, columns=cols_common,
                                    where=where_quick, order_by=order_quick, top=top_sample
                                )
                                st.dataframe(df_old_prev, use_container_width=True, key=f"df_old_prev_{tname}")
                                st.download_button(
                                    "⬇️ ดาวน์โหลด CSV (OLD - sample)",
                                    data=df_old_prev.to_csv(index=False).encode("utf-8-sig"),
                                    file_name=f"{tname}_OLD_sample.csv",
                                    mime="text/csv",
                                    key=f"dl_old_prev_{tname}"
                                )
                            except Exception as e:
                                st.error(f"ดึงข้อมูล OLD ไม่สำเร็จ: {e}")

                        with col_new_prev:
                            st.write("**NEW**")
                            try:
                                df_new_prev = fetch_table_sample(
                                    conn_new, tname, columns=cols_common,
                                    where=where_quick, order_by=order_quick, top=top_sample
                                )
                                st.dataframe(df_new_prev, use_container_width=True, key=f"df_new_prev_{tname}")
                                st.download_button(
                                    "⬇️ ดาวน์โหลด CSV (NEW - sample)",
                                    data=df_new_prev.to_csv(index=False).encode("utf-8-sig"),
                                    file_name=f"{tname}_NEW_sample.csv",
                                    mime="text/csv",
                                    key=f"dl_new_prev_{tname}"
                                )
                            except Exception as e:
                                st.error(f"ดึงข้อมูล NEW ไม่สำเร็จ: {e}")

                st.divider()

st.divider()

# ---- Data Preview Section
st.header("👀 ดูข้อมูลตาราง (Data Preview)")
prev_cat = st.radio("เลือกหมวด", options=["master", "transaction"], horizontal=True, key="preview_cat")
prev_options = tables.get(prev_cat, [])
tbl_preview = st.selectbox("เลือกตาราง", options=prev_options, index=0 if prev_options else None, key="preview_tbl")

if tbl_preview and ok_old and ok_new:
    with open_conn(conn_str_old) as conn_old, open_conn(conn_str_new) as conn_new:
        cols_old = [c for c, _ in q_columns(conn_old, tbl_preview)]
        cols_new = [c for c, _ in q_columns(conn_new, tbl_preview)]
        common_cols = [c for c in cols_old if c in cols_new] or (cols_old or cols_new)

        st.subheader(f"ตาราง: `{tbl_preview}`")
        with st.expander("🧩 ตั้งค่าการดึงข้อมูล", expanded=True):
            c_l, c_r = st.columns([2, 1])
            with c_l:
                picked_cols = st.multiselect(
                    "เลือกคอลัมน์ (เว้นว่าง = คอลัมน์ร่วมทั้งหมด)",
                    options=common_cols,
                    default=common_cols[:min(10, len(common_cols))],
                    key="preview_cols"
                )
                where_clause = st.text_input("WHERE (ไม่ต้องพิมพ์คำว่า WHERE)", placeholder="เช่น Code='TH' AND IsActive=1", key="preview_where")
                order_by = st.text_input("ORDER BY", placeholder="เช่น Code, Name", key="preview_order")
            with c_r:
                top_n = st.number_input("TOP (จำนวนแถว)", min_value=1, max_value=100000, value=200, step=50, key="preview_topn")
                st.caption("แนะนำ 50–1000 เพื่อแสดงผลเร็ว")

            run_preview = st.button("📄 แสดงข้อมูล (OLD/NEW)", key="btn_run_preview")

        if run_preview:
            col_old, col_new = st.columns(2)
            use_cols = picked_cols or common_cols

            with col_old:
                st.write("**OLD**")
                try:
                    df_old = fetch_table_sample(conn_old, tbl_preview, use_cols, where_clause, order_by, top_n)
                    st.dataframe(df_old, use_container_width=True, key="df_prev_old")
                    st.download_button(
                        "⬇️ ดาวน์โหลด CSV (OLD)",
                        data=df_old.to_csv(index=False).encode("utf-8-sig"),
                        file_name=f"{tbl_preview}_OLD.csv",
                        mime="text/csv",
                        key="dl_prev_old"
                    )
                except Exception as e:
                    st.error(f"ดึงข้อมูล OLD ไม่สำเร็จ: {e}")

            with col_new:
                st.write("**NEW**")
                try:
                    df_new = fetch_table_sample(conn_new, tbl_preview, use_cols, where_clause, order_by, top_n)
                    st.dataframe(df_new, use_container_width=True, key="df_prev_new")
                    st.download_button(
                        "⬇️ ดาวน์โหลด CSV (NEW)",
                        data=df_new.to_csv(index=False).encode("utf-8-sig"),
                        file_name=f"{tbl_preview}_NEW.csv",
                        mime="text/csv",
                        key="dl_prev_new"
                    )
                except Exception as e:
                    st.error(f"ดึงข้อมูล NEW ไม่สำเร็จ: {e}")

        with st.expander("🧪 ตัวช่วยเทียบอย่างไว (diff จาก sample ที่ดึงมา)", expanded=False):
            st.caption("ใช้การตั้งค่าด้านบน (คอลัมน์/WHERE/ORDER/TOP) เพื่อดึง sample และหาแถวที่ต่างกัน")
            if st.button("🔍 หาแถวที่ไม่ตรงกัน (from sample)", key="btn_quickdiff"):
                try:
                    use_cols = picked_cols or common_cols
                    df_old = fetch_table_sample(conn_old, tbl_preview, use_cols, where_clause, order_by, top_n)
                    df_new = fetch_table_sample(conn_new, tbl_preview, use_cols, where_clause, order_by, top_n)

                    cols_use = [c for c in use_cols if c in df_old.columns and c in df_new.columns]
                    if not cols_use:
                        st.warning("ไม่มีคอลัมน์ร่วมสำหรับเทียบ")
                    else:
                        set_old = {tuple(str(x) for x in row) for row in df_old[cols_use].itertuples(index=False, name=None)}
                        set_new = {tuple(str(x) for x in row) for row in df_new[cols_use].itertuples(index=False, name=None)}
                        only_old = set_old - set_new
                        only_new = set_new - set_old

                        def tuples_to_df(tset):
                            return pd.DataFrame([dict(zip(cols_use, t)) for t in list(tset)])

                        c1, c2 = st.columns(2)
                        with c1:
                            st.write("🔻 อยู่ใน OLD แต่ไม่อยู่ใน NEW (จาก sample)")
                            df1 = tuples_to_df(only_old)
                            st.dataframe(df1, use_container_width=True, key="df_prev_only_old")
                            if not df1.empty:
                                st.download_button(
                                    "⬇️ CSV (Only in OLD - sample)",
                                    data=df1.to_csv(index=False).encode("utf-8-sig"),
                                    file_name=f"{tbl_preview}_only_in_OLD_sample.csv",
                                    mime="text/csv",
                                    key="dl_prev_only_old"
                                )
                        with c2:
                            st.write("🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (จาก sample)")
                            df2 = tuples_to_df(only_new)
                            st.dataframe(df2, use_container_width=True, key="df_prev_only_new")
                            if not df2.empty:
                                st.download_button(
                                    "⬇️ CSV (Only in NEW - sample)",
                                    data=df2.to_csv(index=False).encode("utf-8-sig"),
                                    file_name=f"{tbl_preview}_only_in_NEW_sample.csv",
                                    mime="text/csv",
                                    key="dl_prev_only_new"
                                )
                except Exception as e:
                    st.error(f"เปรียบเทียบไม่สำเร็จ: {e}")
else:
    if not ok_old or not ok_new:
        st.info("ยังเชื่อมต่อฐานข้อมูลไม่ได้ กรุณาตั้งค่าจากปุ่ม ‘ตั้งค่าเชื่อมต่อฐานข้อมูล’ ด้านบนก่อน")

# ================================
# Notes
# ================================
st.caption(
    "หมายเหตุ: โค้ดนี้ใช้ WITH (NOLOCK) เพื่ออ่านเร็วและลดการล็อก เหมาะกับการตรวจสอบ/อ่านอย่างเดียว "
    "หากต้องการความถูกต้องระดับธุรกรรม 100% ให้พิจารณาเอา NOLOCK ออกตามเหมาะสม."
)
