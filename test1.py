import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import pyodbc
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
        "driver": "ODBC Driver 18 for SQL Server",
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
    driver = cfg.get("driver") or "ODBC Driver 18 for SQL Server"
    encrypt = "yes" if cfg.get("encrypt", True) else "no"
    trust = "yes" if cfg.get("trust_server_cert", True) else "no"

    part = cfg.get(which, {})
    server = part.get("server", "")
    database = part.get("database", "")
    uid = part.get("uid", "")
    pwd = part.get("pwd", "")

    # วงเล็บ { } รอบชื่อไดรเวอร์ต้องเป็น {{ }} ใน f-string
    return (
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"UID={uid};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={trust}"
    )

def open_conn(conn_str: str):
    return pyodbc.connect(conn_str, timeout=10)

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

        # ถ้า checksum/rowcount ต่าง -> สุ่มตัวอย่างความต่างแบบ set-based จาก 2 ฝั่ง
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

    # เปลี่ยนข้อมูลเป็นชุด tuples (แปลงทุกคอลัมน์เป็น string เพื่อเทียบง่าย)
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
    """วาดฟอร์มตั้งค่า (ส่วนกลาง) และคืนค่าคอนฟิกใหม่"""
    cfg_editor = {}
    for db_key, db_label in [("old_db", "ฐานเก่า (OLD)"), ("new_db", "ฐานใหม่ (NEW)")]:
        st.subheader(db_label)
        c1, c2 = st.columns(2)
        with c1:
            server = st.text_input(f"{db_label} - Server", value=cfg[db_key].get("server", ""), key=f"{db_key}_server")
            database = st.text_input(f"{db_label} - Database", value=cfg[db_key].get("database", ""), key=f"{db_key}_database")
        with c2:
            uid = st.text_input(f"{db_label} - User", value=cfg[db_key].get("uid", ""), key=f"{db_key}_uid")
            pwd = st.text_input(f"{db_label} - Password", value=cfg[db_key].get("pwd", ""), type="password", key=f"{db_key}_pwd")

        cfg_editor[db_key] = {"server": server, "database": database, "uid": uid, "pwd": pwd}

    st.subheader("🧩 ตัวเลือกเพิ่มเติม")
    driver = st.text_input("ODBC Driver", value=cfg.get("driver", "ODBC Driver 18 for SQL Server"), key="driver_txt")
    encrypt = st.checkbox("Encrypt", value=cfg.get("encrypt", True), key="encrypt_chk")
    trust = st.checkbox("Trust Server Certificate", value=cfg.get("trust_server_cert", True), key="trust_chk")

    cfg_new = {**cfg, **cfg_editor, "driver": driver, "encrypt": encrypt, "trust_server_cert": trust}
    return cfg_new

def config_editor_ui(cfg: dict):
    """แสดงปุ่มเปิด popup และ fallback เป็น expander อัตโนมัติถ้าเวอร์ชันไม่รองรับ"""
    # ปุ่มเปิดตั้งค่า
    top_bar = st.container()
    with top_bar:
        colA, colB = st.columns([1, 3])
        with colA:
            if "show_config" not in st.session_state:
                st.session_state["show_config"] = False
            if st.button("⚙️ ตั้งค่าเชื่อมต่อฐานข้อมูล"):
                st.session_state["show_config"] = True
        with colB:
            st.caption("กดเพื่อแก้ไข config.json โดยไม่ต้องเปิดไฟล์เอง")

    # ตรวจเวอร์ชันว่ามี st.modal หรือไม่
    use_modal = hasattr(st, "modal")

    if use_modal and st.session_state.get("show_config", False):
        with st.modal("ตั้งค่าเชื่อมต่อฐานข้อมูล"):
            cfg_new = render_config_form(cfg)
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🔌 ทดสอบเชื่อมต่อ OLD"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "old_db")):
                            st.success("OLD: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")
            with col2:
                if st.button("🔌 ทดสอบเชื่อมต่อ NEW"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "new_db")):
                            st.success("NEW: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")
            with col3:
                st.write("")  # spacer
                st.write("")

            colS, colC = st.columns(2)
            with colS:
                if st.button("💾 บันทึก", type="primary"):
                    if save_json(CONFIG_PATH, cfg_new):
                        st.success("บันทึก config.json สำเร็จ")
                        st.session_state["show_config"] = False
                        st.experimental_rerun()
            with colC:
                if st.button("❌ ยกเลิก"):
                    st.session_state["show_config"] = False

    # Fallback: Expander (รองรับทุกเวอร์ชัน)
    if not use_modal:
        with st.expander("⚙️ ตั้งค่าเชื่อมต่อฐานข้อมูล (Expander)"):
            cfg_new = render_config_form(cfg)
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("🔌 ทดสอบ OLD"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "old_db")):
                            st.success("OLD: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")
            with c2:
                if st.button("🔌 ทดสอบ NEW"):
                    try:
                        with open_conn(build_conn_str(cfg_new, "new_db")):
                            st.success("NEW: เชื่อมต่อได้")
                    except Exception as e:
                        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")
            with c3:
                st.write("")

            if st.button("💾 บันทึกการตั้งค่า", type="primary"):
                if save_json(CONFIG_PATH, cfg_new):
                    st.success("บันทึก config.json สำเร็จ")
                    st.experimental_rerun()

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
    tables_editor = st.text_area("แก้ไขรายการตาราง", value=json.dumps(tables, ensure_ascii=False, indent=2), height=200)
    if st.button("💾 บันทึก tables.json"):
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
selected = st.multiselect("เลือกตารางที่ต้องการเปรียบเทียบ", options=options, default=options)

if st.button("เริ่มเปรียบเทียบ", disabled=not (ok_old and ok_new)):
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
                # if res["messages"]:\n                    with st.expander(\"รายละเอียด / คำเตือน\"):\n                        for m in res[\"messages\"]:\n                            st.write(f\"- {m}\")\n\n                if not res[\"ok\"]:\n                    c1, c2 = st.columns(2)\n                    with c1:\n                        st.subheader(\"🔻 อยู่ใน OLD แต่ไม่อยู่ใน NEW (sample)\")\n                        if res[\"only_in_old\"]:\n                            st.dataframe(pd.DataFrame(res[\"only_in_old\"]), use_container_width=True)\n                            csv1 = pd.DataFrame(res[\"only_in_old\"]).to_csv(index=False).encode(\"utf-8-sig\")\n                            st.download_button(\"⬇️ CSV (Only in OLD - sample)\", data=csv1,\n                                               file_name=f\"{tname}_only_in_OLD_sample.csv\", mime=\"text/csv\")\n                        else:\n                            st.caption(\"— ไม่มีตัวอย่าง —\")\n                    with c2:\n                        st.subheader(\"🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (sample)\")\n                        if res[\"only_in_new\"]:\n                            st.dataframe(pd.DataFrame(res[\"only_in_new\"]), use_container_width=True)\n                            csv2 = pd.DataFrame(res[\"only_in_new\"]).to_csv(index=False).encode(\"utf-8-sig\")\n                            st.download_button(\"⬇️ CSV (Only in NEW - sample)\", data=csv2,\n                                               file_name=f\"{tname}_only_in_NEW_sample.csv\", mime=\"text/csv\")\n                        else:\n                            st.caption(\"— ไม่มีตัวอย่าง —\")\n                st.divider()\n\nst.divider()\n\n# ---- Data Preview Section\nst.header(\"👀 ดูข้อมูลตาราง (Data Preview)\")\nprev_cat = st.radio(\"เลือกหมวด\", options=[\"master\", \"transaction\"], horizontal=True, key=\"preview_cat\")\nprev_options = tables.get(prev_cat, [])\ntbl_preview = st.selectbox(\"เลือกตาราง\", options=prev_options, index=0 if prev_options else None)\n\nif tbl_preview and ok_old and ok_new:\n    with open_conn(conn_str_old) as conn_old, open_conn(conn_str_new) as conn_new:\n        cols_old = [c for c, _ in q_columns(conn_old, tbl_preview)]\n        cols_new = [c for c, _ in q_columns(conn_new, tbl_preview)]\n        common_cols = [c for c in cols_old if c in cols_new] or (cols_old or cols_new)\n\n        st.subheader(f\"ตาราง: `{tbl_preview}`\")\n        with st.expander(\"🧩 ตั้งค่าการดึงข้อมูล\", expanded=True):\n            c_l, c_r = st.columns([2, 1])\n            with c_l:\n                picked_cols = st.multiselect(\"เลือกคอลัมน์ (เว้นว่าง = คอลัมน์ร่วมทั้งหมด)\",\n                                             options=common_cols,\n                                             default=common_cols[:min(10, len(common_cols))])\n                where_clause = st.text_input(\"WHERE (ไม่ต้องพิมพ์คำว่า WHERE)\", placeholder=\"เช่น Code='TH' AND IsActive=1\")\n                order_by = st.text_input(\"ORDER BY\", placeholder=\"เช่น Code, Name\")\n            with c_r:\n                top_n = st.number_input(\"TOP (จำนวนแถว)\", min_value=1, max_value=100000, value=200, step=50)\n                st.caption(\"แนะนำ 50–1000 เพื่อแสดงผลเร็ว\")\n\n            run_preview = st.button(\"📄 แสดงข้อมูล (OLD/NEW)\")\n\n        if run_preview:\n            col_old, col_new = st.columns(2)\n            use_cols = picked_cols or common_cols\n\n            with col_old:\n                st.write(\"**OLD**\")\n                try:\n                    df_old = fetch_table_sample(conn_old, tbl_preview, use_cols, where_clause, order_by, top_n)\n                    st.dataframe(df_old, use_container_width=True)\n                    st.download_button(\"⬇️ ดาวน์โหลด CSV (OLD)\",\n                                       data=df_old.to_csv(index=False).encode(\"utf-8-sig\"),\n                                       file_name=f\"{tbl_preview}_OLD.csv\",\n                                       mime=\"text/csv\")\n                except Exception as e:\n                    st.error(f\"ดึงข้อมูล OLD ไม่สำเร็จ: {e}\")\n\n            with col_new:\n                st.write(\"**NEW**\")\n                try:\n                    df_new = fetch_table_sample(conn_new, tbl_preview, use_cols, where_clause, order_by, top_n)\n                    st.dataframe(df_new, use_container_width=True)\n                    st.download_button(\"⬇️ ดาวน์โหลด CSV (NEW)\",\n                                       data=df_new.to_csv(index=False).encode(\"utf-8-sig\"),\n                                       file_name=f\"{tbl_preview}_NEW.csv\",\n                                       mime=\"text/csv\")\n                except Exception as e:\n                    st.error(f\"ดึงข้อมูล NEW ไม่สำเร็จ: {e}\")\n\n        with st.expander(\"🧪 ตัวช่วยเทียบอย่างไว (diff จาก sample ที่ดึงมา)\"):\n            st.caption(\"ใช้การตั้งค่าด้านบน (คอลัมน์/WHERE/ORDER/TOP) เพื่อดึง sample และหาแถวที่ต่างกัน\")\n            if st.button(\"🔍 หาแถวที่ไม่ตรงกัน (from sample)\"):\n                try:\n                    use_cols = picked_cols or common_cols\n                    df_old = fetch_table_sample(conn_old, tbl_preview, use_cols, where_clause, order_by, top_n)\n                    df_new = fetch_table_sample(conn_new, tbl_preview, use_cols, where_clause, order_by, top_n)\n\n                    cols_use = [c for c in use_cols if c in df_old.columns and c in df_new.columns]\n                    if not cols_use:\n                        st.warning(\"ไม่มีคอลัมน์ร่วมสำหรับเทียบ\")\n                    else:\n                        set_old = {tuple(str(x) for x in row) for row in df_old[cols_use].itertuples(index=False, name=None)}\n                        set_new = {tuple(str(x) for x in row) for row in df_new[cols_use].itertuples(index=False, name=None)}\n                        only_old = set_old - set_new\n                        only_new = set_new - set_old\n\n                        def tuples_to_df(tset):\n                            return pd.DataFrame([dict(zip(cols_use, t)) for t in list(tset)])\n\n                        c1, c2 = st.columns(2)\n                        with c1:\n                            st.write(\"🔻 อยู่ใน OLD แต่ไม่อยู่ใน NEW (จาก sample)\")\n                            df1 = tuples_to_df(only_old)\n                            st.dataframe(df1, use_container_width=True)\n                            if not df1.empty:\n                                st.download_button(\"⬇️ CSV (Only in OLD - sample)\",\n                                                   data=df1.to_csv(index=False).encode(\"utf-8-sig\"),\n                                                   file_name=f\"{tbl_preview}_only_in_OLD_sample.csv\",\n                                                   mime=\"text/csv\")\n                        with c2:\n                            st.write(\"🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (จาก sample)\")\n                            df2 = tuples_to_df(only_new)\n                            st.dataframe(df2, use_container_width=True)\n                            if not df2.empty:\n                                st.download_button(\"⬇️ CSV (Only in NEW - sample)\",\n                                                   data=df2.to_csv(index=False).encode(\"utf-8-sig\"),\n                                                   file_name=f\"{tbl_preview}_only_in_NEW_sample.csv\",\n                                                   mime=\"text/csv\")\n                except Exception as e:\n                    st.error(f\"เปรียบเทียบไม่สำเร็จ: {e}\")\nelse:\n    st.info(\"ยังเชื่อมต่อฐานข้อมูลไม่ได้ กรุณาตั้งค่าจากปุ่ม ‘ตั้งค่าเชื่อมต่อฐานข้อมูล’ ด้านบนก่อน\")\n\n# ================================\n# Notes\n# ================================\nst.caption(\n    \"หมายเหตุ: โค้ดนี้ใช้ WITH (NOLOCK) เพื่ออ่านเร็วและลดการล็อก เหมาะกับการตรวจสอบ/อ่านอย่างเดียว \"\n    \"หากต้องการความถูกต้องระดับธุรกรรม 100% ให้พิจารณาเอา NOLOCK ออกตามเหมาะสม.\"\n)\n```

# ### ใช้งานยังไง
# 1. รันแอป (`streamlit run your_app.py`)  
# 2. กดปุ่ม **⚙️ ตั้งค่าเชื่อมต่อฐานข้อมูล** ด้านบน  
#    - ถ้า Streamlit ใหม่พอ จะขึ้น **Popup (Modal)**  
#    - ถ้าเวอร์ชันเก่า จะมี **Expander** ให้ตั้งค่า  
# 3. กด **ทดสอบเชื่อมต่อ** (OLD/NEW) ได้จาก popup/expander  
# 4. กด **บันทึก** แล้วแอปรันต่อได้ทันที (มี `st.experimental_rerun()`)

# > หมายเหตุ: ผมแก้ `build_conn_str` ให้ใช้รูปแบบ f-string ที่ถูกต้องสำหรับ `{driver}` แล้ว (ใช้ `{{ }}` ครอบค่าใน f-string)

# ถ้าอยากให้ทำเป็นหน้าต่าง **แยกหน้า (multi-page)** สำหรับตั้งค่า หรืออยากเพิ่มฟีเจอร์ **ทดสอบสิทธิ์/ROLE/DBCOLLATION** บอกได้เลย เดี๋ยวผมจัดให้ครับ 👍
