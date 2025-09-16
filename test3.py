import json
from pathlib import Path
import streamlit as st
import pyodbc
from typing import List, Dict, Tuple
import pandas as pd  # NEW


# -------------------------------
# หากพบ ImportError เกี่ยวกับ pyodbc/unixODBC บน Mac เช่น:
#   ImportError: ... Library not loaded: /opt/homebrew/opt/unixodbc/lib/libodbc.2.dylib
# ให้ติดตั้ง unixODBC ด้วยคำสั่งนี้ใน Terminal:
#   brew install unixodbc
# แล้วจึงติดตั้ง pyodbc ใหม่ (ถ้าจำเป็น):
#   pip install --force-reinstall pyodbc
# -------------------------------


#----------------------------------------------------


def fetch_table_sample(conn, table_name: str, columns: List[str] | None = None,
                       where: str | None = None, order_by: str | None = None,
                       top: int = 200) -> pd.DataFrame:
    """ดึงข้อมูลตัวอย่างจากตาราง (รองรับเลือกคอลัมน์, where, order by, top)"""
    cols = q_columns(conn, table_name)
    all_cols = [c for c, _ in cols]
    if not all_cols:
        return pd.DataFrame()

    if columns:
        # keep only columns that exist
        use_cols = [c for c in columns if c in all_cols]
        if not use_cols:
            use_cols = all_cols
    else:
        use_cols = all_cols

    col_list = ", ".join(quote_ident(c) for c in use_cols)
    t = quote_ident(table_name)

    where_sql = f" WHERE {where} " if where and where.strip() else ""
    order_sql = f" ORDER BY {order_by} " if order_by and order_by.strip() else ""

    sql = f"SELECT TOP ({top}) {col_list} FROM {t} WITH (NOLOCK){where_sql}{order_sql}"

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        df = pd.DataFrame.from_records(rows, columns=use_cols)
    return df


#-----------------------------------------------------


# -------------------------------
# Paths
# -------------------------------
CONFIG_PATH = Path("config.json")       # ค่าการเชื่อมต่อฐานเก่า/ใหม่ (ไฟล์เดิมจากหน้าตั้งค่า)
TABLES_PATH = Path("tables.json")       # รายชื่อตาราง { "master": [...], "transaction": [...] }

# -------------------------------
# Utilities: Loaders
# -------------------------------
def load_config() -> dict:
    if not CONFIG_PATH.exists():
        st.error(f"ไม่พบไฟล์ตั้งค่า {CONFIG_PATH.resolve()}")
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        st.error(f"โหลด config.json ไม่สำเร็จ: {e}")
        return {}

def load_tables() -> dict:
    if not TABLES_PATH.exists():
        # ใส่ค่า default ให้ก่อน หากผู้ใช้ยังไม่มีไฟล์
        default = {
            "master": ["PNM_Zone", "PNM_Province", "COM_Company", "DOC_DocumentName"],
            "transaction": ["DOC_Header", "DOC_Detail", "PNM_Position_His"]
        }
        TABLES_PATH.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(TABLES_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        st.error(f"โหลด tables.json ไม่สำเร็จ: {e}")
        return {"master": [], "transaction": []}

# -------------------------------
# Utilities: DB
# -------------------------------
def build_conn_str(cfg: dict, which: str) -> str:
    """
    which: 'old_db' หรือ 'new_db'
    ใช้ driver/encrypt/trust_server_cert จาก cfg
    """
    driver = cfg.get("driver") or "ODBC Driver 18 for SQL Server"
    encrypt = "yes" if cfg.get("encrypt", True) else "no"
    trust = "yes" if cfg.get("trust_server_cert", True) else "no"
    part = cfg.get(which, {})
    server = part.get("server", "")
    database = part.get("database", "")
    uid = part.get("uid", "")
    pwd = part.get("pwd", "")
    return (
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"UID={uid};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={trust}"
    )

def open_conn(conn_str: str):
    return pyodbc.connect(conn_str, timeout=10)

def q_columns(conn, table_name: str) -> List[Tuple[str, int]]:
    """
    ดึงรายชื่อคอลัมน์และลำดับ (column_id) จาก sys.columns เพื่อเรียงคอลัมน์ให้ตรงกันเวลา EXCEPT
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
    sql = f"SELECT COUNT_BIG(1) AS cnt FROM {quote_ident(table_name)} WITH (NOLOCK)"
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])

def q_checksum(conn, table_name: str) -> int:
    """
    ใช้ SUM(BINARY_CHECKSUM(*)) เพื่อดูภาพรวมความต่าง
    หมายเหตุ: ไม่เที่ยงตรง 100% แบบ bit-by-bit แต่เร็วและดีพอสำหรับ pre-check
    """
    sql = f"SELECT ISNULL(SUM(BINARY_CHECKSUM(*)), 0) AS cs FROM {quote_ident(table_name)} WITH (NOLOCK)"
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])

def common_columns(conn_old, conn_new, table_name: str) -> List[str]:
    cols_old = [c for c, _ in q_columns(conn_old, table_name)]
    cols_new = [c for c, _ in q_columns(conn_new, table_name)]
    return [c for c in cols_old if c in cols_new]  # รักษาลำดับตาม old

def q_sample_diff(conn_old, conn_new, table_name: str, limit: int = 100) -> Tuple[List[Dict], List[Dict], List[str]]:
    """
    คืนตัวอย่าง rows ต่างกัน 2 ทาง:
      - only_in_old: อยู่ใน Old แต่ไม่มีใน New
      - only_in_new: อยู่ใน New แต่ไม่มีใน Old
    โดยเทียบเฉพาะคอลัมน์ร่วมกัน (เรียงตาม old)
    """
    cols = common_columns(conn_old, conn_new, table_name)
    if not cols:
        return [], [], []

    col_list = ", ".join(quote_ident(c) for c in cols)
    t = quote_ident(table_name)

    sql_old_minus_new = f"""
    SELECT TOP ({limit}) {col_list} FROM {t} WITH (NOLOCK)
    EXCEPT
    SELECT {col_list} FROM {t} WITH (NOLOCK)
    """

    sql_new_minus_old = f"""
    SELECT TOP ({limit}) {col_list} FROM {t} WITH (NOLOCK)
    EXCEPT
    SELECT {col_list} FROM {t} WITH (NOLOCK)
    """

    # ต้องรัน cross-connection: เอา result จาก old เทียบ new และกลับกัน
    only_in_old = exec_except(conn_old, conn_new, sql_old_minus_new, sql_new_minus_old, cols, direction="old_minus_new")
    only_in_new = exec_except(conn_old, conn_new, sql_old_minus_new, sql_new_minus_old, cols, direction="new_minus_old")
    return only_in_old, only_in_new, cols

def exec_except(conn_old, conn_new, sql_old_minus_new, sql_new_minus_old, cols, direction="old_minus_new"):
    """
    ทำ EXCEPT โดยรัน SQL แยก connection:
      - old_minus_new: ดึง rows จาก old, ลบด้วย new
      - new_minus_old: ดึง rows จาก new, ลบด้วย old
    Trick: เราสร้าง temp table ในแต่ละฝั่งไม่สะดวก จึงใช้แนวทางดึง 2 ชุดแล้วเทียบใน Python:
      old_rows - new_rows  หรือ  new_rows - old_rows
    เพื่อให้ compatible โดยไม่ต้องมี linked server
    """
    def fetch_rows(conn, which: str):
        sql = sql_old_minus_new if which == "old" else sql_new_minus_old
        # เปลี่ยนคำสั่งที่สองให้สลับเป็นอีกตาราง (เพราะ sql ใช้ชื่อเดียวกัน)
        # วิธีง่าย: ดึง rows ทั้งสองฝั่งแยกกัน แล้วเทียบ set
        with conn.cursor() as cur:
            cur.execute(sql)
            # แปลง rows → tuple เพื่อใช้ set ได้
            return [tuple(r) for r in cur.fetchall()]

    old_sample = fetch_rows(conn_old, "old")
    new_sample = fetch_rows(conn_new, "new")

    if direction == "old_minus_new":
        rows = set(old_sample) - set(new_sample)
    else:
        rows = set(new_sample) - set(old_sample)

    # คืนเป็น dict list เพื่อแสดงผลง่าย
    out = []
    for tup in list(rows)[:100]:
        out.append({cols[i]: tup[i] for i in range(len(cols))})
    return out

def quote_ident(name: str) -> str:
    # ป้องกันชื่อคอลัมน์/ตารางพิเศษ
    return f"[{name.replace(']', ']]')}]"

# -------------------------------
# Compare Logic
# -------------------------------
def compare_table(conn_old, conn_new, table_name: str) -> dict:
    result = {"table": table_name, "ok": True, "messages": [], "rowcount_old": None,
              "rowcount_new": None, "checksum_old": None, "checksum_new": None,
              "schema_equal": True, "only_in_old": [], "only_in_new": [], "columns_used": []}
    try:
        cols_old = q_columns(conn_old, table_name)
        cols_new = q_columns(conn_new, table_name)

        set_old = {(c.lower(), i) for c, i in cols_old}
        set_new = {(c.lower(), i) for c, i in cols_new}

        # เช็คชื่อคอลัมน์อย่างเดียวพอ (ไม่เทียบ type เพื่อลดซับซ้อน)
        names_old = [c.lower() for c, _ in cols_old]
        names_new = [c.lower() for c, _ in cols_new]
        if names_old != names_new:
            result["schema_equal"] = False
            # รายงานต่าง
            miss_in_new = [c for c in names_old if c not in names_new]
            miss_in_old = [c for c in names_new if c not in names_old]
            if miss_in_new:
                result["messages"].append(f"คอลัมน์ใน OLD ที่ไม่มีใน NEW: {', '.join(miss_in_new)}")
            if miss_in_old:
                result["messages"].append(f"คอลัมน์ใน NEW ที่ไม่มีใน OLD: {', '.join(miss_in_old)}")

        # นับแถว
        result["rowcount_old"] = q_rowcount(conn_old, table_name)
        result["rowcount_new"] = q_rowcount(conn_new, table_name)
        if result["rowcount_old"] != result["rowcount_new"]:
            result["ok"] = False
            result["messages"].append(f"Row count ต่างกัน (OLD={result['rowcount_old']}, NEW={result['rowcount_new']})")

        # checksum
        result["checksum_old"] = q_checksum(conn_old, table_name)
        result["checksum_new"] = q_checksum(conn_new, table_name)
        if result["checksum_old"] != result["checksum_new"]:
            result["ok"] = False
            result["messages"].append("Checksum ต่างกัน")

        # ถ้าพบความต่าง → ดึงตัวอย่างแถวที่ต่าง
        if not result["ok"]:
            only_old, only_new, cols_used = q_sample_diff(conn_old, conn_new, table_name)
            result["only_in_old"] = only_old
            result["only_in_new"] = only_new
            result["columns_used"] = cols_used

        return result
    except Exception as e:
        result["ok"] = False
        result["messages"].append(f"เกิดข้อผิดพลาด: {e}")
        return result

# -------------------------------
# UI
# -------------------------------
st.set_page_config(page_title="DB Compare (Old vs New)", page_icon="🧪", layout="wide")
st.title("🧪 เปรียบเทียบข้อมูล: ฐานเก่า vs ฐานใหม่")

cfg = load_config()
tables = load_tables()

if not cfg:
    st.stop()

conn_str_old = build_conn_str(cfg, "old_db")
conn_str_new = build_conn_str(cfg, "new_db")

col_cfg, col_tbl = st.columns([1, 2])
with col_cfg:
    st.subheader("สถานะการเชื่อมต่อ")
    try:
        with open_conn(conn_str_old) as _:
            st.success("OLD: เชื่อมต่อได้")
    except Exception as e:
        st.error(f"OLD: เชื่อมต่อไม่ได้ - {e}")
    try:
        with open_conn(conn_str_new) as _:
            st.success("NEW: เชื่อมต่อได้")
    except Exception as e:
        st.error(f"NEW: เชื่อมต่อไม่ได้ - {e}")

with col_tbl:
    st.subheader("ชุดตารางที่ต้องการเปรียบเทียบ")
    tab_choice = st.radio("เลือกหมวด", options=["master", "transaction"], horizontal=True)
    table_list = tables.get(tab_choice, [])
    if not table_list:
        st.info(f"ยังไม่พบตารางในหมวด `{tab_choice}` โปรดแก้ไข {TABLES_PATH.name}")
    selected = st.multiselect("เลือกตาราง", options=table_list, default=table_list)

col_btn1, col_btn2 = st.columns([1, 6])
with col_btn1:
    run_compare = st.button("🔍 Compare")

st.divider()

if run_compare and selected:
    with open_conn(conn_str_old) as conn_old, open_conn(conn_str_new) as conn_new:
        for tbl in selected:
            st.markdown(f"### 📄 ตาราง: `{tbl}`")
            res = compare_table(conn_old, conn_new, tbl)

            status = "✅ เหมือนกันทั้งหมด" if (res["ok"] and res["schema_equal"]) else \
                     "🟡 โครงสร้างต่างกันเล็กน้อยแต่ข้อมูลอาจเหมือน" if (res["ok"] and not res["schema_equal"]) else \
                     "❌ พบความแตกต่าง"

            st.write(f"ผลการเปรียบเทียบ: **{status}**")
            # Summary table
            st.write(
                f"- Schema equal: **{res['schema_equal']}**  \n"
                f"- RowCount: OLD = **{res['rowcount_old']}**, NEW = **{res['rowcount_new']}**  \n"
                f"- Checksum: OLD = **{res['checksum_old']}**, NEW = **{res['checksum_new']}**"
            )
            if res["messages"]:
                with st.expander("รายละเอียด / คำเตือน"):
                    for m in res["messages"]:
                        st.write(f"- {m}")

            if not res["ok"]:
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("🔻 Rows in OLD but not in NEW (sample)")
                    if res["only_in_old"]:
                        st.dataframe(res["only_in_old"], use_container_width=True)
                    else:
                        st.caption("— ไม่มีตัวอย่าง —")
                with c2:
                    st.subheader("🔺 Rows in NEW but not in OLD (sample)")
                    if res["only_in_new"]:
                        st.dataframe(res["only_in_new"], use_container_width=True)
                    else:
                        st.caption("— ไม่มีตัวอย่าง —")

            st.divider()

# -------------------------------
# Sidebar: tables.json editor
# -------------------------------
with st.sidebar:
    st.header("📄 tables.json")
    st.caption("แก้ไขรายการตารางที่ต้องการเทียบได้ที่นี่ แล้วกดบันทึก")
    editor = st.text_area("JSON", value=json.dumps(tables, ensure_ascii=False, indent=2), height=300)
    if st.button("💾 บันทึก tables.json"):
        try:
            new_data = json.loads(editor)
            TABLES_PATH.write_text(json.dumps(new_data, ensure_ascii=False, indent=2), encoding="utf-8")
            st.success("บันทึก tables.json เรียบร้อย")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"บันทึกไม่สำเร็จ: {e}")
