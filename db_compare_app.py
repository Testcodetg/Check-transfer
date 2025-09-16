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
# Streamlit UI
# ================================
st.set_page_config(page_title="DB Compare (Old vs New)", page_icon="🧪", layout="wide")
st.title("🧪 เปรียบเทียบข้อมูล: ฐานเก่า vs ฐานใหม่")

cfg = load_config()
tables = load_tables()

# ---- Connection Status
conn_str_old = build_conn_str(cfg, "old_db")
conn_str_new = build_conn_str(cfg, "new_db")

col_status, col_edit_tables = st.columns([1, 1])
with col_status:
    st.subheader("สถานะการเชื่อมต่อ")
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
                if res["messages"]:
                    with st.expander("รายละเอียด / คำเตือน"):
                        for m in res["messages"]:
                            st.write(f"- {m}")

                if not res["ok"]:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.subheader("🔻 อยู่ใน OLD แต่ไม่อยู่ใน NEW (sample)")
                        if res["only_in_old"]:
                            st.dataframe(pd.DataFrame(res["only_in_old"]), use_container_width=True)
                            csv1 = pd.DataFrame(res["only_in_old"]).to_csv(index=False).encode("utf-8-sig")
                            st.download_button("⬇️ CSV (Only in OLD - sample)", data=csv1,
                                               file_name=f"{tname}_only_in_OLD_sample.csv", mime="text/csv")
                        else:
                            st.caption("— ไม่มีตัวอย่าง —")
                    with c2:
                        st.subheader("🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (sample)")
                        if res["only_in_new"]:
                            st.dataframe(pd.DataFrame(res["only_in_new"]), use_container_width=True)
                            csv2 = pd.DataFrame(res["only_in_new"]).to_csv(index=False).encode("utf-8-sig")
                            st.download_button("⬇️ CSV (Only in NEW - sample)", data=csv2,
                                               file_name=f"{tname}_only_in_NEW_sample.csv", mime="text/csv")
                        else:
                            st.caption("— ไม่มีตัวอย่าง —")
                st.divider()

st.divider()

# ---- Data Preview Section
st.header("👀 ดูข้อมูลตาราง (Data Preview)")
prev_cat = st.radio("เลือกหมวด", options=["master", "transaction"], horizontal=True, key="preview_cat")
prev_options = tables.get(prev_cat, [])
tbl_preview = st.selectbox("เลือกตาราง", options=prev_options, index=0 if prev_options else None)

if tbl_preview and ok_old and ok_new:
    with open_conn(conn_str_old) as conn_old, open_conn(conn_str_new) as conn_new:
        cols_old = [c for c, _ in q_columns(conn_old, tbl_preview)]
        cols_new = [c for c, _ in q_columns(conn_new, tbl_preview)]
        common_cols = [c for c in cols_old if c in cols_new] or (cols_old or cols_new)

        st.subheader(f"ตาราง: `{tbl_preview}`")
        with st.expander("🧩 ตั้งค่าการดึงข้อมูล", expanded=True):
            c_l, c_r = st.columns([2, 1])
            with c_l:
                picked_cols = st.multiselect("เลือกคอลัมน์ (เว้นว่าง = คอลัมน์ร่วมทั้งหมด)",
                                             options=common_cols,
                                             default=common_cols[:min(10, len(common_cols))])
                where_clause = st.text_input("WHERE (ไม่ต้องพิมพ์คำว่า WHERE)", placeholder="เช่น Code='TH' AND IsActive=1")
                order_by = st.text_input("ORDER BY", placeholder="เช่น Code, Name")
            with c_r:
                top_n = st.number_input("TOP (จำนวนแถว)", min_value=1, max_value=100000, value=200, step=50)
                st.caption("แนะนำ 50–1000 เพื่อแสดงผลเร็ว")

            run_preview = st.button("📄 แสดงข้อมูล (OLD/NEW)")

        if run_preview:
            col_old, col_new = st.columns(2)
            use_cols = picked_cols or common_cols

            with col_old:
                st.write("**OLD**")
                try:
                    df_old = fetch_table_sample(conn_old, tbl_preview, use_cols, where_clause, order_by, top_n)
                    st.dataframe(df_old, use_container_width=True)
                    st.download_button("⬇️ ดาวน์โหลด CSV (OLD)",
                                       data=df_old.to_csv(index=False).encode("utf-8-sig"),
                                       file_name=f"{tbl_preview}_OLD.csv",
                                       mime="text/csv")
                except Exception as e:
                    st.error(f"ดึงข้อมูล OLD ไม่สำเร็จ: {e}")

            with col_new:
                st.write("**NEW**")
                try:
                    df_new = fetch_table_sample(conn_new, tbl_preview, use_cols, where_clause, order_by, top_n)
                    st.dataframe(df_new, use_container_width=True)
                    st.download_button("⬇️ ดาวน์โหลด CSV (NEW)",
                                       data=df_new.to_csv(index=False).encode("utf-8-sig"),
                                       file_name=f"{tbl_preview}_NEW.csv",
                                       mime="text/csv")
                except Exception as e:
                    st.error(f"ดึงข้อมูล NEW ไม่สำเร็จ: {e}")

        with st.expander("🧪 ตัวช่วยเทียบอย่างไว (diff จาก sample ที่ดึงมา)"):
            st.caption("ใช้การตั้งค่าด้านบน (คอลัมน์/WHERE/ORDER/TOP) เพื่อดึง sample และหาแถวที่ต่างกัน")
            if st.button("🔍 หาแถวที่ไม่ตรงกัน (from sample)"):
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
                            st.dataframe(df1, use_container_width=True)
                            if not df1.empty:
                                st.download_button("⬇️ CSV (Only in OLD - sample)",
                                                   data=df1.to_csv(index=False).encode("utf-8-sig"),
                                                   file_name=f"{tbl_preview}_only_in_OLD_sample.csv",
                                                   mime="text/csv")
                        with c2:
                            st.write("🔺 อยู่ใน NEW แต่ไม่อยู่ใน OLD (จาก sample)")
                            df2 = tuples_to_df(only_new)
                            st.dataframe(df2, use_container_width=True)
                            if not df2.empty:
                                st.download_button("⬇️ CSV (Only in NEW - sample)",
                                                   data=df2.to_csv(index=False).encode("utf-8-sig"),
                                                   file_name=f"{tbl_preview}_only_in_NEW_sample.csv",
                                                   mime="text/csv")
                except Exception as e:
                    st.error(f"เปรียบเทียบไม่สำเร็จ: {e}")
else:
    if not ok_old or not ok_new:
        st.info("ยังเชื่อมต่อฐานข้อมูลไม่ได้ กรุณาตรวจสอบ config.json จากหน้า 'ตั้งค่าเชื่อมต่อ' ที่คุณทำไว้ก่อนหน้า")

# ================================
# Notes
# ================================
st.caption(
    "หมายเหตุ: โค้ดนี้ใช้ WITH (NOLOCK) เพื่ออ่านเร็วและลดการล็อก เหมาะกับการตรวจสอบ/อ่านอย่างเดียว "
    "หากต้องการความถูกต้องระดับธุรกรรม 100% ให้พิจารณาเอา NOLOCK ออกตามเหมาะสม."
)
