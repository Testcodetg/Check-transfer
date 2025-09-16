import json
import hashlib
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from typing import List, Tuple, Optional, Dict
import streamlit as st

st.set_page_config(page_title="DB Comparator", layout="wide")

# -------------------------- Utilities --------------------------

@st.cache_data(show_spinner=False)
def make_engine(db_type: str, **kwargs) -> Engine:
    if db_type == "SQL Server":
        driver = kwargs.get("driver") or "ODBC Driver 17 for SQL Server"
        query = {"driver": driver}
        url = sa.engine.URL.create(
            "mssql+pyodbc",
            username=kwargs.get("username"),
            password=kwargs.get("password"),
            host=kwargs.get("host"),
            port=kwargs.get("port"),
            database=kwargs.get("database"),
            query=query,
        )
    else:
        raise ValueError("Only SQL Server is supported in this demo")
    return sa.create_engine(url, pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def list_tables_from_json(json_file: str) -> Dict[str, List[str]]:
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        st.error(f"ไม่สามารถโหลด JSON ได้: {e}")
        return {}

@st.cache_data(show_spinner=True, ttl=600)
def read_table(engine: Engine, schema: Optional[str], table: str, limit: Optional[int]) -> pd.DataFrame:
    full = f"{schema}.{table}" if schema else table
    q = f"SELECT * FROM {full}"
    df = pd.read_sql_query(sa.text(q), engine)
    if limit:
        return df.head(limit)
    return df

def compare_tables(df_a: pd.DataFrame, df_b: pd.DataFrame, key_cols: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
    res = {}
    meta = {
        "rows_A": len(df_a),
        "rows_B": len(df_b),
        "cols_A": len(df_a.columns),
        "cols_B": len(df_b.columns),
    }
    common_cols = [c for c in df_a.columns if c in set(df_b.columns)]
    df_a_c = df_a[common_cols].copy()
    df_b_c = df_b[common_cols].copy()

    if not key_cols:
        df_a_c["_rowhash"] = df_a_c.apply(lambda row: hashlib.md5("|".join(str(v) for v in row).encode()).hexdigest(), axis=1)
        df_b_c["_rowhash"] = df_b_c.apply(lambda row: hashlib.md5("|".join(str(v) for v in row).encode()).hexdigest(), axis=1)
        key_cols = ["_rowhash"]

    A = df_a_c.set_index(key_cols, drop=False)
    B = df_b_c.set_index(key_cols, drop=False)

    only_in_a = A.loc[~A.index.isin(B.index)]
    only_in_b = B.loc[~B.index.isin(A.index)]

    shared_idx = A.index.intersection(B.index)
    A_shared = A.loc[shared_idx]
    B_shared = B.loc[shared_idx]

    value_cols = [c for c in common_cols if c not in key_cols]
    diffs = []
    for c in value_cols:
        neq = (A_shared[c].astype(str).fillna("") != B_shared[c].astype(str).fillna(""))
        if neq.any():
            tmp = pd.DataFrame({
                "__key__": list(shared_idx),
                "column": c,
                "A": A_shared.loc[neq, c].astype(str).fillna("").values,
                "B": B_shared.loc[neq, c].astype(str).fillna("").values,
            }, index=A_shared.loc[neq].index)
            diffs.append(tmp)
    diff_on_keys = pd.concat(diffs, axis=0) if diffs else pd.DataFrame(columns=["__key__", "column", "A", "B"])

    res["meta"] = pd.DataFrame([meta])
    res["only_in_a"] = only_in_a.reset_index(drop=True)
    res["only_in_b"] = only_in_b.reset_index(drop=True)
    res["diff_on_keys"] = diff_on_keys.reset_index(drop=True)
    return res

# -------------------------- UI --------------------------

st.title("🔍 DB Comparator — เปรียบเทียบข้อมูลระหว่าง 2 ฐานข้อมูล")
st.caption("ปรับ UI ให้กำหนดค่าตามภาพตัวอย่าง: กรอก connection string สำหรับ Old/New และโหลดรายชื่อ table จากไฟล์ JSON")

# ---------- Helpers for connection string (SQL Server style) ----------
from urllib.parse import quote_plus

def mssql_odbc_url_from_kvp(kvp: str, driver: str = "ODBC Driver 17 for SQL Server") -> sa.engine.URL:
    """Convert ADO-style k=v; strings (server=..;database=..;uid=..;pwd=..) to SQLAlchemy pyodbc URL."""
    parts = {}
    for seg in kvp.split(";"):
        seg = seg.strip()
        if not seg:
            continue
        if "=" in seg:
            k, v = seg.split("=", 1)
            parts[k.strip().lower()] = v.strip()
    server = parts.get("server") or parts.get("data source") or "localhost"
    database = parts.get("database") or parts.get("initial catalog") or "master"
    uid = parts.get("uid") or parts.get("user id")
    pwd = parts.get("pwd") or parts.get("password")
    # Build a pyodbc connection string
    odbc_cs = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};TrustServerCertificate=yes;"
    if uid:
        odbc_cs += f"UID={uid};"
    if pwd:
        odbc_cs += f"PWD={pwd};"
    return sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": quote_plus(odbc_cs)})

@st.cache_data(show_spinner=False)
def make_engine_from_connstr(conn_str: str, db_type: str = "SQL Server") -> Engine:
    if db_type == "SQL Server":
        url = mssql_odbc_url_from_kvp(conn_str)
        return sa.create_engine(url, pool_pre_ping=True)
    elif db_type == "SQLite":
        # allow passing path via database=...; in conn_str
        parts = dict(seg.split("=",1) for seg in conn_str.split(";") if "=" in seg)
        return sa.create_engine(sa.engine.URL.create("sqlite", database=parts.get("database","")))
    else:
        raise ValueError("Currently UI แบบนี้รองรับ SQL Server/SQLite; ต้องการ MySQL/Postgres แจ้งได้ครับ")

# ---------- JSON schema for table list ----------
# {
#   "master": ["PNM_Zone", "PNM_Province"],
#   "transaction": ["DocHeader", "DocDetail"],
#   "schema": {"PNM_Zone": "dbo", "DocHeader": "hr"}  # (ถ้ามี) ระบุสคีมาเฉพาะตาราง
# }

# ---------- Settings Dialog (Popup) ----------
try:
    dialog_api = st.dialog  # Streamlit >= 1.32
except Exception:
    dialog_api = st.experimental_dialog  # Fallback for older versions

@dialog_api("ตั้งค่า Database & รายการ Table (JSON)")
def settings_dialog():
    st.write("กรอก **Connection String** สำหรับ Old/New และอัปโหลดไฟล์ JSON รายชื่อ Table")
st.session_state["old_server"] = st.text_input("Old server", value=st.session_state["old_server"], key="dlg_old_server")
st.session_state["old_db"] = st.text_input("Old database", value=st.session_state["old_db"], key="dlg_old_db")
st.session_state["old_uid"] = st.text_input("Old uid", value=st.session_state["old_uid"], key="dlg_old_uid")
st.session_state["old_pwd"] = st.text_input("Old pwd", value=st.session_state["old_pwd"], type="password", key="dlg_old_pwd")
    col = st.columns(2)
    with col[0]:
        st.markdown("**Old Database**")
        st.session_state.setdefault("conn_old", "")
        st.session_state.setdefault("old_server", "")
        st.session_state.setdefault("old_db", "")
        st.session_state.setdefault("old_uid", "")
        st.session_state.setdefault("old_pwd", "")
        st.session_state["old_server"] = st.text_input("Old server", value=st.session_state["old_server"], key="dlg_old_server")
        st.session_state["old_db"] = st.text_input("Old database", value=st.session_state["old_db"], key="dlg_old_db")
        st.session_state["old_uid"] = st.text_input("Old uid", value=st.session_state["old_uid"], key="dlg_old_uid")
        st.session_state["old_pwd"] = st.text_input("Old pwd", value=st.session_state["old_pwd"], type="password", key="dlg_old_pwd")
        st.caption("ตัวอย่าง: server=TG\\MSSQL2017;database=Cyberhm;uid=sa;pwd=xxxx")
    with col[1]:
        st.markdown("**New Database**")
        st.session_state.setdefault("conn_new", "")
        st.session_state["conn_new"] = st.text_input("server=...;database=...;uid=...;pwd=...", value=st.session_state["conn_new"], key="dlg_conn_new")
        st.caption("ตัวอย่าง: server=TG\\MSSQL2017;database=HROpenspaceDB;uid=sa;pwd=xxxx")

    st.markdown("---")
    st.subheader("ไฟล์ JSON รายชื่อ Table")
    up = st.file_uploader("เลือกไฟล์ JSON", type=["json"], key="dlg_json")
    if up is not None:
        import json
        try:
            st.session_state["tables_cfg"] = json.load(up)
            st.success("อ่านไฟล์ JSON สำเร็จ")
        except Exception as e:
            st.error(f"อ่านไฟล์ JSON ไม่ได้: {e}")

    st.markdown("---")
    default_limit = st.number_input("จำกัดจำนวนแถวสูงสุดที่ดึงต่อครั้ง", min_value=100, max_value=500000, value=st.session_state.get("default_limit",50000), step=1000, key="dlg_limit")

    save, cancel = st.columns([1,1])
    with save:
        if st.button("💾 บันทึก & เชื่อมต่อ", use_container_width=True):
            try:
                st.session_state["engA"] = make_engine_from_connstr(st.session_state["conn_old"] or "")
                st.session_state["engB"] = make_engine_from_connstr(st.session_state["conn_new"] or "")
                st.session_state["default_limit"] = st.session_state["dlg_limit"]
                st.session_state["_settings_ok"] = True
                st.success("ตั้งค่า Database สำเร็จ")
                st.rerun()
            except Exception as e:
                st.error(f"เชื่อมต่อไม่สำเร็จ: {e}")
    with cancel:
        if st.button("ยกเลิก", use_container_width=True):
            st.session_state["_settings_open"] = False
            st.rerun()

# Top bar with gear button
bar_l, bar_r = st.columns([6,1])
with bar_l:
    st.caption("ใช้ปุ่ม ⚙️ เพื่อเปิดหน้าตั้งค่า (ไม่ต้องตั้งค่าบ่อย)")
with bar_r:
    if st.button("⚙️ ตั้งค่า", use_container_width=True):
        st.session_state["_settings_open"] = True

if st.session_state.get("_settings_open"):
    settings_dialog()

# เตรียม state
if btn_connect:
    try:
        st.session_state["engA"] = make_engine_from_connstr(conn_old or "")
        st.session_state["engB"] = make_engine_from_connstr(conn_new or "")
        st.success("ตั้งค่า Database สำเร็จ")
    except Exception as e:
        st.error(f"เชื่อมต่อไม่สำเร็จ: {e}")

if json_file is not None:
    try:
        import json
        tables_cfg = json.load(json_file)
        st.session_state["tables_cfg"] = tables_cfg
    except Exception as e:
        st.error(f"อ่านไฟล์ JSON ไม่ได้: {e}")

# Main body (อิงตาม JSON)
engA = st.session_state.get("engA")
engB = st.session_state.get("engB")
tables_cfg = st.session_state.get("tables_cfg") or {}

col1, col2 = st.columns(2)
with col1:
    st.subheader("1. โอนข้อมูล Master / ตรวจสอบ Master")
with col2:
    st.subheader("2. โอนข้อมูล Transaction / ตรวจสอบ Transaction")

master_list = tables_cfg.get("master", [])
tran_list = tables_cfg.get("transaction", [])
schema_map = tables_cfg.get("schema", {})

left, right = st.columns(2)
with left:
    with st.expander("Master (จาก JSON)", expanded=True):
        if not master_list:
            st.info("ยังไม่มีรายชื่อ master ใน JSON")
        for i, tname in enumerate(master_list):
            label = f"• {schema_map.get(tname,'')+'.' if schema_map.get(tname) else ''}{tname}"
            if st.button(label, key=f"json_master_{i}"):
                st.session_state["selected"] = (schema_map.get(tname), tname)

with right:
    with st.expander("Transaction (จาก JSON)", expanded=True):
        if not tran_list:
            st.info("ยังไม่มีรายชื่อ transaction ใน JSON")
        for i, tname in enumerate(tran_list):
            label = f"• {schema_map.get(tname,'')+'.' if schema_map.get(tname) else ''}{tname}"
            if st.button(label, key=f"json_tran_{i}"):
                st.session_state["selected"] = (schema_map.get(tname), tname)

st.markdown("---")
st.subheader("🧰 ตัวเลือกการตรวจสอบ")
st.caption("คลิกชื่อ Table เพื่อดึงจาก Old/New (A/B) โดยใช้ชื่อเดียวกัน")
compare_mode = st.radio("โหมดเปรียบเทียบ", ["Row count only", "Primary key & values", "No key (row hash)"], index=1, horizontal=True)
custom_keys = st.text_input("กำหนดคีย์คอลัมน์ (ถ้ามีหลายคอลัมน์ให้คั่นด้วย ,)", value="")
preview_limit = st.number_input("แสดงผลต่างตัวอย่างสูงสุด (rows)", min_value=10, max_value=5000, value=200, step=10)

selected = st.session_state.get("selected")

if engA and engB and selected:
    sch, tbl = selected
    st.info(f"ตารางที่เลือก: {sch+'.' if sch else ''}{tbl} — ระบบจะดึงจากทั้ง Old(A) และ New(B)")

    pkA = get_primary_key(engA, sch or None, tbl)
    pkB = get_primary_key(engB, sch or None, tbl)

    if custom_keys.strip():
        key_cols = [k.strip() for k in custom_keys.split(",") if k.strip()]
    else:
        key_cols = pkA if pkA else (pkB if pkB else None)
        if compare_mode == "Row count only":
            key_cols = []
        elif compare_mode == "No key (row hash)":
            key_cols = None

    with st.spinner("กำลังดึงข้อมูลจาก Old/New และเปรียบเทียบ..."):
        dfA = read_table(engA, sch or None, tbl, limit=default_limit)
        dfB = read_table(engB, sch or None, tbl, limit=default_limit)

        if compare_mode == "Row count only":
            meta = pd.DataFrame([{ "table": tbl, "rows_A": len(dfA), "rows_B": len(dfB), "equal?": len(dfA)==len(dfB)}])
            st.dataframe(meta, use_container_width=True)
        else:
            res = compare_tables(dfA, dfB, key_cols=key_cols)
            st.markdown("**สรุปผล (Meta)**")
            st.dataframe(res["meta"], use_container_width=True)

            st.markdown("**เฉพาะที่มีใน Old(A) (ตัวอย่าง)**")
            st.dataframe(res["only_in_a"].head(preview_limit), use_container_width=True)

            st.markdown("**เฉพาะที่มีใน New(B) (ตัวอย่าง)**")
            st.dataframe(res["only_in_b"].head(preview_limit), use_container_width=True)

            st.markdown("**คอลัมน์ที่ค่าต่างกันเมื่อ Key ตรงกัน (ตัวอย่าง)**")
            st.dataframe(res["diff_on_keys"].head(preview_limit), use_container_width=True)

elif not (engA and engB):
    st.warning("โปรดกด \"ตั้งค่า Database\" ให้สำเร็จก่อน")
else:
    st.info("อัปโหลด JSON และคลิกชื่อ Table เพื่อเริ่มการตรวจสอบ")

st.markdown("---")
st.caption("รูปแบบ JSON: { 'master': ['PNM_Zone', ...], 'transaction': ['DocHeader', ...], 'schema': {'PNM_Zone': 'dbo'} }
หมายเหตุ: ระบบดึงข้อมูลตาม Limit เพื่อความเร็ว แนะนำระบุคีย์คอลัมน์เพื่อความแม่นยำ")
