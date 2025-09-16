import json
import pyodbc
import streamlit as st
from pathlib import Path

# -------------------------------
# Utilities
# -------------------------------
DEFAULT_CONFIG_PATH = Path("config.json")

def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict:
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"โหลดไฟล์ config ไม่สำเร็จ: {e}")
    # default empty config
    return {
        "old_db": {"server": "", "database": "", "uid": "", "pwd": ""},
        "new_db": {"server": "", "database": "", "uid": "", "pwd": ""},
        "driver": "",
        "encrypt": True,
        "trust_server_cert": True
    }

def save_config(cfg: dict, path: Path = DEFAULT_CONFIG_PATH):
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        st.success(f"บันทึกค่าเรียบร้อย: {path}")
    except Exception as e:
        st.error(f"บันทึกไฟล์ไม่สำเร็จ: {e}")

def available_sql_drivers():
    # คืนรายชื่อ ODBC Drivers ที่มีในเครื่อง
    try:
        drivers = pyodbc.drivers()
        # ให้ driver รุ่นใหม่ขึ้นมาอยู่ก่อน (18 > 17 > 13 > SQL Server)
        desired_order = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server",
                         "ODBC Driver 13 for SQL Server", "SQL Server"]
        sorted_list = [d for d in desired_order if d in drivers] + [d for d in drivers if d not in desired_order]
        return sorted_list
    except Exception:
        return ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]

def build_conn_str(driver: str, server: str, database: str, uid: str, pwd: str,
                   encrypt: bool = True, trust_server_cert: bool = True) -> str:
    # ข้อควรทราบ:
    # - ODBC Driver 18 เปิด Encrypt=Yes เป็นค่า default (ควรกำหนดให้ชัด)
    # - หากไม่มีใบรับรอง TLS ภายใน ใช้ TrustServerCertificate=Yes เพื่อทดสอบภายในองค์กร
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"UID={uid}",
        f"PWD={pwd}",
    ]
    if encrypt:
        parts.append("Encrypt=yes")
    else:
        parts.append("Encrypt=no")
    if trust_server_cert:
        parts.append("TrustServerCertificate=yes")
    conn_str = ";".join(parts)
    return conn_str

def test_connection(name: str, conn_str: str) -> bool:
    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            # คำสั่งเบาๆ เพื่อยืนยันว่า query ได้
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        st.success(f"✅ {name}: เชื่อมต่อฐานข้อมูลสำเร็จ")
        return True
    except Exception as e:
        st.error(f"❌ {name}: เชื่อมต่อฐานข้อมูลไม่สำเร็จ")
        st.caption(f"รายละเอียด: {e}")
        return False


# -------------------------------
# UI
# -------------------------------
st.set_page_config(page_title="Database Connection Tester", page_icon="🗄️", layout="centered")
st.title("🗄️ ทดสอบการเชื่อมต่อฐานข้อมูล (Old / New)")

cfg = load_config()

with st.expander("⚙️ ตั้งค่า Driver และความปลอดภัย", expanded=True):
    drivers = available_sql_drivers()
    cfg["driver"] = st.selectbox("ODBC Driver", options=drivers, index=(drivers.index(cfg.get("driver")) if cfg.get("driver") in drivers else 0))
    col_enc, col_tsc = st.columns(2)
    with col_enc:
        cfg["encrypt"] = st.checkbox("Encrypt (แนะนำ: เปิด)", value=cfg.get("encrypt", True))
    with col_tsc:
        cfg["trust_server_cert"] = st.checkbox("TrustServerCertificate (ทดสอบภายใน)", value=cfg.get("trust_server_cert", True))
    st.caption("หมายเหตุ: ถ้าเจอปัญหา SSL/TLS ในเครือข่ายภายใน ให้เปิด TrustServerCertificate เพื่อทดสอบได้เร็วขึ้น")

st.subheader("ฐานข้อมูลเก่า (Old)")
col1, col2 = st.columns(2)
with col1:
    cfg["old_db"]["server"] = st.text_input("Server (เช่น 20240329-3408 หรือ 10.0.0.5 หรือ SRV\\INSTANCE)", value=cfg["old_db"].get("server", ""))
    cfg["old_db"]["database"] = st.text_input("Database", value=cfg["old_db"].get("database", ""))
with col2:
    cfg["old_db"]["uid"] = st.text_input("UID", value=cfg["old_db"].get("uid", ""), key="old_uid")
    cfg["old_db"]["pwd"] = st.text_input("PWD", value=cfg["old_db"].get("pwd", ""), type="password", key="old_pwd")

st.subheader("ฐานข้อมูลใหม่ (New)")
col3, col4 = st.columns(2)
with col3:
    cfg["new_db"]["server"] = st.text_input("Server (เช่น 2024SRV\\MSSQL2022)", value=cfg["new_db"].get("server", ""))
    cfg["new_db"]["database"] = st.text_input("Database", value=cfg["new_db"].get("database", ""))
with col4:
    cfg["new_db"]["uid"] = st.text_input("UID", value=cfg["new_db"].get("uid", ""), key="new_uid")
    cfg["new_db"]["pwd"] = st.text_input("PWD", value=cfg["new_db"].get("pwd", ""), type="password", key="new_pwd")

# ปุ่มต่างๆ
col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
with col_btn1:
    if st.button("💾 บันทึก config.json"):
        save_config(cfg)
with col_btn2:
    if st.button("📂 โหลด config.json"):
        cfg = load_config()
        st.experimental_rerun()
with col_btn3:
    if st.button("🔌 ทดสอบฐานเก่า (Old)"):
        conn_old = build_conn_str(
            driver=cfg["driver"],
            server=cfg["old_db"]["server"],
            database=cfg["old_db"]["database"],
            uid=cfg["old_db"]["uid"],
            pwd=cfg["old_db"]["pwd"],
            encrypt=cfg.get("encrypt", True),
            trust_server_cert=cfg.get("trust_server_cert", True),
        )
        test_connection("ฐานเก่า", conn_old)
with col_btn4:
    if st.button("🔌 ทดสอบฐานใหม่ (New)"):
        conn_new = build_conn_str(
            driver=cfg["driver"],
            server=cfg["new_db"]["server"],
            database=cfg["new_db"]["database"],
            uid=cfg["new_db"]["uid"],
            pwd=cfg["new_db"]["pwd"],
            encrypt=cfg.get("encrypt", True),
            trust_server_cert=cfg.get("trust_server_cert", True),
        )
        test_connection("ฐานใหม่", conn_new)

st.divider()
if st.button("🧪 ทดสอบทั้งคู่ (Old & New)"):
    conn_old = build_conn_str(
        driver=cfg["driver"],
        server=cfg["old_db"]["server"],
        database=cfg["old_db"]["database"],
        uid=cfg["old_db"]["uid"],
        pwd=cfg["old_db"]["pwd"],
        encrypt=cfg.get("encrypt", True),
        trust_server_cert=cfg.get("trust_server_cert", True),
    )
    conn_new = build_conn_str(
        driver=cfg["driver"],
        server=cfg["new_db"]["server"],
        database=cfg["new_db"]["database"],
        uid=cfg["new_db"]["uid"],
        pwd=cfg["new_db"]["pwd"],
        encrypt=cfg.get("encrypt", True),
        trust_server_cert=cfg.get("trust_server_cert", True),
    )
    ok_old = test_connection("ฐานเก่า", conn_old)
    ok_new = test_connection("ฐานใหม่", conn_new)
    if ok_old and ok_new:
        st.success("🎉 ทั้งฐานเก่าและฐานใหม่ เชื่อมต่อสำเร็จ")
    else:
        st.info("ตรวจสอบรายละเอียด error ที่แสดงด้านบนเพื่อแก้ไข")

# แสดง connection string (ซ่อนรหัสผ่าน)
with st.expander("🔍 ดู Connection String (ซ่อนรหัสผ่าน)"):
    def mask(s: str) -> str:
        return "*" * len(s) if s else ""
    st.code(
        "OLD: "
        + build_conn_str(cfg["driver"], cfg["old_db"]["server"], cfg["old_db"]["database"],
                         cfg["old_db"]["uid"], mask(cfg["old_db"]["pwd"]),
                         cfg.get("encrypt", True), cfg.get("trust_server_cert", True))
        + "\n"
        + "NEW: "
        + build_conn_str(cfg["driver"], cfg["new_db"]["server"], cfg["new_db"]["database"],
                         cfg["new_db"]["uid"], mask(cfg["new_db"]["pwd"]),
                         cfg.get("encrypt", True), cfg.get("trust_server_cert", True))
    )
