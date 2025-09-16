import json
import pyodbc
import pandas as pd
import streamlit as st

# ========================== CONFIG UTILS ==========================
CONFIG_PATH = "config.json"
DEFAULT_CONFIG = {
    "server": "",
    "database": "",
    "uid": "",
    "pwd": "",
    "timeout": 5,
    "trust_server_certificate": True,
    "driver": "",          # ว่าง = ให้ auto เลือก (18 -> 17)
    "windows_auth": False, # True = ใช้ Windows Auth (ไม่ต้องกรอก uid/pwd)
    "port": ""             # ใส่เฉพาะถ้าจะใช้ host,port
}

def load_config(path=CONFIG_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception:
        return DEFAULT_CONFIG.copy()

def save_config(cfg, path=CONFIG_PATH):
    safe_cfg = cfg.copy()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(safe_cfg, f, ensure_ascii=False, indent=2)

def pick_driver(preferred: str | None = None) -> str | None:
    drivers = [d.strip() for d in pyodbc.drivers()]
    if preferred and preferred in drivers:
        return preferred
    for name in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
        if name in drivers:
            return name
    for d in drivers:
        if "SQL Server" in d:
            return d
    return None

def build_conn_str(cfg: dict) -> tuple[str, int]:
    # driver
    driver = pick_driver(cfg.get("driver") or None)
    if not driver:
        raise RuntimeError("ไม่พบ ODBC Driver ของ SQL Server ในเครื่องนี้ (แนะนำติดตั้ง ODBC Driver 18 หรือ 17)")

    # server (host\instance | host,port)
    server = (cfg.get("server") or "").strip()
    port = str(cfg.get("port") or "").strip()
    if port and ("\\" not in server) and ("," not in server):
        server = f"{server},{port}"

    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}"]
    if cfg.get("database"):
        parts.append(f"DATABASE={cfg['database']}")

    if cfg.get("windows_auth"):
        parts.append("Trusted_Connection=yes")
    else:
        uid = cfg.get("uid") or ""
        pwd = cfg.get("pwd") or ""
        if not uid or not pwd:
            raise ValueError("โหมด SQL Auth ต้องกรอก uid/pwd")
        parts.append(f"UID={uid}")
        parts.append(f"PWD={pwd}")

    timeout = int(cfg.get("timeout") or 5)
    parts.append(f"Connection Timeout={timeout}")

    if cfg.get("trust_server_certificate", True):
        parts.append("TrustServerCertificate=yes")

    if "encrypt" in cfg:
        parts.append(f"Encrypt={'yes' if cfg['encrypt'] else 'no'}")

    conn_str = ";".join(parts)
    return conn_str, timeout

def diagnose_error_text(msg: str) -> str:
    tips = []
    if "IM002" in msg:
        tips += [
            "- ไม่พบชื่อ DRIVER/DSN: ตรวจสอบชื่อ driver ให้ตรงกับที่เครื่องมีอยู่จริงด้วย `pyodbc.drivers()`",
            "- ติดตั้ง Microsoft ODBC Driver 18/17 ให้ตรงกับบิตของ Python (32/64-bit)",
        ]
    if "28000" in msg or "Login failed" in msg:
        tips += [
            "- UID/PWD ไม่ถูกต้อง หรือสิทธิ์ฐานข้อมูลไม่พอ",
            "- ถ้าใช้ Windows Auth ให้ตั้ง `windows_auth: true` และไม่ใส่ UID/PWD",
        ]
    if "08001" in msg or "10060" in msg or "53" in msg:
        tips += [
            "- ติดต่อเซิร์ฟเวอร์ไม่ได้: ตรวจสอบ SERVER/PORT/INSTANCE, เปิด TCP/IP, และ Firewall พอร์ต 1433",
        ]
    if "SSL" in msg or "TLS" in msg or "certificate" in msg:
        tips += [
            "- ปัญหา TLS/ใบรับรอง: ลองติ๊ก Trust Server Certificate (ใช้ชั่วคราวสำหรับ dev)",
            "- โปรดติดตั้งใบรับรองที่ถูกต้องถ้าเป็นโปรดักชัน และพิจารณา Encrypt=yes",
        ]
    if not tips:
        tips.append("- ตรวจสอบค่า config, driver, และเวอร์ชัน ODBC/Python ให้สอดคล้องกัน")
    return "\n".join(tips)

# ========================== UI: SETTINGS DIALOG ==========================
try:
    dialog_decorator = st.dialog  # Streamlit >= 1.32
except AttributeError:
    dialog_decorator = st.experimental_dialog  # Streamlit < 1.32

@dialog_decorator("⚙️ ตั้งค่าการเชื่อมต่อ SQL Server")
def settings_dialog():
    cfg = st.session_state.cfg

    st.markdown("กรอกค่าที่จำเป็น แล้วกด **บันทึก**")
    with st.form("settings_form"):
        col1, col2 = st.columns(2)
        with col1:
            server = st.text_input("Server", value=cfg.get("server") or "", placeholder="เช่น TG\\MSSQL2017 หรือ 10.0.0.5")
            database = st.text_input("Database", value=cfg.get("database") or "", placeholder="เช่น Cyberhm")
            port = st.text_input("Port (ไม่บังคับ)", value=str(cfg.get("port") or ""))
        with col2:
            windows_auth = st.toggle("Windows Authentication (Trusted_Connection)", value=bool(cfg.get("windows_auth", False)))
            uid = st.text_input("User (UID)", value=cfg.get("uid") or "", disabled=windows_auth)
            pwd = st.text_input("Password (PWD)", value=cfg.get("pwd") or "", type="password", disabled=windows_auth)

        col3, col4 = st.columns(2)
        with col3:
            timeout = st.number_input("Connection Timeout (วินาที)", min_value=1, max_value=60, value=int(cfg.get("timeout") or 5))
            trust = st.toggle("Trust Server Certificate", value=bool(cfg.get("trust_server_certificate", True)))
        with col4:
            driver = st.text_input("ODBC Driver (ปล่อยว่าง = ให้ระบบเลือก)", value=cfg.get("driver") or "", placeholder="ODBC Driver 18 for SQL Server")
            encrypt = st.selectbox("Encrypt", options=["ไม่กำหนด", "yes", "no"], index=0)

        submitted = st.form_submit_button("💾 บันทึก")
        if submitted:
            new_cfg = {
                "server": server.strip(),
                "database": database.strip(),
                "uid": uid.strip(),
                "pwd": pwd,
                "timeout": int(timeout),
                "trust_server_certificate": trust,
                "driver": driver.strip(),
                "windows_auth": bool(windows_auth),
                "port": port.strip(),
            }
            if encrypt != "ไม่กำหนด":
                new_cfg["encrypt"] = (encrypt == "yes")
            st.session_state.cfg = new_cfg
            save_config(new_cfg)
            st.success("บันทึกการตั้งค่าเรียบร้อย")
            st.rerun()

# ========================== PAGE LAYOUT ==========================
st.set_page_config(page_title="SQL Server Connection UI", page_icon="🧪", layout="centered")
st.title("🧪 SQL Server Connection UI (Streamlit)")

# โหลด config ครั้งแรก
if "cfg" not in st.session_state:
    st.session_state.cfg = load_config()

# แถบหัว: ปุ่มตั้งค่า + โชว์ driver ที่มีในเครื่อง
top_left, top_right = st.columns([1,1])
with top_left:
    if st.button("⚙️ ตั้งค่า (Popup)"):
        settings_dialog()
with top_right:
    with st.expander("🧩 ODBC Drivers ที่พบในเครื่อง", expanded=False):
        try:
            st.code("\n".join(pyodbc.drivers()) or "(ไม่พบ driver)", language="text")
        except Exception as e:
            st.write("ไม่สามารถอ่านรายชื่อไดรเวอร์:", e)

st.divider()

# สรุปค่าปัจจุบัน (mask รหัสผ่าน)
cfg = st.session_state.cfg
masked_cfg = cfg.copy()
if masked_cfg.get("pwd"):
    masked_cfg["pwd"] = "***"
st.subheader("ค่าเชื่อมต่อปัจจุบัน")
st.json(masked_cfg)

# ปุ่มทดสอบการเชื่อมต่อ
st.subheader("ทดสอบการเชื่อมต่อ")
if st.button("▶️ ทดสอบตอนนี้"):
    try:
        conn_str, timeout = build_conn_str(cfg)
        masked = conn_str.replace(f"PWD={cfg.get('pwd','')}", "PWD=***") if cfg.get("pwd") else conn_str
        with st.status("กำลังทดสอบการเชื่อมต่อ...", expanded=True) as status:
            st.write("Connection String (masked):")
            st.code(masked, language="text")

            conn = pyodbc.connect(conn_str, timeout=timeout)
            status.update(label="เชื่อมต่อสำเร็จ ✅", state="complete")
            st.success("✅ เชื่อมต่อฐานข้อมูลสำเร็จ")

            # ข้อมูลเซิร์ฟเวอร์
            info = pd.read_sql("SELECT @@VERSION AS [Version], DB_NAME() AS [DatabaseName]", conn)
            st.dataframe(info, use_container_width=True)

            st.divider()
            st.write("รายชื่อตาราง (Top 50):")
            try:
                tables = pd.read_sql(
                    """
                    SELECT TOP 50 s.name AS [schema], t.name AS [table]
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    ORDER BY s.name, t.name
                    """,
                    conn
                )
                st.dataframe(tables, use_container_width=True)
            except Exception as te:
                st.info(f"เชื่อมต่อได้ แต่ดึงรายชื่อตารางไม่สำเร็จ: {te}")

            conn.close()

    except Exception as e:
        st.error("❌ เชื่อมต่อไม่สำเร็จ")
        st.code(str(e), language="text")
        st.markdown("**คำแนะนำ:**")
        st.markdown(diagnose_error_text(str(e)))

st.caption("หมายเหตุ: สำหรับโปรดักชัน แนะนำเก็บรหัสผ่านใน `st.secrets` หรือ environment variables แทนไฟล์ทั่วไป")
