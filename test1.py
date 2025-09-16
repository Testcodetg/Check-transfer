import streamlit as st
import pyodbc
import pandas as pd

st.set_page_config(page_title="SQL Server Connection Tester", page_icon="🧪", layout="centered")
st.title("🧪 SQL Server Connection Tester (Streamlit)")

st.caption("กรอกค่าการเชื่อมต่อ SQL Server แล้วกด **ทดสอบการเชื่อมต่อ**")

# --- ฟอร์มการเชื่อมต่อ ---
with st.form("conn_form", clear_on_submit=False):
    col1, col2 = st.columns(2)
    with col1:
        server = st.text_input("Server", placeholder="เช่น TG\\MSSQL2017 หรือ 10.0.0.5,1433")
        database = st.text_input("Database", placeholder="เช่น Cyberhm")
    with col2:
        uid = st.text_input("User (UID)", placeholder="เช่น sa")
        pwd = st.text_input("Password (PWD)", type="password")

    # ตัวเลือก Driver + option
    adv = st.expander("ตัวเลือกขั้นสูง", expanded=False)
    with adv:
        driver = st.selectbox(
            "ODBC Driver",
            options=[
                "Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server",  # เก่ามาก ใช้เมื่อไม่มีตัวเลือกอื่น
            ],
            index=0
        )
        timeout = st.number_input("Connection Timeout (วินาที)", min_value=1, max_value=60, value=5)
        trust_cert = st.checkbox("Trust Server Certificate (แก้ปัญหา TLS เบื้องต้น)", value=True)

    test_btn = st.form_submit_button("ทดสอบการเชื่อมต่อ")

def build_conn_str(server, database, uid, pwd, driver, timeout, trust_cert=True):
    parts = [
        f"DRIVER={{{{{{driver}}}}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"UID={uid}",
        f"PWD={pwd}",
        f"Connection Timeout={timeout}",
    ]
    # สำหรับ Driver 18/17 มักต้องตั้งค่า TLS; ใช้ตัวเลือกนี้เมื่อยังไม่มีใบรับรอง
    if trust_cert:
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts)

def try_connect(conn_str: str):
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        return True, conn, None
    except Exception as e:
        return False, None, str(e)

def probe_info(conn):
    try:
        q = "SELECT @@VERSION AS [Version], DB_NAME() AS [DatabaseName]"
        df = pd.read_sql(q, conn)
        return df
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})

if test_btn:
    # ตรวจสอบค่าว่าง
    missing = []
    if not server: missing.append("Server")
    if not database: missing.append("Database")
    if not uid: missing.append("User (UID)")
    if not pwd: missing.append("Password (PWD)")
    if missing:
        st.error("กรุณากรอก: " + ", ".join(missing))
    else:
        conn_str = build_conn_str(server, database, uid, pwd, driver, timeout, trust_cert)
        with st.status("กำลังทดสอบการเชื่อมต่อ...", expanded=True) as status:
            st.write("กำลังเชื่อมต่อด้วย ODBC:", driver)
            ok, conn, err = try_connect(conn_str)
            if ok:
                status.update(label="เชื่อมต่อสำเร็จ ✅", state="complete")
                st.success("✅ เชื่อมต่อได้สำเร็จ")
                info_df = probe_info(conn)
                st.subheader("ข้อมูลเซิร์ฟเวอร์/ฐานข้อมูล")
                st.dataframe(info_df, use_container_width=True)

                # ตัวอย่าง: ทดสอบอ่านตาราง
                st.divider()
                st.subheader("ทดสอบดึงรายชื่อตาราง (Top 50)")
                try:
                    tables = pd.read_sql(
                        """
                        SELECT TOP 50
                            *
                        FROM PNT_person
                        """,
                        conn
                    )
                    st.dataframe(tables, use_container_width=True)
                except Exception as e:
                    st.info(f"ไม่สามารถดึงรายชื่อตารางได้: {e}")

                # กล่องลองรันคำสั่ง SQL
                st.divider()
                st.subheader("ลองรันคำสั่ง SQL (อ่านอย่างเดียว)")
                sql = st.text_area("คำสั่ง SQL", value="SELECT TOP 10 name, object_id FROM sys.objects ORDER BY name")
                if st.button("รัน SQL"):
                    try:
                        df_sql = pd.read_sql(sql, conn)
                        st.dataframe(df_sql, use_container_width=True)
                    except Exception as e:
                        st.error(f"รันคำสั่งไม่สำเร็จ: {e}")

                conn.close()
            else:
                status.update(label="เชื่อมต่อล้มเหลว ❌", state="error")
                st.error("❌ เชื่อมต่อไม่สำเร็จ")
                with st.expander("รายละเอียดข้อผิดพลาด"):
                    st.code(err)
                st.markdown(
                    "- ตรวจสอบ **Server/Port/Instance** (เช่น `HOST,1433` หรือ `HOST\\INSTANCE`)\n"
                    "- ตรวจสอบว่าเปิดไฟร์วอลล์พอร์ต 1433 หรืออนุญาต Remote\n"
                    "- ตรวจสอบสิทธิ์ผู้ใช้ (SQL Authentication) และฐานข้อมูลปลายทาง\n"
                    "- ถ้าเจอ TLS/SSL ให้ลองเปิด **Trust Server Certificate** หรือใช้ Driver 18/17"
                )

st.caption("ข้อแนะนำด้านความปลอดภัย: สำหรับโปรดักชันให้เก็บรหัสไว้ใน `st.secrets` หรือ environment variables แทนการพิมพ์ลงหน้าจอ")
