import pyodbc
import json

def load_config(file_path="config.json"):
    """โหลดค่าการเชื่อมต่อจากไฟล์ JSON"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print("❌ โหลดไฟล์ config ไม่สำเร็จ:", e)
        return None

def test_connection(name, server, database, uid, pwd):
    """ฟังก์ชันตรวจสอบการเชื่อมต่อฐานข้อมูล"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};"
            f"PWD={pwd}"
        )
        conn = pyodbc.connect(conn_str, timeout=5)
        print(f"✅ {name}: เชื่อมต่อฐานข้อมูลสำเร็จ")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ {name}: เชื่อมต่อฐานข้อมูลไม่สำเร็จ")
        print("รายละเอียด:", e)
        return False

if __name__ == "__main__":
    config = load_config()
    if config:
        # ทดสอบฐานข้อมูลเก่า
        test_connection(
            "ฐานเก่า",
            config["old_db"]["server"],
            config["old_db"]["database"],
            config["old_db"]["uid"],
            config["old_db"]["pwd"]
        )

        # ทดสอบฐานข้อมูลใหม่
        test_connection(
            "ฐานใหม่",
            config["new_db"]["server"],
            config["new_db"]["database"],
            config["new_db"]["uid"],
            config["new_db"]["pwd"]
        )
