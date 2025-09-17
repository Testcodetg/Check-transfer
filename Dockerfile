# ใช้ Python base image
FROM python:3.12

# ติดตั้ง system dependencies ที่จำเป็นสำหรับ pymssql
RUN apt-get update && \
    apt-get install -y gcc freetds-dev && \
    rm -rf /var/lib/apt/lists/*

# ตั้ง working directory
WORKDIR /app

# คัดลอก requirements.txt และไฟล์โปรเจค
COPY requirements.txt ./
COPY . .

# ติดตั้ง Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# เปิด port 8501 สำหรับ Streamlit
EXPOSE 8501

# คำสั่งรันแอป
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]