# ใช้ Python base image โดยบังคับ Platform เป็น amd64 และระบุ OS เป็น bookworm
FROM --platform=linux/amd64 python:3.12-bookworm

# ติดตั้ง system dependencies ที่จำเป็น
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unixodbc \
        unixodbc-dev \
        gcc \
        g++ \
        make \
    && rm -rf /var/lib/apt/lists/*

# ติดตั้ง Microsoft ODBC Driver 17 
# (Repository key และ list ถูกต้องสำหรับ bookworm อยู่แล้ว)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg && \
    echo "deb [arch=amd64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
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