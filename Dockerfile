# ใช้ Python base image
FROM python:3.12

# ติดตั้ง system dependencies สำหรับ ODBC Driver 17 และ pymssql/pyodbc
RUN apt-get update && \
    apt-get install -y \
        curl \
        gnupg2 \
        apt-transport-https \
        software-properties-common \
        unixodbc \
        unixodbc-dev \
        gcc \
        g++ \
        make \
        python3-dev \
        && rm -rf /var/lib/apt/lists/*

# ติดตั้ง Microsoft ODBC Driver 17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
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
CMD ["streamlit", "run", "main3.py", "--server.port=8501", "--server.address=0.0.0.0"]
