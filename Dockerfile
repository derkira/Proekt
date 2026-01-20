FROM python:3.10-slim-bullseye

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    wget \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Обновление pip
RUN pip install --upgrade pip setuptools wheel

# Копируем requirements
COPY requirements.txt .

# Устанавливаем Python пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY . .

# Создаём директории для данных и логов
RUN mkdir -p ~/.streamlit data logs data/lake/{raw,processing,processed}

# Streamlit конфигурация
RUN mkdir -p ~/.streamlit && cat > ~/.streamlit/config.toml << 'EOF'
[server]
headless = true
port = 8501
address = "0.0.0.0"
enableXsrfProtection = false
runOnSave = false
maxUploadSize = 500

[client]
showErrorDetails = false

[logger]
level = "error"
EOF

# Переменные окружения для Spark и Java
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_CLIENT_SHOW_ERROR_DETAILS=false
ENV STREAMLIT_LOGGER_LEVEL=error
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

EXPOSE 8501

# Простая проверка здоровья
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=2 \
    CMD curl -f http://localhost:8501/_stcore/health 2>/dev/null || exit 1

CMD ["streamlit", "run", "app_main.py", "--server.port=8501", "--server.address=0.0.0.0"]
