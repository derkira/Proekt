#!/usr/bin/env python3
"""
Скрипт запуска приложения ГосЗакупки с полным Big Data стеком

Функции:
- Проверка Docker
- Запуск docker-compose с полным стеком
- Инициализация HDFS и Hive
- Запуск Streamlit приложения
"""

import os
import sys
import subprocess
import time
import socket
import requests
from pathlib import Path

class LaunchManager:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.docker_compose_full = self.project_root / "docker-compose-full.yml"
        self.docker_compose_minimal = self.project_root / "docker-compose-minimal.yml"
        self.logs_dir = self.project_root / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.log_file = self.logs_dir / "launch.log"
        
    def log(self, msg, level="INFO"):
        """Логирование"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{timestamp}] [{level}] {msg}"
        print(full_msg)
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(full_msg + "\n")
    
    def run_command(self, cmd, description=""):
        """Выполнить команду с логированием"""
        self.log(f"Выполнение: {description or cmd}")
        try:
            # Заменяем 'docker' на 'docker.exe' для Windows
            cmd = cmd.replace("docker ", "docker.exe ")
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                self.log(f"Ошибка: {result.stderr}", "ERROR")
                return False
            self.log(f"✓ Успешно: {description or cmd}")
            return True
        except Exception as e:
            self.log(f"Исключение: {e}", "ERROR")
            return False
    
    def check_docker(self):
        """Проверка Docker"""
        self.log("=== ПРОВЕРКА DOCKER ===")
        
        # Проверка установки Docker
        if not self.run_command("docker.exe --version", "Проверка версии Docker"):
            self.log("Docker не установлен! Установите Docker Desktop.", "ERROR")
            return False
        
        # Проверка запуска Docker daemon
        if not self.run_command("docker.exe ps", "Проверка Docker daemon"):
            self.log("Docker daemon не запущен! Запустите Docker Desktop.", "ERROR")
            return False
        
        self.log("✓ Docker готов к работе")
        return True
    
    def check_port(self, port):
        """Проверка доступности порта"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        return result == 0
    
    def wait_for_service(self, port, service_name, max_retries=30):
        """Ожидание запуска сервиса"""
        self.log(f"Ожидание запуска {service_name} на порту {port}...")
        
        for i in range(max_retries):
            if self.check_port(port):
                self.log(f"✓ {service_name} запущен")
                return True
            
            if i % 5 == 0:
                self.log(f"  Попытка {i+1}/{max_retries}...", "DEBUG")
            time.sleep(1)
        
        self.log(f"Timeout при ожидании {service_name}", "ERROR")
        return False
    
    def start_docker_compose(self, full_stack=True):
        """Запуск docker-compose"""
        self.log("=== ЗАПУСК DOCKER COMPOSE ===")
        
        compose_file = self.docker_compose_full if full_stack else self.docker_compose_minimal
        
        if not compose_file.exists():
            self.log(f"Файл {compose_file} не найден!", "ERROR")
            return False
        
        cmd = f"docker.exe compose -f {compose_file} up -d"
        if not self.run_command(cmd, f"Запуск docker-compose из {compose_file.name}"):
            return False
        
        self.log("Дождаемся инициализации сервисов...")
        time.sleep(10)
        
        # Проверка ключевых сервисов
        services = [
            (9870, "HDFS NameNode"),
            (8080, "Spark Master"),
            (5432, "PostgreSQL"),
            (6379, "Redis"),
            (8888, "Jupyter"),
        ]
        
        all_ready = True
        for port, service in services:
            if not self.wait_for_service(port, service, max_retries=20):
                all_ready = False
                self.log(f"⚠️ {service} не готов, но продолжаем...", "WARNING")
        
        return True
    
    def initialize_hdfs(self):
        """Инициализация HDFS директорий"""
        self.log("=== ИНИЦИАЛИЗАЦИЯ HDFS ===")
        
        # Создание директорий в HDFS
        hdfs_dirs = [
            "/data/raw",
            "/data/processed",
            "/user/root",
        ]
        
        for dir_path in hdfs_dirs:
            cmd = f"docker.exe exec namenode hdfs dfs -mkdir -p {dir_path}"
            self.run_command(cmd, f"Создание {dir_path}")
            
            cmd2 = f"docker.exe exec namenode hdfs dfs -chmod -R 777 {dir_path}"
            self.run_command(cmd2, f"Разрешения для {dir_path}")
        
        self.log("✓ HDFS инициализирован")
        return True
    
    def print_services_info(self):
        """Вывод информации о запущенных сервисах"""
        self.log("\n" + "="*60)
        self.log("✓ ВСЕ СЕРВИСЫ ЗАПУЩЕНЫ")
        self.log("="*60)
        
        services_info = """
╔════════════════════════════════════════════════════════════╗
║           ДОСТУПНЫЕ СЕРВИСЫ И ИНТЕРФЕЙСЫ                 ║
╠════════════════════════════════════════════════════════════╣
║ 🎨 Streamlit App        http://localhost:8501             ║
║ 📊 Jupyter Notebook     http://localhost:8888             ║
║ 🐘 HDFS NameNode        http://localhost:9870             ║
║ ⚡ Spark Master          http://localhost:8080             ║
║ 🐝 Hive Server          localhost:10000                   ║
║ 🗄️  PostgreSQL          localhost:5432                    ║
║ 💾 Redis                localhost:6379                    ║
╠════════════════════════════════════════════════════════════╣
║ РЕКОМЕНДУЕМЫЙ ПОРЯДОК:                                   ║
║ 1. Откройте Jupyter: http://localhost:8888               ║
║ 2. Запустите ETL сценарий (notebooks/etl_pipeline.ipynb) ║
║ 3. Откройте приложение: http://localhost:8501            ║
╚════════════════════════════════════════════════════════════╝
        """
        print(services_info)
        self.log(services_info)
    
    def start_streamlit(self):
        """Запуск Streamlit приложения"""
        self.log("\n=== ЗАПУСК STREAMLIT ПРИЛОЖЕНИЯ ===")
        
        app_file = self.project_root / "app_main.py"
        if not app_file.exists():
            self.log("app_main.py не найден!", "ERROR")
            return False
        
        cmd = f"streamlit run {app_file} --server.port 8501"
        self.log(f"Запуск: {cmd}")
        
        try:
            subprocess.run(cmd, shell=True, cwd=self.project_root)
        except KeyboardInterrupt:
            self.log("\nПрограмма остановлена пользователем")
        
        return True
    
    def launch(self, full_stack=True):
        """Основной процесс запуска"""
        self.log("╔════════════════════════════════════════╗")
        self.log("║  ГосЗакупки - Система управления      ║")
        self.log("║  Big Data архитектура                 ║")
        self.log("╚════════════════════════════════════════╝")
        
        # 1. Проверка Docker
        if not self.check_docker():
            return False
        
        # 2. Запуск docker-compose
        if not self.start_docker_compose(full_stack=full_stack):
            return False
        
        # 3. Инициализация HDFS (если полный стек)
        if full_stack:
            if not self.initialize_hdfs():
                self.log("⚠️ Ошибка инициализации HDFS, но продолжаем...", "WARNING")
        
        # 4. Информация о сервисах
        self.print_services_info()
        
        # 5. Запуск Streamlit
        self.start_streamlit()
        
        return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Запуск приложения ГосЗакупки с Big Data стеком"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Использовать полный стек (Hadoop/Spark/NiFi)"
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help="Использовать минимальный стек (без Hadoop/Spark/Hive)"
    )
    parser.add_argument(
        "--no-app",
        action="store_true",
        help="Не запускать Streamlit приложение"
    )
    
    args = parser.parse_args()
    
    launcher = LaunchManager()
    # Если передан --full, используем полный стек. Иначе проверяем --minimal
    if args.full:
        full_stack = True
    else:
        full_stack = not args.minimal
    
    if launcher.launch(full_stack=full_stack):
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())
