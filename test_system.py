#!/usr/bin/env python3
"""
Предварительная проверка всех компонентов системы перед запуском
Проверяет:
1. Наличие Docker и docker compose
2. Валидность конфигов docker-compose
3. Наличие всех Python пакетов
4. Синтаксис всех Python файлов
5. Статус портов
"""

import subprocess
import sys
import socket
from pathlib import Path

def run_cmd(cmd, description=""):
    """Выполнить команду"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)

def check_docker():
    """Проверка Docker"""
    print("\n=== ПРОВЕРКА DOCKER ===")
    
    success, output = run_cmd("docker.exe version")
    if success:
        print("[OK] Docker установлен и запущен")
        return True
    else:
        print("[ОШИБКА] Docker не работает")
        return False

def check_docker_compose():
    """Проверка docker compose"""
    print("\n=== ПРОВЕРКА DOCKER COMPOSE ===")
    
    success, output = run_cmd("docker.exe compose version")
    if success:
        print(f"[OK] Docker Compose работает")
        return True
    else:
        print("[ОШИБКА] Docker Compose не работает")
        return False

def check_compose_files():
    """Проверка конфигов docker-compose"""
    print("\n=== ПРОВЕРКА КОНФИГОВ ===")
    
    files = ["docker-compose-full.yml", "docker-compose-minimal.yml"]
    all_ok = True
    
    for file in files:
        success, output = run_cmd(f"docker.exe compose -f {file} config")
        if success:
            print(f"[OK] {file} валиден")
        else:
            print(f"[ОШИБКА] {file} содержит ошибки")
            all_ok = False
    
    return all_ok

def check_python_files():
    """Проверка Python файлов"""
    print("\n=== ПРОВЕРКА PYTHON ФАЙЛОВ ===")
    
    files = [
        "launch.py",
        "app_main.py",
        "etl/spark_processor.py",
        "etl/scalability_test_big_data.py"
    ]
    
    all_ok = True
    for file in files:
        success, output = run_cmd(f"python -m py_compile {file}")
        if success:
            print(f"[OK] {file}")
        else:
            print(f"[ОШИБКА] {file}: {output}")
            all_ok = False
    
    return all_ok

def check_python_packages():
    """Проверка Python пакетов"""
    print("\n=== ПРОВЕРКА PYTHON ПАКЕТОВ ===")
    
    packages = {
        "streamlit": "Streamlit",
        "pandas": "Pandas",
        "numpy": "NumPy",
        "plotly": "Plotly",
        "pyspark": "PySpark",
        "sklearn": "Scikit-learn",
        "yaml": "PyYAML",
    }
    
    all_ok = True
    for pkg, name in packages.items():
        success, output = run_cmd(f"python -c \"import {pkg}; print('{pkg}')\"")
        if success:
            print(f"[OK] {name}")
        else:
            print(f"[ОШИБКА] {name} не установлен")
            all_ok = False
    
    return all_ok

def check_ports():
    """Проверка доступности портов"""
    print("\n=== ПРОВЕРКА ПОРТОВ ===")
    
    ports = {
        8501: "Streamlit",
        8888: "Jupyter",
        9870: "HDFS NameNode",
        8080: "Spark Master",
        5432: "PostgreSQL",
        6379: "Redis",
    }
    
    occupied = 0
    
    for port, service in ports.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        
        if result == 0:
            print(f"[ЗАНЯТ] {service} :{port}")
            occupied += 1
        else:
            print(f"[СВОБОДЕН] {service} :{port}")
    
    print(f"\nУчтите: занятые порты могут принадлежать запущенным контейнерам")
    return True  # Это не ошибка, просто информация

def main():
    print("=" * 60)
    print("ПРОВЕРКА СИСТЕМЫ ПЕРЕД ЗАПУСКОМ")
    print("ГосЗакупки - Big Data ETL System v2.1")
    print("=" * 60)
    
    checks = [
        ("Docker", check_docker),
        ("Docker Compose", check_docker_compose),
        ("Docker Compose конфигурация", check_compose_files),
        ("Python файлы", check_python_files),
        ("Python пакеты", check_python_packages),
        ("Доступные порты", check_ports),
    ]
    
    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"\n[ОШИБКА] {name}: {e}")
            results[name] = False
    
    # Итоговый отчет
    print("\n" + "="*60)
    print("ИТОГОВЫЙ ОТЧЕТ")
    print("="*60)
    
    for name, result in results.items():
        status = "[OK]" if result else "[ОШИБКА]"
        print(f"{status} {name}")
    
    all_ok = all(results.values())
    
    if all_ok:
        print("\n✓ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        print("\nЧтобы запустить систему, выполните:")
        print("  Windows: LAUNCH.bat")
        print("  Linux/Mac: python launch.py")
        return 0
    else:
        print("\n✗ ОБНАРУЖЕНЫ ОШИБКИ, ИСПРАВЬТЕ ИХ ПЕРЕД ЗАПУСКОМ")
        return 1

if __name__ == "__main__":
    sys.exit(main())
