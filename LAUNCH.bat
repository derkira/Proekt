@echo off
chcp 65001 >nul
REM ====================================================
REM   ГосЗакупки - Big Data ETL System Launcher
REM   Запуск приложения с полным Docker стеком
REM ====================================================

setlocal enabledelayedexpansion

echo.
echo   ====================================================
echo   ГосЗакупки - Big Data ETL System
echo   Запуск полного стека приложения
echo   ====================================================
echo.

REM Проверяем Docker
echo [*] Проверка Docker...
docker.exe --version >nul 2>&1
if errorlevel 1 (
    echo [X] Docker не установлен или не запущен!
    echo     Установите Docker Desktop с https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)
echo [OK] Docker готов

REM Проверяем Python
echo [*] Проверка Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo [X] Python не установлен!
    echo     Установите Python 3.8+ с https://www.python.org/
    pause
    exit /b 1
)
echo [OK] Python готов

REM Создаем директории
echo [*] Создание директорий...
if not exist "notebooks" mkdir notebooks
if not exist "data\hdfs\namenode" mkdir data\hdfs\namenode
if not exist "data\hdfs\datanode1" mkdir data\hdfs\datanode1
if not exist "data\hdfs\datanode2" mkdir data\hdfs\datanode2
if not exist "data\postgresql" mkdir data\postgresql
if not exist "data\postgresql\app" mkdir data\postgresql\app
if not exist "data\redis" mkdir data\redis
if not exist "logs" mkdir logs
echo [OK] Директории созданы

REM Выбор режима
echo.
echo [?] Выберите режим запуска:
echo     1) Полный стек (Hadoop + Spark + Hive + NiFi + Jupyter)
echo     2) Минимальный стек (только основное приложение)
echo.
set /p choice="Введите 1 или 2 (по умолчанию 1): "
if "%choice%"=="" set choice=1

if "%choice%"=="1" (
    echo [*] Запуск ПОЛНОГО стека...
    set COMPOSE_FILE=docker-compose-full.yml
    set FULL_STACK=--full
) else (
    echo [*] Запуск МИНИМАЛЬНОГО стека...
    set COMPOSE_FILE=docker-compose-minimal.yml
    set FULL_STACK=--minimal
)

REM Проверяем наличие docker-compose файла
if not exist "!COMPOSE_FILE!" (
    echo [X] Файл !COMPOSE_FILE! не найден!
    pause
    exit /b 1
)

REM Запускаем docker-compose
echo [*] Запуск Docker Compose из !COMPOSE_FILE!...
docker.exe compose -f !COMPOSE_FILE! up -d
if errorlevel 1 (
    echo [X] Ошибка при запуске Docker Compose
    pause
    exit /b 1
)

echo [OK] Docker контейнеры запущены

REM Ожидаем инициализации
echo [*] Инициализация сервисов (примерно 10 сек)...
timeout /t 10 /nobreak

REM Запускаем Python скрипт
echo [*] Запуск приложения...
python launch.py %FULL_STACK%

if errorlevel 1 (
    echo [X] Ошибка при запуске приложения
    pause
    exit /b 1
)

echo [OK] Приложение готово!
echo.
echo Откройте браузер и перейдите на:
echo   - Streamlit: http://localhost:8501
echo   - Jupyter: http://localhost:8888
echo   - Spark Master: http://localhost:8080
echo   - HDFS NameNode: http://localhost:9870
echo.

pause
