## Установка и Запуск

### Требования

- **Python**: 3.8+
- **SQLite**: встроенная в Python
- **RAM**: минимум 2GB для полной функциональности
- **ОС**: Windows, Linux, macOS

### Шаг 1: Установка зависимостей

```bash
cd c:\goszakupki_project
pip install -r requirements.txt
```

**Основные библиотеки:**
- `streamlit` - веб-интерфейс
- `pandas` - обработка данных
- `scikit-learn` - ML модели и TF-IDF
- `plotly` - интерактивные графики
- `sqlite3` - БД (встроена)

### Шаг 2: Запуск приложения

**Полный запуск (рекомендуется):**
```bash
python launch.py --full
```

**Быстрый запуск:**
```bash
streamlit run app_main.py
```

**Использование LAUNCH.bat (Windows):**
```bash
LAUNCH.bat
```

### Шаг 3: Откройте браузер

Приложение будет доступно по адресу:
```
http://localhost:8501
```
