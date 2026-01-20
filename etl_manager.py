import sqlite3
import pandas as pd
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ETLDataManager:
    """Менеджер для работы с ETL данными и их сохранения в БД"""
    
    def __init__(self, db_path="./data/zakupki.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Инициализация таблиц для ETL мониторинга"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица для логирования ETL процессов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stage TEXT,
                    status TEXT,
                    records_count INTEGER,
                    timestamp TEXT,
                    error_message TEXT,
                    duration_seconds REAL
                )
            """)
            
            # Таблица для сырых данных
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nomer TEXT,
                    organizaciya TEXT,
                    opisanie TEXT,
                    kategoriya TEXT,
                    region TEXT,
                    byudzhet REAL,
                    status TEXT,
                    data TEXT,
                    istochnik TEXT,
                    loaded_at TEXT,
                    source TEXT
                )
            """)
            
            # Таблица для обработанных данных
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_processed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nomer TEXT,
                    kategoriya TEXT,
                    byudzhet REAL,
                    byudzhet_mln REAL,
                    region TEXT,
                    mesyac INTEGER,
                    kvartal INTEGER,
                    processed_at TEXT,
                    raw_id INTEGER,
                    FOREIGN KEY(raw_id) REFERENCES etl_raw(id)
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("✅ ETL БД инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации ETL БД: {e}")
    
    def load_raw_data(self, df, source="manual"):
        """
        Загрузить данные в Raw Layer
        
        Args:
            df: DataFrame с данными
            source: источник данных (manual, rss, api, etc.)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Добавляем служебные колонки
            df['loaded_at'] = datetime.now().isoformat()
            df['source'] = source
            
            # Сохраняем в raw
            df.to_sql('etl_raw', conn, if_exists='append', index=False)
            
            # Логируем операцию
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO etl_logs (stage, status, records_count, timestamp)
                VALUES (?, ?, ?, ?)
            """, ('RAW', 'SUCCESS', len(df), datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Загружено {len(df)} записей в Raw Layer")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки в Raw Layer: {e}")
            return False
    
    def process_data(self):
        """Перемещение и обработка данных из Raw в Processed Layer"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Читаем необработанные данные
            query = """
                SELECT * FROM etl_raw 
                WHERE id NOT IN (SELECT raw_id FROM etl_processed WHERE raw_id IS NOT NULL)
            """
            raw_data = pd.read_sql(query, conn)
            
            if raw_data.empty:
                logger.info("ℹ️ Нет новых данных для обработки")
                return 0
            
            # Трансформируем данные
            processed = raw_data.copy()
            
            # Извлекаем месяц и квартал из даты
            processed['data'] = pd.to_datetime(processed['data'], errors='coerce')
            processed['mesyac'] = processed['data'].dt.month
            processed['kvartal'] = (processed['data'].dt.month - 1) // 3 + 1
            
            # Вычисляем бюджет в миллионах
            processed['byudzhet_mln'] = processed['byudzhet'] / 1_000_000
            processed['processed_at'] = datetime.now().isoformat()
            processed['raw_id'] = processed['id']
            
            # Выбираем нужные колонки
            columns = ['nomer', 'kategoriya', 'byudzhet', 'byudzhet_mln', 
                      'region', 'mesyac', 'kvartal', 'processed_at', 'raw_id']
            processed_subset = processed[[col for col in columns if col in processed.columns]]
            
            # Сохраняем в processed
            processed_subset.to_sql('etl_processed', conn, if_exists='append', index=False)
            
            # Логируем
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO etl_logs (stage, status, records_count, timestamp)
                VALUES (?, ?, ?, ?)
            """, ('PROCESSING', 'SUCCESS', len(processed), datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Обработано {len(processed)} записей")
            return len(processed)
        except Exception as e:
            logger.error(f"❌ Ошибка обработки данных: {e}")
            return 0
    
    def get_raw_data(self, limit=100):
        """Получить данные из Raw Layer"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"""
                SELECT * FROM etl_raw 
                ORDER BY loaded_at DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка чтения Raw Layer: {e}")
            return pd.DataFrame()
    
    def get_processed_data(self, limit=100):
        """Получить данные из Processed Layer"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"""
                SELECT * FROM etl_processed 
                ORDER BY processed_at DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка чтения Processed Layer: {e}")
            return pd.DataFrame()
    
    def get_etl_statistics(self):
        """Получить статистику ETL процесса"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Количество записей в raw
            cursor.execute("SELECT COUNT(*) FROM etl_raw")
            raw_count = cursor.fetchone()[0]
            
            # Количество записей в processed
            cursor.execute("SELECT COUNT(*) FROM etl_processed")
            processed_count = cursor.fetchone()[0]
            
            # Последние логи
            cursor.execute("""
                SELECT stage, status, records_count, timestamp 
                FROM etl_logs 
                ORDER BY timestamp DESC 
                LIMIT 10
            """)
            logs = cursor.fetchall()
            
            conn.close()
            
            return {
                'raw_count': raw_count,
                'processed_count': processed_count,
                'logs': logs
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    def get_processing_stats(self):
        """Получить статистику по категориям и регионам"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # По категориям
            cat_query = """
                SELECT kategoriya, COUNT(*) as count, SUM(byudzhet_mln) as total_budget
                FROM etl_processed
                GROUP BY kategoriya
                ORDER BY total_budget DESC
            """
            categories = pd.read_sql(cat_query, conn)
            
            # По регионам
            region_query = """
                SELECT region, COUNT(*) as count, SUM(byudzhet_mln) as total_budget
                FROM etl_processed
                GROUP BY region
                ORDER BY total_budget DESC
            """
            regions = pd.read_sql(region_query, conn)
            
            # По месяцам
            month_query = """
                SELECT mesyac, COUNT(*) as count, SUM(byudzhet_mln) as total_budget
                FROM etl_processed
                GROUP BY mesyac
                ORDER BY mesyac
            """
            months = pd.read_sql(month_query, conn)
            
            conn.close()
            
            return {
                'categories': categories,
                'regions': regions,
                'months': months
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики обработки: {e}")
            return {}
