"""
Фоновый сервис для постоянной загрузки данных из RSS ленты
Работает независимо от Streamlit и автоматически загружает данные
"""
import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import time
import os
import sys
import signal

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_source_manager import DataSourceManager
from etl_manager import ETLDataManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BackgroundDataLoader:
    """Фоновый загрузчик данных"""
    
    def __init__(self, interval_minutes: int = 15):
        self.interval_minutes = interval_minutes
        self.scheduler = BackgroundScheduler()
        self.data_source_manager = DataSourceManager()
        self.etl_manager = ETLDataManager()
        self.is_running = False
        
    def start(self):
        """Запустить фоновый загрузчик"""
        try:
            logger.info("=" * 80)
            logger.info("🚀 ЗАПУСК ФОНОВОГО ЗАГРУЗЧИКА ДАННЫХ")
            logger.info(f"📊 Интервал загрузки: каждые {self.interval_minutes} минут")
            logger.info("=" * 80)
            
            # Добавляем задачу загрузки RSS
            self.scheduler.add_job(
                self._load_rss_data,
                IntervalTrigger(minutes=self.interval_minutes),
                id='rss_loader',
                name='RSS Data Loader',
                max_instances=1,
                replace_existing=True
            )
            
            # Добавляем задачу обработки данных (раз в час)
            self.scheduler.add_job(
                self._process_data,
                IntervalTrigger(minutes=60),
                id='data_processor',
                name='Data Processor',
                max_instances=1,
                replace_existing=True
            )
            
            # Добавляем задачу логирования статистики (раз в 30 минут)
            self.scheduler.add_job(
                self._log_statistics,
                IntervalTrigger(minutes=30),
                id='stats_logger',
                name='Statistics Logger',
                max_instances=1,
                replace_existing=True
            )
            
            # Запускаем первую загрузку сразу
            logger.info("⏳ Первая загрузка начинается сейчас...")
            self._load_rss_data()
            
            # Запускаем планировщик
            self.scheduler.start()
            self.is_running = True
            
            logger.info("✅ Фоновый загрузчик запущен успешно")
            logger.info(f"📅 Следующая загрузка через {self.interval_minutes} минут")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске загрузчика: {e}")
            raise
    
    def stop(self):
        """Остановить фоновый загрузчик"""
        try:
            logger.info("🛑 Остановка фонового загрузчика...")
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("✅ Загрузчик остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка при остановке: {e}")
    
    def _load_rss_data(self):
        """Загрузить данные из RSS"""
        try:
            logger.info("")
            logger.info("=" * 80)
            logger.info("📡 ЗАГРУЗКА ДАННЫХ ИЗ RSS ЛЕНТЫ")
            logger.info(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            
            result = self.data_source_manager.load_rss_data(limit=50)
            
            if result['status'] == 'success':
                logger.info(f"✅ Успешно загружено: {result['loaded_count']} новых записей")
                if result['duplicate_count'] > 0:
                    logger.info(f"⏭️  Пропущено дубликатов: {result['duplicate_count']}")
                if result['error_count'] > 0:
                    logger.warning(f"⚠️  Ошибок: {result['error_count']}")
            elif result['status'] == 'no_data':
                logger.info("📭 Новых данных не найдено")
            else:
                logger.error(f"❌ Ошибка загрузки: {result.get('error', 'Unknown')}")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при загрузке RSS: {e}", exc_info=True)
    
    def _process_data(self):
        """Обработать накопленные данные"""
        try:
            logger.info("")
            logger.info("=" * 80)
            logger.info("🔄 ОБРАБОТКА НАКОПЛЕННЫХ ДАННЫХ")
            logger.info(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            
            # Получаем количество необработанных записей
            import sqlite3
            conn = sqlite3.connect('./data/zakupki.db')
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM etl_raw WHERE processed_at IS NULL")
            raw_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM etl_processed")
            processed_count = cursor.fetchone()[0]
            
            conn.close()
            
            if raw_count > 0:
                logger.info(f"📊 Необработанных записей: {raw_count}")
                self.etl_manager.process_data()
                logger.info(f"✅ Обработка завершена")
                logger.info(f"📈 Всего обработано: {processed_count} записей")
            else:
                logger.info("✅ Все записи уже обработаны")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке данных: {e}", exc_info=True)
    
    def _log_statistics(self):
        """Вывести статистику"""
        try:
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 СТАТИСТИКА ПОТОКА ДАННЫХ")
            logger.info(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            
            import sqlite3
            conn = sqlite3.connect('./data/zakupki.db')
            cursor = conn.cursor()
            
            # Статистика по таблицам
            cursor.execute("SELECT COUNT(*) FROM etl_raw")
            raw_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM etl_processed")
            processed_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM loaded_purchases")
            total_loaded = cursor.fetchone()[0]
            
            # Статистика за последний час
            since = (datetime.now() - timedelta(hours=1)).isoformat()
            cursor.execute(
                "SELECT COUNT(*) FROM etl_raw WHERE loaded_at > ?",
                (since,)
            )
            last_hour = cursor.fetchone()[0]
            
            conn.close()
            
            logger.info(f"📥 Raw Layer: {raw_count} записей")
            logger.info(f"📤 Processed Layer: {processed_count} записей")
            logger.info(f"📊 Всего загружено: {total_loaded} записей")
            logger.info(f"⚡ За последний час: {last_hour} новых записей")
            
            # Статистика источников
            stats = self.data_source_manager.get_source_stats()
            if stats:
                logger.info("")
                logger.info("Источники данных:")
                for source in stats:
                    logger.info(f"  • {source['name']}")
                    logger.info(f"    Статус: {source['status']}")
                    logger.info(f"    Последняя загрузка: {source['last_load_time']}")
                    logger.info(f"    Записей загружено: {source['last_load_count']}")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при логировании статистики: {e}", exc_info=True)


def signal_handler(signum, frame):
    """Обработчик сигнала для корректной остановки"""
    logger.info("\n🛑 Получен сигнал остановки...")
    loader.stop()
    sys.exit(0)


# Глобальная переменная загрузчика
loader = None


def main():
    """Главная функция"""
    global loader
    
    # Создаем директорию для логов если её нет
    os.makedirs('./logs', exist_ok=True)
    
    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Создаем и запускаем загрузчик
    interval = int(os.getenv('DATA_LOAD_INTERVAL', '15'))  # По умолчанию 15 минут
    
    loader = BackgroundDataLoader(interval_minutes=interval)
    loader.start()
    
    # Держим процесс живым
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n⌨️  Прерывание пользователя...")
        loader.stop()
        sys.exit(0)


if __name__ == '__main__':
    main()
