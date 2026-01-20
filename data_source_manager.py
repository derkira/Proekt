"""
Менеджер источников данных - парсер RSS и управление потоком данных
"""
import feedparser
import requests
import pandas as pd
import logging
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import sqlite3
import hashlib
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RSSFeedParser:
    """Парсер RSS ленты закупок.gov.ru с поддержкой VPN и retry"""
    
    def __init__(self):
        self.feed_url = "https://zakupki.gov.ru/epz/order/extendedsearch/rss.html"
        self.timeout = 60  # Увеличено для VPN
        self.session = self._create_session()
        self.cache_file = "./data/.rss_cache.json"
        self.cache_ttl = 600  # 10 минут кэш
        
    def _create_session(self):
        """Создать сессию с retry логикой"""
        session = requests.Session()
        
        # Конфигурация retry
        retry_strategy = Retry(
            total=5,  # 5 попыток
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2  # Экспоненциальный backoff: 2, 4, 8, 16, 32 сек
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_cached_feed(self) -> Optional[Dict]:
        """Получить кэшированный RSS"""
        try:
            if not os.path.exists(self.cache_file):
                return None
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            # Проверяем TTL
            cached_time = datetime.fromisoformat(cache.get('time', ''))
            if (datetime.now() - cached_time).total_seconds() < self.cache_ttl:
                logger.info(f"💾 Используется кэшированный RSS (возраст: {(datetime.now() - cached_time).total_seconds():.0f}сек)")
                return cache.get('feed')
        except:
            pass
        
        return None
    
    def _save_cache(self, feed_text: str):
        """Сохранить RSS в кэш"""
        try:
            os.makedirs("./data", exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'time': datetime.now().isoformat(),
                    'feed': feed_text
                }, f, ensure_ascii=False)
        except Exception as e:
            logger.debug(f"Не удалось сохранить кэш: {e}")
    
    def fetch_feed(self) -> Optional[Dict]:
        """Получить RSS ленту с retry при VPN и кэшированием"""
        try:
            logger.info(f"📡 Загрузка RSS ленты: {self.feed_url}")
            logger.info(f"⏱️ Таймаут: {self.timeout}сек, Попытки: 5")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/rss+xml,application/atom+xml,text/html;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0'
            }
            
            # Отключаем SSL проверку для VPN (может быть MITM)
            response = self.session.get(
                self.feed_url, 
                headers=headers, 
                timeout=self.timeout,
                verify=False  # Для VPN
            )
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                feed = feedparser.parse(response.text)
                logger.info(f"✅ RSS загружена успешно. Записей: {len(feed.entries)}")
                
                # Сохраняем в кэш
                self._save_cache(response.text)
                
                return feed
            else:
                logger.error(f"❌ Ошибка получения RSS: статус {response.status_code}")
                # Пробуем использовать кэш если была ошибка
                cached_feed = self._get_cached_feed()
                if cached_feed:
                    logger.info("💾 Используем кэш из-за ошибки подключения")
                    return feedparser.parse(cached_feed)
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке RSS: {type(e).__name__}: {e}")
            logger.info("💡 Если используется VPN, попробуйте отключить его и повторить")
            
            # Пробуем использовать кэш при ошибке подключения
            cached_feed = self._get_cached_feed()
            if cached_feed:
                logger.info("💾 Используем кэшированный RSS из-за ошибки подключения")
                return feedparser.parse(cached_feed)
            
            return None
    
    def parse_entry(self, entry) -> Optional[Dict]:
        """Парсить одну запись RSS"""
        try:
            # Базовые данные из RSS
            title = entry.get('title', '')
            link = entry.get('link', '')
            published = entry.get('published', '')
            description = entry.get('description', '')
            
            # Парсим описание для получения деталей
            details = self._parse_description(description)
            
            # Генерируем номер закупки из ссылки
            nomer = self._extract_purchase_number(link)
            
            # Формируем запись
            purchase = {
                'nomer': nomer,
                'organizaciya': details.get('organizaciya', 'Не указана'),
                'opisanie': title[:200],  # Первые 200 символов заголовка
                'kategoriya': details.get('kategoriya', 'Не указана'),
                'region': details.get('region', 'Не указана'),
                'byudzhet': details.get('byudzhet', 0),
                'status': 'Объявлена',
                'data': self._parse_date(published),
                'istochnik': 'zakupki.gov.ru',
                'url': link,
                'raw_description': description,
                'loaded_at': datetime.now().isoformat()
            }
            
            return purchase
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга записи: {e}")
            return None
    
    def _parse_description(self, description: str) -> Dict:
        """Парсить HTML описание для получения деталей"""
        details = {
            'organizaciya': 'Не указана',
            'kategoriya': 'Не указана',
            'region': 'Не указана',
            'byudzhet': 0
        }
        
        try:
            soup = BeautifulSoup(description, 'html.parser')
            text = soup.get_text()
            
            # Парсим организацию
            if 'Заказчик:' in text:
                org_match = re.search(r'Заказчик:\s*([^\n<]+)', text)
                if org_match:
                    details['organizaciya'] = org_match.group(1).strip()[:100]
            
            # Парсим регион
            if 'Регион:' in text:
                reg_match = re.search(r'Регион:\s*([^\n<]+)', text)
                if reg_match:
                    details['region'] = reg_match.group(1).strip()[:50]
            
            # Парсим НМЦк
            if 'Начальная' in text or 'Начальная максимальная цена' in text:
                price_match = re.search(r'Начальная[^:]*:\s*([0-9,.\s]+)', text)
                if price_match:
                    price_str = price_match.group(1).replace(' ', '').replace(',', '.')
                    try:
                        details['byudzhet'] = float(price_str)
                    except:
                        pass
            
            # Определяем категорию по описанию
            categories = {
                'Медицина': ['медицин', 'фармац', 'здоровь', 'больниц', 'клиник'],
                'Транспорт': ['авто', 'машин', 'транспорт', 'дорог'],
                'Строительство': ['строи', 'ремонт', 'сметн', 'проектн'],
                'Энергетика': ['электр', 'энерг', 'газ'],
                'IT': ['программ', 'софт', 'компьютер', 'сервер', 'систем'],
                'Образование': ['образов', 'учеб', 'школ', 'универси'],
                'Жилье': ['жил', 'дом', 'квартир'],
                'Благоустройство': ['парк', 'озелен', 'скверы', 'дорог'],
            }
            
            text_lower = text.lower()
            for cat, keywords in categories.items():
                if any(kw in text_lower for kw in keywords):
                    details['kategoriya'] = cat
                    break
                    
        except Exception as e:
            logger.debug(f"Ошибка парсинга описания: {e}")
        
        return details
    
    def _extract_purchase_number(self, link: str) -> str:
        """Извлечь номер закупки из URL"""
        try:
            match = re.search(r'registrationNumber=([^&]+)', link)
            if match:
                return f"44-{match.group(1)}"
        except:
            pass
        
        # Генерируем на основе текущего времени
        return f"44-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def _parse_date(self, date_str: str) -> str:
        """Парсить дату из RSS"""
        try:
            # Попробуем стандартные форматы
            for fmt in ['%a, %d %b %Y %H:%M:%S %z', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                try:
                    dt = datetime.strptime(date_str.split('+')[0].split('Z')[0].strip(), fmt)
                    return dt.strftime('%Y-%m-%d')
                except:
                    continue
            
            # Возвращаем текущую дату если не смогли спарсить
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.debug(f"Ошибка парсинга даты '{date_str}': {e}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def get_new_entries(self, last_load_time: Optional[datetime] = None) -> List[Dict]:
        """Получить новые записи с момента последней загрузки"""
        feed = self.fetch_feed()
        if not feed or not feed.entries:
            return []
        
        entries = []
        for entry in feed.entries[:100]:  # Берем последние 100
            parsed = self.parse_entry(entry)
            if parsed:
                # Если последняя загрузка не указана, берем все
                if last_load_time is None:
                    entries.append(parsed)
                else:
                    # Проверяем дату
                    try:
                        entry_date = datetime.fromisoformat(parsed['data'])
                        if entry_date > last_load_time:
                            entries.append(parsed)
                    except:
                        entries.append(parsed)
        
        return entries


class DataSourceManager:
    """Менеджер источников данных"""
    
    def __init__(self, db_path="./data/zakupki.db"):
        self.db_path = db_path
        self.rss_parser = RSSFeedParser()
        self._init_db()
    
    def _init_db(self):
        """Инициализировать таблицы"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Таблица источников данных
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    type TEXT,
                    url TEXT,
                    last_load_time TEXT,
                    last_load_count INTEGER,
                    status TEXT,
                    created_at TEXT
                )
            """)
            
            # Таблица загруженных данных (дедупликация)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS loaded_purchases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nomer TEXT UNIQUE,
                    source TEXT,
                    loaded_at TEXT,
                    data_hash TEXT
                )
            """)
            
            # Обновляем таблицу etl_raw для хранения source_id
            cursor.execute("""
                ALTER TABLE etl_raw ADD COLUMN source_id INTEGER DEFAULT NULL
            """)
            
            conn.commit()
            conn.close()
            
            # Инициализируем источник RSS если его нет
            self._init_rss_source()
            
        except Exception as e:
            logger.debug(f"Ошибка инициализации БД: {e}")
    
    def _init_rss_source(self):
        """Инициализировать RSS источник"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR IGNORE INTO data_sources 
                (name, type, url, status, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                'RSS - закупки.gov.ru',
                'rss',
                self.rss_parser.feed_url,
                'active',
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.debug(f"Ошибка инициализации RSS источника: {e}")
    
    def _get_data_hash(self, data: Dict) -> str:
        """Получить хеш данных для дедупликации"""
        content = f"{data['nomer']}{data['opisanie']}{data['byudzhet']}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _is_duplicate(self, nomer: str, data_hash: str) -> bool:
        """Проверить, загружены ли уже эти данные"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT id FROM loaded_purchases WHERE nomer = ? AND data_hash = ?",
                (nomer, data_hash)
            )
            
            result = cursor.fetchone()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            logger.debug(f"Ошибка проверки дедупликации: {e}")
            return False
    
    def load_rss_data(self, limit: int = 50) -> Dict:
        """Загрузить данные из RSS ленты"""
        logger.info("🔄 Начало загрузки данных из RSS...")
        
        try:
            # Получаем новые записи
            entries = self.rss_parser.get_new_entries()[:limit]
            
            if not entries:
                logger.info("📭 Новых данных не найдено")
                return {
                    'status': 'no_data',
                    'loaded_count': 0,
                    'duplicate_count': 0,
                    'error_count': 0
                }
            
            # Загружаем в БД
            from etl_manager import ETLDataManager
            etl_manager = ETLDataManager(self.db_path)
            
            loaded_count = 0
            duplicate_count = 0
            error_count = 0
            
            for entry in entries:
                try:
                    # Проверяем дедупликацию
                    data_hash = self._get_data_hash(entry)
                    if self._is_duplicate(entry['nomer'], data_hash):
                        duplicate_count += 1
                        continue
                    
                    # Преобразуем в DataFrame
                    df = pd.DataFrame([entry])
                    
                    # Загружаем в raw слой
                    etl_manager.load_raw_data(df, source='RSS - закупки.gov.ru')
                    
                    # Записываем в таблицу загруженных
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO loaded_purchases (nomer, source, loaded_at, data_hash)
                        VALUES (?, ?, ?, ?)
                    """, (entry['nomer'], 'rss', datetime.now().isoformat(), data_hash))
                    conn.commit()
                    conn.close()
                    
                    loaded_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка загрузки записи: {e}")
                    error_count += 1
            
            # Обновляем информацию об источнике
            self._update_source_info('RSS - закупки.gov.ru', loaded_count)
            
            logger.info(f"✅ Загрузка завершена: {loaded_count} новых, {duplicate_count} дубликатов, {error_count} ошибок")
            
            return {
                'status': 'success',
                'loaded_count': loaded_count,
                'duplicate_count': duplicate_count,
                'error_count': error_count
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке RSS: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'loaded_count': 0,
                'duplicate_count': 0,
                'error_count': 0
            }
    
    def _update_source_info(self, source_name: str, count: int):
        """Обновить информацию об источнике"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE data_sources 
                SET last_load_time = ?, last_load_count = ?, status = ?
                WHERE name = ?
            """, (datetime.now().isoformat(), count, 'active', source_name))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.debug(f"Ошибка обновления информации об источнике: {e}")
    
    def get_source_stats(self) -> List[Dict]:
        """Получить статистику по источникам"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name, type, last_load_time, last_load_count, status
                FROM data_sources
                ORDER BY last_load_time DESC
            """)
            
            results = cursor.fetchall()
            conn.close()
            
            stats = []
            for row in results:
                stats.append({
                    'name': row[0],
                    'type': row[1],
                    'last_load_time': row[2],
                    'last_load_count': row[3],
                    'status': row[4]
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return []
    
    def get_duplicates_count(self, hours: int = 24) -> int:
        """Получить количество дубликатов за последние N часов"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            since = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            cursor.execute("""
                SELECT COUNT(*) FROM loaded_purchases 
                WHERE loaded_at > ?
            """, (since,))
            
            count = cursor.fetchone()[0]
            conn.close()
            
            return count
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения дубликатов: {e}")
            return 0


if __name__ == '__main__':
    # Тест парсера
    manager = DataSourceManager()
    result = manager.load_rss_data(limit=10)
    print(f"✅ Результат: {result}")
    
    # Показываем статистику
    stats = manager.get_source_stats()
    print(f"📊 Статистика источников: {stats}")
