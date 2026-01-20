#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
УМНЫЙ ПОИСК - Comprehensive Search and Filtering System
========================================================

Модуль для полнотекстового поиска, фильтрации и анализа закупок
Работает со всеми загруженными данными (100000+ записей)

Возможности:
- Полнотекстовый поиск с релевантностью
- Фильтрация по категориям, регионам, статусам, бюджету
- Объединенный поиск с несколькими фильтрами
- Получение справочников (категории, регионы, статусы)
- Статистика по результатам поиска
- Поддержка кириллицы в поиске
"""

import sqlite3
import pandas as pd
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SmartSearch:
    """
    Умный поиск по закупкам
    
    Использует полнотекстовый поиск, фильтрацию и статистику
    для работы с большими объемами данных
    """
    
    def __init__(self, db_path: str = './data/zakupki.db', timeout: int = 30):
        """
        Инициализация поиска
        
        Args:
            db_path: Путь к БД SQLite
            timeout: Таймаут соединения (сек)
        """
        self.db_path = db_path
        self.timeout = timeout
        self.conn = None
        self._initialize_connection()
        logger.info(f"✓ SmartSearch инициализирован для {db_path}")
    
    def _initialize_connection(self):
        """Инициализировать соединение с БД"""
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
            self.conn.row_factory = sqlite3.Row
            logger.debug("✓ Соединение с БД установлено")
        except Exception as e:
            logger.error(f"❌ Ошибка соединения: {e}")
            raise
    
    def full_search(self, query: str, limit: int = 100) -> pd.DataFrame:
        """
        Полнотекстовый поиск с релевантностью
        
        Ищет по: номер, организация, описание, категория, регион
        Сортирует по релевантности (точное совпадение → частичное)
        
        Args:
            query: Поисковая строка (2+ символа)
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        
        Примеры:
            >>> search.full_search("медикаменты", limit=50)
            >>> search.full_search("ООО Рога и Копыта")
        """
        if not query or len(query.strip()) < 2:
            logger.warning("⚠️  Поисковая строка слишком короткая")
            return pd.DataFrame()
        
        query_lower = query.lower()
        pattern = f"%{query_lower}%"
        
        try:
            sql = """
            SELECT 
                nomer, 
                organizaciya, 
                opisanie, 
                kategoriya, 
                region, 
                byudzhet, 
                status, 
                data
            FROM zakupki
            WHERE 
                LOWER(nomer) LIKE ? OR
                LOWER(organizaciya) LIKE ? OR
                LOWER(opisanie) LIKE ? OR
                LOWER(kategoriya) LIKE ? OR
                LOWER(region) LIKE ?
            ORDER BY 
                CASE 
                    WHEN LOWER(nomer) = ? THEN 1
                    WHEN LOWER(organizaciya) = ? THEN 2
                    WHEN LOWER(nomer) LIKE ? THEN 3
                    WHEN LOWER(organizaciya) LIKE ? THEN 4
                    ELSE 5
                END,
                length(nomer)
            LIMIT ?
            """
            
            params = (
                pattern, pattern, pattern, pattern, pattern,
                query_lower, query_lower,
                f"{query_lower}%", f"{query_lower}%",
                limit
            )
            
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            if data:
                logger.info(f"✓ Найдено {len(data)} результатов по '{query}'")
                return pd.DataFrame([dict(row) for row in data], columns=columns)
            else:
                logger.info(f"⚠️  Результатов не найдено по '{query}'")
                return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"❌ Ошибка поиска: {e}")
            return pd.DataFrame()
    
    def semantic_search_tfidf(self, query: str, limit: int = 100) -> pd.DataFrame:
        """
        Семантический поиск на основе TF-IDF
        
        Работает с темами: если ввести "фарма", выйдут медикаменты, аптеки, здоровье
        Ищет по смыслу, а не по точному совпадению слов
        
        Args:
            query: Поисковая строка
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами отсортированными по релевантности
        
        Примеры:
            >>> search.semantic_search_tfidf("фарма", limit=50)
            >>> search.semantic_search_tfidf("дорога", limit=30)
        """
        if not query or len(query.strip()) < 2:
            logger.warning("⚠️  Поисковая строка слишком короткая")
            return pd.DataFrame()
        
        try:
            # Загружаем все данные (кеш)
            sql = """
            SELECT 
                nomer, organizaciya, opisanie, kategoriya, region,
                byudzhet, status, data
            FROM zakupki
            ORDER BY data DESC
            LIMIT 10000
            """
            
            cursor = self.conn.cursor()
            cursor.execute(sql)
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            if not data:
                logger.warning("⚠️  Данные не найдены в БД")
                return pd.DataFrame()
            
            df = pd.DataFrame([dict(zip(columns, row)) for row in data])
            
            # Создаём поле для анализа (описание + категория + регион)
            df['full_text'] = (
                df['opisanie'].astype(str) + ' ' +
                df['kategoriya'].astype(str) + ' ' +
                df['region'].astype(str) + ' ' +
                df['organizaciya'].astype(str)
            ).str.lower()
            
            # TF-IDF векторизация
            vectorizer = TfidfVectorizer(
                analyzer='char',  # посимвольный анализ для кириллицы
                ngram_range=(2, 3),  # 2-3 буквенные комбинации
                stop_words=None,
                max_features=5000,
                min_df=1
            )
            
            tfidf_matrix = vectorizer.fit_transform(df['full_text'])
            
            # Превращаем query в вектор
            query_vector = vectorizer.transform([query.lower()])
            
            # Считаем схожесть (косинусная схожесть)
            similarities = cosine_similarity(query_vector, tfidf_matrix)[0]
            
            # Фильтруем только релевантные результаты (порог > 0.1)
            relevant_indices = np.where(similarities > 0.1)[0]
            
            if len(relevant_indices) == 0:
                logger.warning(f"⚠️  По запросу '{query}' релевантных результатов не найдено")
                return pd.DataFrame()
            
            # Сортируем по релевантности
            sorted_indices = relevant_indices[np.argsort(similarities[relevant_indices])[::-1]]
            
            # Берём top-N результатов
            result_df = df.iloc[sorted_indices[:limit]].copy()
            result_df['relevance_score'] = similarities[sorted_indices[:limit]]
            
            # Удаляем вспомогательное поле
            result_df = result_df.drop('full_text', axis=1)
            
            logger.info(f"✓ TF-IDF поиск по '{query}': найдено {len(result_df)} результатов")
            
            return result_df
        
        except Exception as e:
            logger.error(f"❌ Ошибка TF-IDF поиска: {e}")
            return pd.DataFrame()

    def combined_search(self, 
                       query: str = "",
                       category: Optional[str] = None,
                       region: Optional[str] = None,
                       min_budget: Optional[float] = None,
                       max_budget: Optional[float] = None,
                       organization: Optional[str] = None,
                       status: Optional[str] = None,
                       limit: int = 100) -> pd.DataFrame:
        """
        Объединенный поиск с несколькими фильтрами
        
        Args:
            query: Текст для полнотекстового поиска
            category: Фильтр по категории
            region: Фильтр по региону
            min_budget: Минимальный бюджет
            max_budget: Максимальный бюджет
            organization: Фильтр по организации
            status: Фильтр по статусу
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        
        Примеры:
            >>> search.combined_search(
            ...     query="медикаменты",
            ...     region="Москва",
            ...     min_budget=100000,
            ...     max_budget=1000000
            ... )
        """
        try:
            # Базовый запрос
            sql = """
            SELECT 
                nomer, 
                organizaciya, 
                opisanie, 
                kategoriya, 
                region, 
                byudzhet, 
                status, 
                data
            FROM zakupki
            WHERE 1=1
            """
            params = []
            
            # Фильтры
            if query and len(query.strip()) > 1:
                query_pattern = f"%{query.lower()}%"
                sql += """ AND (
                    LOWER(nomer) LIKE ? OR
                    LOWER(organizaciya) LIKE ? OR
                    LOWER(opisanie) LIKE ? OR
                    LOWER(kategoriya) LIKE ? OR
                    LOWER(region) LIKE ?
                )"""
                params.extend([query_pattern] * 5)
            
            if category and category != "Все":
                sql += " AND LOWER(kategoriya) = LOWER(?)"
                params.append(category)
            
            if region and region != "Все":
                sql += " AND LOWER(region) = LOWER(?)"
                params.append(region)
            
            if organization and organization != "":
                sql += " AND LOWER(organizaciya) LIKE LOWER(?)"
                params.append(f"%{organization}%")
            
            if status and status != "Все":
                sql += " AND LOWER(status) = LOWER(?)"
                params.append(status)
            
            if min_budget is not None and min_budget >= 0:
                sql += " AND byudzhet >= ?"
                params.append(min_budget)
            
            if max_budget is not None and max_budget >= 0:
                sql += " AND byudzhet <= ?"
                params.append(max_budget)
            
            sql += " LIMIT ?"
            params.append(limit)
            
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            if data:
                logger.info(f"✓ Объединенный поиск: {len(data)} результатов")
                return pd.DataFrame([dict(row) for row in data], columns=columns)
            else:
                logger.info("⚠️  Результатов не найдено по фильтрам")
                return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"❌ Ошибка объединенного поиска: {e}")
            return pd.DataFrame()
    
    def filter_by_category(self, category: str, limit: int = 100) -> pd.DataFrame:
        """
        Фильтр по категории закупок
        
        Args:
            category: Название категории
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        """
        try:
            sql = """
            SELECT * FROM zakupki
            WHERE LOWER(kategoriya) = LOWER(?)
            ORDER BY byudzhet DESC
            LIMIT ?
            """
            cursor = self.conn.cursor()
            cursor.execute(sql, (category, limit))
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            return pd.DataFrame([dict(row) for row in data], columns=columns)
        
        except Exception as e:
            logger.error(f"❌ Ошибка фильтра категории: {e}")
            return pd.DataFrame()
    
    def filter_by_region(self, region: str, limit: int = 100) -> pd.DataFrame:
        """
        Фильтр по региону
        
        Args:
            region: Название региона
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        """
        try:
            sql = """
            SELECT * FROM zakupki
            WHERE LOWER(region) = LOWER(?)
            ORDER BY byudzhet DESC
            LIMIT ?
            """
            cursor = self.conn.cursor()
            cursor.execute(sql, (region, limit))
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            return pd.DataFrame([dict(row) for row in data], columns=columns)
        
        except Exception as e:
            logger.error(f"❌ Ошибка фильтра региона: {e}")
            return pd.DataFrame()
    
    def filter_by_budget(self, min_budget: float, max_budget: float, 
                        limit: int = 100) -> pd.DataFrame:
        """
        Фильтр по диапазону бюджета
        
        Args:
            min_budget: Минимальный бюджет
            max_budget: Максимальный бюджет
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        """
        try:
            sql = """
            SELECT * FROM zakupki
            WHERE byudzhet >= ? AND byudzhet <= ?
            ORDER BY byudzhet DESC
            LIMIT ?
            """
            cursor = self.conn.cursor()
            cursor.execute(sql, (min_budget, max_budget, limit))
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            return pd.DataFrame([dict(row) for row in data], columns=columns)
        
        except Exception as e:
            logger.error(f"❌ Ошибка фильтра бюджета: {e}")
            return pd.DataFrame()
    
    def filter_by_organization(self, org_name: str, limit: int = 100) -> pd.DataFrame:
        """
        Поиск по организации
        
        Args:
            org_name: Название организации (может быть частичным)
            limit: Максимум результатов
        
        Returns:
            DataFrame с результатами
        """
        try:
            sql = """
            SELECT * FROM zakupki
            WHERE LOWER(organizaciya) LIKE LOWER(?)
            ORDER BY byudzhet DESC
            LIMIT ?
            """
            cursor = self.conn.cursor()
            cursor.execute(sql, (f"%{org_name}%", limit))
            
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            return pd.DataFrame([dict(row) for row in data], columns=columns)
        
        except Exception as e:
            logger.error(f"❌ Ошибка фильтра организации: {e}")
            return pd.DataFrame()
    
    def get_categories(self) -> List[str]:
        """
        Получить список всех уникальных категорий
        
        Returns:
            Отсортированный список категорий
        """
        try:
            sql = """
            SELECT DISTINCT kategoriya 
            FROM zakupki 
            WHERE kategoriya IS NOT NULL
            ORDER BY kategoriya
            """
            cursor = self.conn.cursor()
            cursor.execute(sql)
            
            categories = [row[0] for row in cursor.fetchall() if row[0]]
            logger.info(f"✓ Загружено {len(categories)} категорий")
            return categories
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения категорий: {e}")
            return []
    
    def get_regions(self) -> List[str]:
        """
        Получить список всех уникальных регионов
        
        Returns:
            Отсортированный список регионов
        """
        try:
            sql = """
            SELECT DISTINCT region 
            FROM zakupki 
            WHERE region IS NOT NULL
            ORDER BY region
            """
            cursor = self.conn.cursor()
            cursor.execute(sql)
            
            regions = [row[0] for row in cursor.fetchall() if row[0]]
            logger.info(f"✓ Загружено {len(regions)} регионов")
            return regions
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения регионов: {e}")
            return []
    
    def get_statuses(self) -> List[str]:
        """
        Получить список всех уникальных статусов
        
        Returns:
            Отсортированный список статусов
        """
        try:
            sql = """
            SELECT DISTINCT status 
            FROM zakupki 
            WHERE status IS NOT NULL
            ORDER BY status
            """
            cursor = self.conn.cursor()
            cursor.execute(sql)
            
            statuses = [row[0] for row in cursor.fetchall() if row[0]]
            logger.info(f"✓ Загружено {len(statuses)} статусов")
            return statuses
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения статусов: {e}")
            return []
    
    def get_budget_range(self) -> Tuple[float, float]:
        """
        Получить минимальный и максимальный бюджет
        
        Returns:
            Кортеж (минимум, максимум)
        """
        try:
            sql = """
            SELECT 
                MIN(CAST(byudzhet AS REAL)) as min_budget,
                MAX(CAST(byudzhet AS REAL)) as max_budget
            FROM zakupki
            WHERE byudzhet IS NOT NULL AND byudzhet > 0
            """
            cursor = self.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            if row:
                min_b = row[0] or 0
                max_b = row[1] or 1000000
                logger.info(f"✓ Диапазон бюджета: {min_b} - {max_b}")
                return (float(min_b), float(max_b))
            
            return (0.0, 1000000.0)
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения диапазона бюджета: {e}")
            return (0.0, 1000000.0)
    
    def get_total_stats(self) -> Dict:
        """
        Получить общую статистику по всем данным
        
        Returns:
            Словарь со статистикой
        """
        try:
            sql = """
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT kategoriya) as categories_count,
                COUNT(DISTINCT region) as regions_count,
                COUNT(DISTINCT organizaciya) as organizations_count,
                COUNT(DISTINCT status) as statuses_count,
                CAST(AVG(byudzhet) AS REAL) as avg_budget,
                CAST(SUM(byudzhet) AS REAL) as total_budget,
                MIN(CAST(byudzhet AS REAL)) as min_budget,
                MAX(CAST(byudzhet AS REAL)) as max_budget
            FROM zakupki
            WHERE byudzhet IS NOT NULL
            """
            cursor = self.conn.cursor()
            cursor.execute(sql)
            
            row = cursor.fetchone()
            if row:
                stats = {
                    'total_count': row[0] or 0,
                    'categories_count': row[1] or 0,
                    'regions_count': row[2] or 0,
                    'organizations_count': row[3] or 0,
                    'statuses_count': row[4] or 0,
                    'avg_budget': float(row[5]) if row[5] else 0,
                    'total_budget': float(row[6]) if row[6] else 0,
                    'min_budget': float(row[7]) if row[7] else 0,
                    'max_budget': float(row[8]) if row[8] else 0
                }
                logger.info(f"✓ Статистика получена: {stats['total_count']} записей")
                return stats
            
            return {}
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    def get_results_stats(self, df: pd.DataFrame) -> Dict:
        """
        Получить статистику по результатам поиска
        
        Args:
            df: DataFrame с результатами
        
        Returns:
            Словарь со статистикой
        """
        if df.empty:
            return {
                'count': 0,
                'avg_budget': 0,
                'total_budget': 0,
                'min_budget': 0,
                'max_budget': 0,
                'categories': 0,
                'regions': 0
            }
        
        try:
            budget_col = 'byudzhet'
            if budget_col not in df.columns:
                return {'count': len(df)}
            
            df_numeric = pd.to_numeric(df[budget_col], errors='coerce')
            
            return {
                'count': len(df),
                'avg_budget': float(df_numeric.mean()) if not df_numeric.empty else 0,
                'total_budget': float(df_numeric.sum()) if not df_numeric.empty else 0,
                'min_budget': float(df_numeric.min()) if not df_numeric.empty else 0,
                'max_budget': float(df_numeric.max()) if not df_numeric.empty else 0,
                'categories': df['kategoriya'].nunique() if 'kategoriya' in df.columns else 0,
                'regions': df['region'].nunique() if 'region' in df.columns else 0
            }
        
        except Exception as e:
            logger.error(f"❌ Ошибка расчета статистики: {e}")
            return {'count': len(df)}
    
    def close(self):
        """Закрыть соединение с БД"""
        if self.conn:
            self.conn.close()
            logger.info("✓ Соединение с БД закрыто")


# ============================================================================
# ПРИМЕРЫ И ТЕСТЫ
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("ТЕСТ УМНОГО ПОИСКА")
    print("=" * 80)
    print()
    
    try:
        # 1. Инициализация
        print("1️⃣  Инициализация SmartSearch...")
        search = SmartSearch()
        print()
        
        # 2. Получение справочников
        print("2️⃣  Получение справочников...")
        categories = search.get_categories()
        regions = search.get_regions()
        statuses = search.get_statuses()
        budget_min, budget_max = search.get_budget_range()
        
        print(f"   Категорий: {len(categories)}")
        print(f"   Регионов: {len(regions)}")
        print(f"   Статусов: {len(statuses)}")
        print(f"   Бюджет: {budget_min:,.0f} - {budget_max:,.0f}")
        print()
        
        # 3. Общая статистика
        print("3️⃣  Общая статистика...")
        stats = search.get_total_stats()
        print(f"   Всего закупок: {stats.get('total_count', 0):,}")
        print(f"   Всего бюджета: {stats.get('total_budget', 0):,.0f}")
        print(f"   Средний бюджет: {stats.get('avg_budget', 0):,.0f}")
        print()
        
        # 4. Полнотекстовый поиск
        print("4️⃣  Полнотекстовый поиск...")
        results = search.full_search("медикаменты", limit=10)
        if not results.empty:
            print(f"   Найдено: {len(results)} результатов")
            print(results[['nomer', 'organizaciya', 'kategoriya', 'byudzhet']].head(3))
        print()
        
        # 5. Фильтр по региону
        print("5️⃣  Фильтр по региону...")
        if regions:
            region_results = search.filter_by_region(regions[0], limit=5)
            print(f"   Регион: {regions[0]}")
            print(f"   Результатов: {len(region_results)}")
        print()
        
        # 6. Объединенный поиск
        print("6️⃣  Объединенный поиск (с фильтрами)...")
        combined_results = search.combined_search(
            query="поставка",
            region=regions[0] if regions else None,
            min_budget=100000,
            max_budget=1000000,
            limit=10
        )
        print(f"   Результатов: {len(combined_results)}")
        
        # 7. Статистика по результатам
        if not combined_results.empty:
            result_stats = search.get_results_stats(combined_results)
            print(f"   Средний бюджет результатов: {result_stats['avg_budget']:,.0f}")
            print(f"   Общий бюджет результатов: {result_stats['total_budget']:,.0f}")
        print()
        
        # 8. Закрытие соединения
        search.close()
        
        print("✅ ВСЕ ТЕСТЫ УСПЕШНЫ")
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    print("=" * 80)
