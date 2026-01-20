"""
Apache Spark интеграция для обработки больших данных

Этот модуль обеспечивает:
- Чтение и запись данных в HDFS
- Обработка структурированных данных (CSV, Parquet)
- Параллельная обработка с использованием RDD и DataFrame
- Оптимизация для линейной масштабируемости
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import logging

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, when, count, sum, avg, max, min, 
        to_timestamp, window, percentile_approx
    )
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    from pyspark.rdd import RDD
except ImportError:
    print("PySpark не установлен! Установите: pip install pyspark")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataProcessor:
    """Обработчик больших данных с использованием Apache Spark"""
    
    def __init__(self, app_name: str = "goszakupki", master: str = "spark://spark-master:7077"):
        """
        Инициализация Spark сессии
        
        Args:
            app_name: Имя приложения
            master: URL мастера Spark (для Docker: spark://spark-master:7077)
        """
        self.app_name = app_name
        self.master = master
        self.spark = self._init_spark_session()
        self.hdfs_path = "hdfs://namenode:9000"
        logger.info(f"✓ Spark сессия инициализирована: {self.app_name}")
    
    def _init_spark_session(self) -> SparkSession:
        """Создание Spark сессии"""
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()
        
        # Отключаем verbose logging
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def read_csv_from_hdfs(self, hdfs_path: str, **options) -> Optional[DataFrame]:
        """
        Чтение CSV файла из HDFS
        
        Args:
            hdfs_path: Путь в HDFS
            **options: Опции для spark.read.csv()
        
        Returns:
            DataFrame или None при ошибке
        """
        try:
            full_path = f"{self.hdfs_path}{hdfs_path}"
            logger.info(f"Чтение CSV из {full_path}")
            
            df = self.spark.read \
                .option("header", options.get("header", "true")) \
                .option("inferSchema", options.get("inferSchema", "true")) \
                .option("encoding", "utf-8") \
                .csv(full_path)
            
            logger.info(f"✓ Загружено {df.count()} записей")
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения CSV: {e}")
            return None
    
    def read_parquet_from_hdfs(self, hdfs_path: str) -> Optional[DataFrame]:
        """
        Чтение Parquet файла из HDFS (оптимален для больших данных)
        
        Args:
            hdfs_path: Путь в HDFS
        
        Returns:
            DataFrame или None при ошибке
        """
        try:
            full_path = f"{self.hdfs_path}{hdfs_path}"
            logger.info(f"Чтение Parquet из {full_path}")
            
            df = self.spark.read.parquet(full_path)
            logger.info(f"✓ Загружено {df.count()} записей из Parquet")
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения Parquet: {e}")
            return None
    
    def write_parquet_to_hdfs(self, df: DataFrame, hdfs_path: str, mode: str = "overwrite"):
        """
        Запись DataFrame в Parquet на HDFS (оптимально для больших данных)
        
        Args:
            df: DataFrame для записи
            hdfs_path: Путь в HDFS
            mode: Режим записи (overwrite, append, ignore, error)
        """
        try:
            full_path = f"{self.hdfs_path}{hdfs_path}"
            logger.info(f"Запись Parquet в {full_path}")
            
            df.write.mode(mode).parquet(full_path)
            logger.info(f"✓ Записано {df.count()} записей в Parquet")
        except Exception as e:
            logger.error(f"Ошибка записи Parquet: {e}")
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Очистка и обработка данных
        
        Args:
            df: Исходный DataFrame
        
        Returns:
            Очищенный DataFrame
        """
        logger.info("Очистка данных...")
        
        # Удаляем пустые строки
        df = df.dropna()
        
        # Удаляем дубликаты по всем колонкам
        df = df.dropDuplicates()
        
        # Преобразуем типы данных
        for col_name in df.columns:
            if "budget" in col_name.lower() or "sum" in col_name.lower():
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            elif "date" in col_name.lower():
                df = df.withColumn(col_name, to_timestamp(col(col_name)))
        
        logger.info(f"✓ Очищено: {df.count()} записей")
        return df
    
    def aggregate_data(self, df: DataFrame, group_by_col: str) -> DataFrame:
        """
        Агрегирование данных
        
        Args:
            df: Исходный DataFrame
            group_by_col: Колонка для группировки
        
        Returns:
            Агрегированный DataFrame
        """
        logger.info(f"Агрегирование по {group_by_col}...")
        
        agg_df = df.groupby(group_by_col).agg({
            "budget" if "budget" in df.columns else df.columns[0]: "sum",
            "id" if "id" in df.columns else df.columns[0]: "count"
        }).withColumnRenamed(f"sum(budget)", "total_budget") \
         .withColumnRenamed(f"count(id)", "count")
        
        logger.info(f"✓ Агрегировано: {agg_df.count()} групп")
        return agg_df
    
    def parallel_partition(self, df: DataFrame, num_partitions: int = None) -> DataFrame:
        """
        Оптимизация разбиения на партиции для параллельной обработки
        
        Args:
            df: DataFrame
            num_partitions: Количество партиций
        
        Returns:
            Переразбитый DataFrame
        """
        if num_partitions is None:
            num_partitions = self.spark.sparkContext.defaultParallelism * 2
        
        logger.info(f"Переразбиение на {num_partitions} партиции")
        df = df.repartition(num_partitions)
        logger.info(f"✓ DataFrame переразбит на {df.rdd.getNumPartitions()} партиции")
        return df
    
    def estimate_partition_size(self, df: DataFrame):
        """
        Оценка размера партиции для оптимизации
        
        Args:
            df: DataFrame
        """
        total_size = df.count()
        num_partitions = df.rdd.getNumPartitions()
        avg_partition_size = total_size / num_partitions if num_partitions > 0 else 0
        
        logger.info(f"Статистика партиций:")
        logger.info(f"  Всего записей: {total_size}")
        logger.info(f"  Партиций: {num_partitions}")
        logger.info(f"  Среднее на партицию: {avg_partition_size:.0f}")
        
        return {
            "total_records": total_size,
            "num_partitions": num_partitions,
            "avg_per_partition": avg_partition_size
        }
    
    def filter_and_transform(self, df: DataFrame, filters: Dict[str, Any]) -> DataFrame:
        """
        Фильтрация и трансформация данных
        
        Args:
            df: Исходный DataFrame
            filters: Словарь фильтров {column: value}
        
        Returns:
            Отфильтрованный DataFrame
        """
        logger.info(f"Применение фильтров: {filters}")
        
        for col_name, value in filters.items():
            if col_name in df.columns:
                if isinstance(value, (list, tuple)):
                    df = df.filter(col(col_name).isin(value))
                else:
                    df = df.filter(col(col_name) == value)
        
        logger.info(f"✓ Отфильтровано: {df.count()} записей")
        return df
    
    def sql_query(self, query: str) -> Optional[DataFrame]:
        """
        Выполнение SQL запроса
        
        Args:
            query: SQL запрос
        
        Returns:
            DataFrame с результатами
        """
        try:
            logger.info(f"Выполнение SQL запроса")
            result = self.spark.sql(query)
            logger.info(f"✓ Результат: {result.count()} записей")
            return result
        except Exception as e:
            logger.error(f"Ошибка SQL: {e}")
            return None
    
    def show_stats(self, df: DataFrame, col_name: str = None):
        """
        Вывод статистики DataFrame
        
        Args:
            df: DataFrame
            col_name: Колонка для анализа (опционально)
        """
        logger.info(f"Статистика DataFrame:")
        logger.info(f"  Строк: {df.count()}")
        logger.info(f"  Колонок: {len(df.columns)}")
        logger.info(f"  Колонки: {df.columns}")
        
        if col_name and col_name in df.columns:
            stats = df.select(
                count(col_name).alias("count"),
                avg(col_name).alias("avg"),
                min(col_name).alias("min"),
                max(col_name).alias("max")
            ).collect()[0]
            
            logger.info(f"  {col_name}: count={stats.count}, avg={stats.avg:.2f}, min={stats.min}, max={stats.max}")
    
    def stop(self):
        """Остановка Spark сессии"""
        if self.spark:
            logger.info("Остановка Spark сессии...")
            self.spark.stop()
            logger.info("✓ Spark сессия остановлена")

# Пример использования
if __name__ == "__main__":
    processor = SparkDataProcessor()
    
    # Пример: чтение CSV
    # df = processor.read_csv_from_hdfs("/data/raw/zakupki.csv")
    # df = processor.clean_data(df)
    # processor.write_parquet_to_hdfs(df, "/data/processed/zakupki.parquet")
    
    processor.stop()
