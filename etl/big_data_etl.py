"""
Расширенный ETL пайплайн для работы с большими данными (миллионы записей)
Поддержка HDFS, Hive, Spark для параллельной обработки
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta
import json

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, desc, asc, year, month, dayofmonth,
    row_number, broadcast, coalesce, concat_ws
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigDataHDFSManager:
    """Менеджер для работы с HDFS и большими данными"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.fs = self.spark.sparkContext._jsc.hadoopConfiguration()
        
    def read_hdfs_parquet(self, path: str) -> 'pyspark.sql.DataFrame':
        """Чтение Parquet файлов из HDFS"""
        try:
            df = self.spark.read.parquet(path)
            logger.info(f"✓ Loaded from HDFS: {path} ({df.count()} rows)")
            return df
        except Exception as e:
            logger.error(f"✗ Failed to read HDFS: {e}")
            return None
    
    def write_hdfs_parquet(self, df: 'pyspark.sql.DataFrame', path: str, mode: str = 'overwrite'):
        """Запись Parquet файлов в HDFS"""
        try:
            df.write.mode(mode).parquet(path)
            row_count = df.count()
            logger.info(f"✓ Written to HDFS: {path} ({row_count} rows)")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to write HDFS: {e}")
            return False
    
    def read_hdfs_csv(self, path: str, header: bool = True) -> 'pyspark.sql.DataFrame':
        """Чтение CSV файлов из HDFS"""
        try:
            df = self.spark.read.csv(path, header=header, inferSchema=True)
            logger.info(f"✓ Loaded CSV from HDFS: {path}")
            return df
        except Exception as e:
            logger.error(f"✗ Failed to read CSV from HDFS: {e}")
            return None
    
    def write_hive_table(self, df: 'pyspark.sql.DataFrame', table_name: str, mode: str = 'overwrite'):
        """Запись данных в Hive таблицу"""
        try:
            df.write.mode(mode).format('hive').saveAsTable(table_name)
            logger.info(f"✓ Written to Hive table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to write to Hive: {e}")
            return False


class DataCleaner:
    """Очистка и валидация данных для больших объемов"""
    
    @staticmethod
    def remove_duplicates(df: 'pyspark.sql.DataFrame', subset: List[str]) -> 'pyspark.sql.DataFrame':
        """Удаление дубликатов по указанным столбцам"""
        initial_count = df.count()
        df_clean = df.dropDuplicates(subset)
        final_count = df_clean.count()
        logger.info(f"📊 Duplicates removed: {initial_count - final_count} rows")
        return df_clean
    
    @staticmethod
    def handle_nulls(df: 'pyspark.sql.DataFrame', strategy: str = 'drop') -> 'pyspark.sql.DataFrame':
        """Обработка NULL значений"""
        if strategy == 'drop':
            df_clean = df.na.drop()
        elif strategy == 'mean':
            # Заполнение числовых столбцов средним значением
            numeric_cols = [c for c, t in df.dtypes if 'int' in t or 'double' in t]
            means = df.select([avg(c) for c in numeric_cols]).collect()[0]
            fill_dict = {numeric_cols[i]: means[i] for i in range(len(numeric_cols))}
            df_clean = df.fillna(fill_dict)
        else:
            df_clean = df
        
        logger.info(f"📊 Nulls handled with strategy: {strategy}")
        return df_clean
    
    @staticmethod
    def filter_invalid_records(df: 'pyspark.sql.DataFrame', rules: Dict) -> 'pyspark.sql.DataFrame':
        """Фильтрация невалидных записей по правилам"""
        for col_name, condition in rules.items():
            df = df.filter(condition)
        
        logger.info(f"📊 Invalid records filtered")
        return df


class BigDataETLPipeline:
    """Расширенный ETL пайплайн для работы с большими данными"""
    
    def __init__(self, app_name: str = "GosZakupki-BigData-ETL"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.hdfs_manager = BigDataHDFSManager(self.spark)
        self.data_cleaner = DataCleaner()
        self.data_lake_path = os.getenv('DATA_LAKE_PATH', './data/lake')
        self.hdfs_path = os.getenv('HDFS_PATH', 'hdfs://namenode:9000/user/hive/warehouse')
        
        logger.info("✓ BigDataETLPipeline initialized")
    
    def extract_raw_data(self, source_path: str) -> 'pyspark.sql.DataFrame':
        """Извлечение сырых данных из различных источников"""
        logger.info(f"📥 Extracting data from: {source_path}")
        
        if source_path.endswith('.csv'):
            df = self.spark.read.csv(source_path, header=True, inferSchema=True)
        elif source_path.endswith('.parquet'):
            df = self.spark.read.parquet(source_path)
        elif source_path.endswith('.json'):
            df = self.spark.read.json(source_path)
        else:
            raise ValueError(f"Unsupported file format: {source_path}")
        
        row_count = df.count()
        logger.info(f"✓ Extracted {row_count:,} rows")
        return df
    
    def transform_large_dataset(self, df: 'pyspark.sql.DataFrame') -> 'pyspark.sql.DataFrame':
        """Трансформация больших датасетов с оптимизацией"""
        logger.info("🔄 Starting data transformation")
        
        # 1. Удаление дубликатов
        df = self.data_cleaner.remove_duplicates(df, subset=['id'])
        
        # 2. Обработка NULL значений
        df = self.data_cleaner.handle_nulls(df, strategy='drop')
        
        # 3. Преобразование типов данных
        df = df.select([
            when(col(c).cast("double").isNotNull(), col(c).cast("double"))
            .otherwise(col(c))
            .alias(c) if dtype.startswith('string') else col(c)
            for c, dtype in df.dtypes
        ])
        
        # 4. Добавление вычисляемых полей
        if 'sum_value' in df.columns and 'quantity' in df.columns:
            df = df.withColumn(
                'unit_price',
                coalesce(col('sum_value') / col('quantity'), 0)
            )
        
        # 5. Добавление временных характеристик
        if 'date' in df.columns:
            df = df \
                .withColumn('year', year(col('date'))) \
                .withColumn('month', month(col('date'))) \
                .withColumn('day', dayofmonth(col('date')))
        
        logger.info(f"✓ Transformation completed")
        return df
    
    def aggregate_by_dimensions(self, df: 'pyspark.sql.DataFrame') -> 'pyspark.sql.DataFrame':
        """Агрегация по различным измерениям"""
        logger.info("📊 Aggregating data by dimensions")
        
        # Агрегация по категориям и регионам
        aggregated = df.groupBy('category', 'region') \
            .agg(
                count('*').alias('count'),
                spark_sum('sum_value').alias('total_value'),
                avg('sum_value').alias('avg_value'),
                spark_max('sum_value').alias('max_value'),
                spark_min('sum_value').alias('min_value')
            ) \
            .orderBy(desc('total_value'))
        
        logger.info(f"✓ Aggregated {aggregated.count()} groups")
        return aggregated
    
    def apply_window_functions(self, df: 'pyspark.sql.DataFrame') -> 'pyspark.sql.DataFrame':
        """Применение оконных функций для рангирования и анализа"""
        logger.info("📈 Applying window functions")
        
        window_spec = Window.partitionBy('category').orderBy(desc('sum_value'))
        df_with_rank = df.withColumn('rank', row_number().over(window_spec))
        
        logger.info(f"✓ Window functions applied")
        return df_with_rank
    
    def load_to_data_lake(self, df: 'pyspark.sql.DataFrame', layer: str = 'processed'):
        """Загрузка обработанных данных в Data Lake"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"{self.data_lake_path}/{layer}/data_{timestamp}"
        
        try:
            df.write.mode('overwrite').parquet(path)
            logger.info(f"✓ Data loaded to Data Lake: {path}")
            return path
        except Exception as e:
            logger.error(f"✗ Failed to load to Data Lake: {e}")
            return None
    
    def load_to_hive(self, df: 'pyspark.sql.DataFrame', table_name: str):
        """Загрузка данных в Hive для запросов"""
        try:
            df.write.mode('overwrite').format('hive').saveAsTable(table_name)
            logger.info(f"✓ Data loaded to Hive table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to load to Hive: {e}")
            return False
    
    def get_statistics(self) -> Dict:
        """Получение статистики по обработанным данным"""
        stats = {
            'timestamp': datetime.now().isoformat(),
            'spark_version': self.spark.version,
            'app_name': self.spark.sparkContext.appName,
            'default_parallelism': self.spark.sparkContext.defaultParallelism,
            'executors': {
                'count': self.spark.sparkContext.defaultMinPartitions,
                'memory': self.spark.conf.get('spark.executor.memory', 'default'),
            }
        }
        logger.info(f"📊 Statistics: {json.dumps(stats, indent=2)}")
        return stats
    
    def stop(self):
        """Остановка Spark сессии"""
        self.spark.stop()
        logger.info("✓ Spark session stopped")


class ScalabilityAnalyzer:
    """Анализ масштабируемости и производительности"""
    
    def __init__(self, pipeline: BigDataETLPipeline):
        self.pipeline = pipeline
        self.results = []
    
    def generate_test_data(self, size: int) -> 'pyspark.sql.DataFrame':
        """Генерация тестовых данных"""
        logger.info(f"🔧 Generating test data: {size:,} rows")
        
        spark = self.pipeline.spark
        data = [
            (
                i,
                f"Tender_{i % 1000}",
                f"Category_{i % 50}",
                f"Region_{i % 30}",
                np.random.randint(1000, 100000000),
                np.random.randint(1, 100),
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                f"Org_{i % 200}",
                np.random.choice(['Open', 'Restricted', 'Cancelled'], 1)[0]
            )
            for i in range(size)
        ]
        
        schema = """
            id INT, tender_name STRING, category STRING, region STRING,
            sum_value LONG, quantity INT, date STRING, organization STRING, status STRING
        """
        
        df = spark.createDataFrame(data, schema=schema)
        logger.info(f"✓ Generated test data: {df.count():,} rows")
        return df
    
    def measure_performance(self, data_sizes: List[int]) -> Dict:
        """Измерение производительности для различных размеров данных"""
        logger.info(f"⏱️  Measuring performance for sizes: {data_sizes}")
        
        results = {}
        
        for size in data_sizes:
            logger.info(f"\n{'='*50}")
            logger.info(f"Testing with {size:,} rows")
            logger.info(f"{'='*50}")
            
            # Генерация данных
            df = self.generate_test_data(size)
            
            # Измерение трансформации
            start_time = datetime.now()
            df_transformed = self.pipeline.transform_large_dataset(df)
            df_transformed.count()  # Force execution
            transform_time = (datetime.now() - start_time).total_seconds()
            
            # Измерение агрегации
            start_time = datetime.now()
            df_aggregated = self.pipeline.aggregate_by_dimensions(df_transformed)
            df_aggregated.count()  # Force execution
            aggregate_time = (datetime.now() - start_time).total_seconds()
            
            # Итоговое время
            total_time = transform_time + aggregate_time
            
            results[size] = {
                'size': size,
                'transform_time': transform_time,
                'aggregate_time': aggregate_time,
                'total_time': total_time,
                'rows_per_second': size / total_time if total_time > 0 else 0,
                'time_per_million': (total_time / size) * 1_000_000 if size > 0 else 0
            }
            
            logger.info(f"✓ Transform: {transform_time:.2f}s, Aggregate: {aggregate_time:.2f}s")
            logger.info(f"✓ Total: {total_time:.2f}s ({results[size]['rows_per_second']:.0f} rows/sec)")
        
        self.results = results
        return results
    
    def analyze_scalability(self) -> Dict:
        """Анализ линейности масштабируемости"""
        if not self.results:
            logger.warning("No results to analyze")
            return {}
        
        sizes = sorted(self.results.keys())
        times = [self.results[s]['total_time'] for s in sizes]
        
        # Вычисление линейности
        if len(sizes) >= 2:
            # Линейная регрессия
            x = np.array(sizes).reshape(-1, 1)
            y = np.array(times)
            
            # Простая формула: y = a * x
            a = np.sum(x.flatten() * y) / np.sum(x.flatten() ** 2)
            
            # R-squared
            y_pred = a * x.flatten()
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            analysis = {
                'coefficient': a,
                'r_squared': r_squared,
                'is_linear': r_squared > 0.95,
                'linearity_score': r_squared
            }
            
            logger.info(f"\n📈 Scalability Analysis:")
            logger.info(f"   Linear coefficient: {a:.6f} seconds/row")
            logger.info(f"   R²: {r_squared:.4f}")
            logger.info(f"   Is linear: {'✓ Yes' if r_squared > 0.95 else '✗ No'}")
            
            return analysis
        
        return {}


def main():
    """Главная функция запуска ETL пайплайна"""
    logger.info("="*60)
    logger.info("🚀 Starting Big Data ETL Pipeline")
    logger.info("="*60)
    
    # Инициализация
    pipeline = BigDataETLPipeline()
    
    try:
        # Пример использования
        test_data_path = "./data/lake/raw/sample_data.csv"
        
        if os.path.exists(test_data_path):
            # Извлечение
            df_raw = pipeline.extract_raw_data(test_data_path)
            
            # Трансформация
            df_transformed = pipeline.transform_large_dataset(df_raw)
            
            # Агрегация
            df_aggregated = pipeline.aggregate_by_dimensions(df_transformed)
            
            # Загрузка в Data Lake
            pipeline.load_to_data_lake(df_aggregated, layer='processed')
            
            # Загрузка в Hive
            pipeline.load_to_hive(df_aggregated, 'goszakupki_aggregated')
        
        # Статистика
        stats = pipeline.get_statistics()
        
    except Exception as e:
        logger.error(f"✗ Error during ETL execution: {e}", exc_info=True)
    finally:
        pipeline.stop()
    
    logger.info("="*60)
    logger.info("✓ ETL Pipeline completed")
    logger.info("="*60)


if __name__ == "__main__":
    main()
