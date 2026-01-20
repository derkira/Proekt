"""
Тесты масштабируемости Big Data ETL Pipeline

Проверяет линейность зависимости время ~ объем данных
для различных операций (генерация, очистка, агрегация)
"""

import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging
import json
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, avg, max, min
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        LongType, IntegerType, TimestampType
    )
except ImportError:
    print("PySpark не установлен!")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class ScalabilityTester:
    """Тестер масштабируемости Big Data pipeline"""
    
    def __init__(self, spark_master: str = "spark://spark-master:7077"):
        """Инициализация Spark сессии"""
        try:
            self.spark = SparkSession.builder \
                .appName("scalability-test") \
                .master(spark_master) \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.results = []
            
            logger.info(f"✓ Spark сессия инициализирована")
        except Exception as e:
            logger.error(f"Ошибка подключения к Spark: {e}")
            sys.exit(1)
    
    def generate_data(self, num_records: int) -> Tuple:
        """Генерация синтетических данных"""
        logger.info(f"Генерация {num_records:,} записей...")
        start_time = time.time()
        
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("nomer", StringType(), True),
            StructField("organizaciya", StringType(), True),
            StructField("opisanie", StringType(), True),
            StructField("kategoriya", StringType(), True),
            StructField("region", StringType(), True),
            StructField("byudzhet", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("data", TimestampType(), True),
        ])
        
        categories = [
            'Медицина', 'Транспорт', 'Строительство', 'Энергетика',
            'IT', 'Образование', 'Жилье', 'Благоустройство'
        ]
        regions = ['Москва', 'СПб', 'Екатеринбург', 'Новосибирск', 'Казань', 'Краснодар']
        statuses = ['Объявлена', 'Закрыта', 'В работе', 'Отменена']
        
        # Генерируем через RDD для параллелизма
        rdd = self.spark.sparkContext.parallelize(
            range(num_records),
            numPartitions=self.spark.sparkContext.defaultParallelism * 2
        ).map(lambda x: (
            x,
            f"44-{1000000 + x:07d}",
            f"Org_{x % 100}",
            f"Закупка тип {x % 10}",
            categories[x % len(categories)],
            regions[x % len(regions)],
            float(np.random.lognormal(10, 2)),
            statuses[x % len(statuses)],
            datetime.now() - timedelta(days=x % 365)
        ))
        
        df = self.spark.createDataFrame(rdd, schema=schema)
        
        # Кэшируем для повторного использования
        df.cache()
        df.count()  # Trigger evaluation
        
        elapsed = time.time() - start_time
        logger.info(f"✓ Сгенерировано {num_records:,} записей за {elapsed:.2f} сек")
        
        return df, elapsed
    
    def clean_data(self, df) -> Tuple:
        """Очистка данных"""
        logger.info("Очистка и дедубликация...")
        start_time = time.time()
        
        df_clean = df.dropna().dropDuplicates()
        count = df_clean.count()
        
        elapsed = time.time() - start_time
        logger.info(f"✓ Очищено {count:,} записей за {elapsed:.2f} сек")
        
        return df_clean, elapsed
    
    def aggregate_data(self, df) -> Tuple:
        """Агрегирование данных"""
        logger.info("Агрегирование данных...")
        start_time = time.time()
        
        agg_df = df.groupby('kategoriya').agg(
            count('*').alias('count'),
            avg('byudzhet').alias('avg_budget'),
            max('byudzhet').alias('max_budget'),
            min('byudzhet').alias('min_budget')
        )
        count = agg_df.count()
        
        elapsed = time.time() - start_time
        logger.info(f"✓ Агрегировано {count:,} групп за {elapsed:.2f} сек")
        
        return agg_df, elapsed
    
    def run_scalability_test(self, sizes: List[int]) -> List[Dict]:
        """Запуск тестов масштабируемости"""
        logger.info("="*60)
        logger.info("НАЧАЛО ТЕСТОВ МАСШТАБИРУЕМОСТИ")
        logger.info("="*60)
        
        results = []
        
        for size in sizes:
            logger.info(f"\n--- ТЕСТ: {size:,} записей ---")
            
            try:
                # Генерация
                df, gen_time = self.generate_data(size)
                
                # Очистка
                df_clean, clean_time = self.clean_data(df)
                
                # Агрегирование
                df_agg, agg_time = self.aggregate_data(df_clean)
                
                total_time = gen_time + clean_time + agg_time
                
                result = {
                    'records': size,
                    'gen_time': gen_time,
                    'clean_time': clean_time,
                    'agg_time': agg_time,
                    'total_time': total_time,
                    'throughput': size / total_time
                }
                results.append(result)
                
                logger.info(f"✓ ИТОГО: {total_time:.2f} сек (пропускная способность: {result['throughput']:,.0f} записей/сек)")
                
                # Очистка памяти
                df.unpersist()
                
            except Exception as e:
                logger.error(f"✗ Ошибка: {e}")
                break
        
        self.results = results
        return results
    
    def stop(self):
        """Остановка Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("✓ Spark сессия закрыта")

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Тесты масштабируемости Big Data ETL")
    parser.add_argument(
        "--sizes",
        type=str,
        default="100000,500000,1000000,2000000,5000000",
        help="Размеры для тестирования (через запятую)"
    )
    parser.add_argument(
        "--spark-master",
        type=str,
        default="spark://spark-master:7077",
        help="Адрес Spark Master"
    )
    
    args = parser.parse_args()
    
    sizes = [int(s.strip()) for s in args.sizes.split(',')]
    
    tester = ScalabilityTester(spark_master=args.spark_master)
    
    try:
        # Запуск тестов
        results = tester.run_scalability_test(sizes)
        
        # Сохранение результатов
        output_file = Path("logs/scalability_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(tester.results, f, indent=2)
        
        logger.info(f"✓ Результаты сохранены в {output_file}")
        
    finally:
        tester.stop()

if __name__ == "__main__":
    main()
