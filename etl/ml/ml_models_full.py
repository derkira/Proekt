"""
Машинное обучение для прогнозирования стоимости закупок
Анализ влияния объема данных на точность модели
Встроение в общий ETL пайплайн
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, Tuple, List, Optional
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, rand
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
    from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler as SklearnScaler, LabelEncoder
from sklearn.linear_model import LinearRegression as SklearnLinearRegression
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor as SklearnRandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProcurementDataGenerator:
    """Генератор синтетических данных о закупках"""
    
    @staticmethod
    def generate_data(num_records: int, random_seed: int = 42) -> pd.DataFrame:
        """Генерация данных о закупках"""
        np.random.seed(random_seed)
        
        logger.info(f"🔧 Generating {num_records:,} procurement records")
        
        categories = [f'Category_{i}' for i in range(1, 51)]
        regions = [f'Region_{i}' for i in range(1, 31)]
        organizations = [f'Organization_{i}' for i in range(1, 201)]
        statuses = ['Open', 'Closed', 'Cancelled']
        procurement_types = ['Goods', 'Services', 'Works']
        
        data = {
            'id': range(1, num_records + 1),
            'category': np.random.choice(categories, num_records),
            'region': np.random.choice(regions, num_records),
            'organization': np.random.choice(organizations, num_records),
            'status': np.random.choice(statuses, num_records),
            'procurement_type': np.random.choice(procurement_types, num_records),
            'quantity': np.random.randint(1, 1000, num_records),
            'unit_price': np.random.lognormal(mean=8, sigma=2, size=num_records),  # Log-normal distribution
            'supplier_rating': np.random.uniform(0, 5, num_records),
            'contract_duration_days': np.random.randint(1, 365, num_records),
            'competitive_level': np.random.randint(1, 5, num_records),  # 1 = no competition, 4 = high competition
            'advance_payment_pct': np.random.uniform(0, 50, num_records),
        }
        
        df = pd.DataFrame(data)
        
        # Target variable: total_cost
        df['total_cost'] = (
            df['quantity'] * df['unit_price'] *
            (1 + np.random.normal(0, 0.1, num_records)) *  # Random variation
            (0.5 + 0.5 * df['competitive_level'] / 4)  # Higher competition = more cost
        )
        
        logger.info(f"✓ Generated {len(df):,} records")
        logger.info(f"  Cost range: {df['total_cost'].min():,.0f} - {df['total_cost'].max():,.0f}")
        
        return df


class ProcurementMLModel:
    """ML модель для прогнозирования стоимости закупок"""
    
    def __init__(self, model_type: str = 'gradient_boosting'):
        self.model_type = model_type
        self.model = None
        self.scaler = SklearnScaler()
        self.label_encoders = {}
        self.feature_columns = None
        self.categorical_columns = ['category', 'region', 'organization', 'status', 'procurement_type']
        self.numerical_columns = ['quantity', 'unit_price', 'supplier_rating', 'contract_duration_days',
                                 'competitive_level', 'advance_payment_pct']
        self.metrics = {}
    
    def prepare_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """Подготовка данных для обучения"""
        logger.info("🔄 Preparing data for training")
        
        df_copy = df.copy()
        
        # Кодирование категориальных переменных
        for col in self.categorical_columns:
            if col not in self.label_encoders:
                self.label_encoders[col] = LabelEncoder()
                df_copy[col] = self.label_encoders[col].fit_transform(df_copy[col])
            else:
                df_copy[col] = self.label_encoders[col].transform(df_copy[col])
        
        # Подготовка признаков
        X = df_copy[self.categorical_columns + self.numerical_columns].copy()
        X = self.scaler.fit_transform(X)
        
        # Target
        y = df_copy['total_cost'].values
        
        self.feature_columns = self.categorical_columns + self.numerical_columns
        
        logger.info(f"✓ Data prepared: {X.shape}")
        return X, y
    
    def train(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """Обучение модели"""
        logger.info(f"🎓 Training {self.model_type} model")
        
        # Разделение на train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Выбор модели
        if self.model_type == 'linear':
            self.model = SklearnLinearRegression()
        elif self.model_type == 'gradient_boosting':
            self.model = GradientBoostingRegressor(
                n_estimators=100, learning_rate=0.1, max_depth=5,
                random_state=42, verbose=0
            )
        elif self.model_type == 'random_forest':
            self.model = SklearnRandomForestRegressor(
                n_estimators=50, max_depth=10, random_state=42,
                n_jobs=-1
            )
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")
        
        # Обучение
        self.model.fit(X_train, y_train)
        
        # Оценка
        y_train_pred = self.model.predict(X_train)
        y_test_pred = self.model.predict(X_test)
        
        # Метрики
        train_mse = mean_squared_error(y_train, y_train_pred)
        test_mse = mean_squared_error(y_test, y_test_pred)
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        
        self.metrics = {
            'train_mse': train_mse,
            'test_mse': test_mse,
            'train_rmse': np.sqrt(train_mse),
            'test_rmse': np.sqrt(test_mse),
            'train_r2': train_r2,
            'test_r2': test_r2,
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_size': len(X_train),
            'test_size': len(X_test)
        }
        
        logger.info(f"✓ Model trained")
        logger.info(f"  Train RMSE: {self.metrics['train_rmse']:,.0f}")
        logger.info(f"  Test RMSE: {self.metrics['test_rmse']:,.0f}")
        logger.info(f"  Train R²: {self.metrics['train_r2']:.4f}")
        logger.info(f"  Test R²: {self.metrics['test_r2']:.4f}")
        
        return self.metrics
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Предсказание"""
        if self.model is None:
            raise ValueError("Model not trained yet")
        return self.model.predict(X)
    
    def save(self, path: str):
        """Сохранение модели"""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({
            'model': self.model,
            'scaler': self.scaler,
            'label_encoders': self.label_encoders,
            'feature_columns': self.feature_columns,
            'metrics': self.metrics
        }, path)
        logger.info(f"✓ Model saved: {path}")
    
    def load(self, path: str):
        """Загрузка модели"""
        data = joblib.load(path)
        self.model = data['model']
        self.scaler = data['scaler']
        self.label_encoders = data['label_encoders']
        self.feature_columns = data['feature_columns']
        self.metrics = data['metrics']
        logger.info(f"✓ Model loaded: {path}")


class DataVolumeImpactAnalyzer:
    """Анализ влияния объема данных на точность модели"""
    
    def __init__(self):
        self.results = []
    
    def analyze(self, data_sizes: List[int], model_type: str = 'gradient_boosting') -> Dict:
        """Анализ влияния объема на точность"""
        logger.info("\n" + "="*60)
        logger.info(f"📊 Analyzing impact of data volume on model accuracy")
        logger.info(f"   Model type: {model_type}")
        logger.info("="*60)
        
        generator = ProcurementDataGenerator()
        
        for size in data_sizes:
            logger.info(f"\n🔬 Testing with {size:,} records")
            
            # Генерация данных
            df = generator.generate_data(size)
            
            # Обучение модели
            model = ProcurementMLModel(model_type=model_type)
            X, y = model.prepare_data(df)
            metrics = model.train(X, y)
            
            result = {
                'data_size': size,
                'model_type': model_type,
                **metrics
            }
            self.results.append(result)
        
        return {'results': self.results}
    
    def visualize_impact(self, output_dir: str = './data/ml_analysis'):
        """Визуализация влияния объема данных"""
        if not self.results:
            logger.warning("No results to visualize")
            return
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        df_results = pd.DataFrame(self.results)
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Impact of Data Volume on Model Accuracy', fontsize=16, fontweight='bold')
        
        # 1. Test RMSE vs Data Size
        ax1 = axes[0, 0]
        ax1.plot(df_results['data_size'] / 1_000_000, df_results['test_rmse'],
                marker='o', linewidth=2, markersize=8)
        ax1.set_xlabel('Data Size (Millions of Records)')
        ax1.set_ylabel('Test RMSE')
        ax1.set_title('Prediction Error vs Data Size')
        ax1.grid(True, alpha=0.3)
        
        # 2. R² Score vs Data Size
        ax2 = axes[0, 1]
        ax2.plot(df_results['data_size'] / 1_000_000, df_results['test_r2'],
                marker='s', linewidth=2, markersize=8, color='green')
        ax2.set_xlabel('Data Size (Millions of Records)')
        ax2.set_ylabel('R² Score')
        ax2.set_title('Model Accuracy vs Data Size')
        ax2.set_ylim([0, 1])
        ax2.grid(True, alpha=0.3)
        
        # 3. Train vs Test R²
        ax3 = axes[1, 0]
        x_pos = np.arange(len(df_results))
        ax3.bar(x_pos - 0.2, df_results['train_r2'], 0.4, label='Train R²')
        ax3.bar(x_pos + 0.2, df_results['test_r2'], 0.4, label='Test R²')
        ax3.set_xlabel('Data Size')
        ax3.set_ylabel('R² Score')
        ax3.set_title('Overfitting Analysis')
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels([f"{s/1e6:.1f}M" for s in df_results['data_size']])
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
        
        # 4. MAE vs Data Size
        ax4 = axes[1, 1]
        ax4.plot(df_results['data_size'] / 1_000_000, df_results['test_mae'],
                marker='^', linewidth=2, markersize=8, color='red')
        ax4.set_xlabel('Data Size (Millions of Records)')
        ax4.set_ylabel('Mean Absolute Error')
        ax4.set_title('Mean Prediction Error vs Data Size')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        output_path = os.path.join(output_dir, 'data_volume_impact.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"✓ Visualization saved: {output_path}")
        plt.close()
    
    def save_results(self, output_dir: str = './data/ml_analysis'):
        """Сохранение результатов анализа"""
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # JSON
        json_path = os.path.join(output_dir, 'ml_analysis_results.json')
        with open(json_path, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"✓ Results saved: {json_path}")
        
        # CSV
        csv_path = os.path.join(output_dir, 'ml_analysis_results.csv')
        pd.DataFrame(self.results).to_csv(csv_path, index=False)
        logger.info(f"✓ CSV saved: {csv_path}")


class IntegratedMLPipeline:
    """Интегрированный пайплайн ML с ETL"""
    
    def __init__(self):
        self.data_generator = ProcurementDataGenerator()
        self.models = {}
        self.impact_analyzer = DataVolumeImpactAnalyzer()
    
    def run_full_analysis(self, output_dir: str = './data/ml_analysis'):
        """Запуск полного анализа"""
        logger.info("\n" + "="*60)
        logger.info("🚀 Starting Integrated ML Pipeline")
        logger.info("="*60)
        
        # Размеры для анализа
        data_sizes = [100_000, 500_000, 1_000_000]
        
        # Анализ влияния объема
        self.impact_analyzer.analyze(data_sizes, model_type='gradient_boosting')
        self.impact_analyzer.visualize_impact(output_dir)
        self.impact_analyzer.save_results(output_dir)
        
        logger.info("\n" + "="*60)
        logger.info("✓ ML Pipeline completed")
        logger.info("="*60)
    
    def train_and_deploy_model(self, data_size: int = 1_000_000):
        """Обучение и развертывание модели"""
        logger.info("\n" + "="*60)
        logger.info("🎓 Training Production Model")
        logger.info("="*60)
        
        # Генерация данных
        df = self.data_generator.generate_data(data_size)
        
        # Обучение моделей
        model_types = ['linear', 'gradient_boosting', 'random_forest']
        
        for model_type in model_types:
            logger.info(f"\n🔧 Training {model_type} model")
            
            model = ProcurementMLModel(model_type=model_type)
            X, y = model.prepare_data(df)
            metrics = model.train(X, y)
            
            # Сохранение модели
            model_path = f'./models/procurement_model_{model_type}.pkl'
            model.save(model_path)
            
            self.models[model_type] = model


def main():
    """Главная функция"""
    logger.info("\n" + "#"*60)
    logger.info("# PROCUREMENT ML MODEL TRAINING")
    logger.info("#"*60)
    
    try:
        # Запуск интегрированного пайплайна
        pipeline = IntegratedMLPipeline()
        
        # Полный анализ влияния объема данных
        pipeline.run_full_analysis()
        
        # Обучение production модели
        pipeline.train_and_deploy_model(data_size=1_000_000)
        
        logger.info("\n" + "#"*60)
        logger.info("✓ ML TRAINING COMPLETED")
        logger.info("#"*60)
        
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
