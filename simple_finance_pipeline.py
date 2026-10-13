from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_finance_data():
    """Генерация тестовых финансовых данных"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    data = {
        'date': dates.values.tolist(),
        'price': np.random.normal(100, 15, 100).cumsum().tolist(),
        'volume': np.random.randint(1000, 10000, 100).tolist()
    }
    df = pd.DataFrame(data)
    print(f"Сгенерировано {len(df)} записей")
    return data

def calculate_metrics(**kwargs):
    """Расчет финансовых метрик"""
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='generate_data')
    
    df['daily_return'] = np.asarray(df['price']).pct_change()
    df['volatility'] = np.asarray(df['daily_return']).rolling(7).std()
    df['moving_avg'] = np.asarray(df['price']).rolling(7).mean()
    
    metrics = {
        'total_days': len(df),
        'avg_price': df['price'].mean(),
        'max_price': df['price'].max(),
        'min_price': df['price'].min(),
        'avg_volume': df['volume'].mean()
    }
    
    print("Рассчитанные метрики:", metrics)
    return metrics

def detect_anomalies(**kwargs):
    """Обнаружение аномалий в данных"""
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='calculate_metrics')
    df = ti.xcom_pull(task_ids='generate_data')
    
    # Простая логика обнаружения аномалий
    price_std = df['price'].std()
    price_mean = df['price'].mean()
    
    anomalies = df[abs(df['price'] - price_mean) > 2 * price_std]
    
    print(f"Обнаружено {len(anomalies)} аномалий")
    return len(anomalies)

def generate_report(**kwargs):
    """Генерация финального отчета"""
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='calculate_metrics')
    anomalies_count = ti.xcom_pull(task_ids='detect_anomalies')
    
    report = {
        'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'summary': f"Проанализировано {metrics['total_days']} дней",
        'price_range': f"{metrics['min_price']:.2f} - {metrics['max_price']:.2f}",
        'anomalies_detected': anomalies_count,
        'status': 'COMPLETED'
    }
    
    print("ФИНАЛЬНЫЙ ОТЧЕТ:")
    for key, value in report.items():
        print(f"  {key}: {value}")
    
    return report

# Настройки DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создание DAG
with DAG(
    'simple_finance_pipeline',
    default_args=default_args,
    description='Простой финансовый пайплайн без SQL',
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['finance', 'simple']
) as dag:

    # Задача 1: Генерация данных
    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_finance_data
    )

    # Задача 2: Расчет метрик
    calculate_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics
    )

    # Задача 3: Поиск аномалий
    anomalies_task = PythonOperator(
        task_id='detect_anomalies',
        python_callable=detect_anomalies
    )

    # Задача 4: Генерация отчета
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    # Определение порядка выполнения
    generate_task >> calculate_task >> anomalies_task >> report_task