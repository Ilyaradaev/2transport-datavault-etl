#!/usr/bin/env python3
"""
2TRANSPORTv2.py - ETL пайплайн для сбора, обработки и загрузки данных о ДТП
Архитектура: Data Vault 2.0 на PostgreSQL
Оркестрация: Apache Airflow
Автор: Ilyaradaev
Репозиторий: https://github.com/Ilyaradaev/2transport-datavault-etl
"""

import os
import sys
import logging
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Text, BigInteger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# ==================== Конфигурация ====================
DATA_PATH = r"C:\MyYml\data"
DB_CONFIG = {
    'user': 'etl_user',
    'password': 'secure_password',
    'database': 'etl_db',
    'host': 'localhost',
    'port': 5432
}
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Настройка логирования
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'etl_{datetime.now().strftime("%Y%m%d")}.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("2TRANSPORT_ETL")

# ==================== Функции очистки данных ====================
def clean_dataframe(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Очистка DataFrame: удаление дубликатов, замена пропусков на NaN
    """
    logger.info(f"Очистка данных: {file_name}, начальный размер: {len(df)}")
    
    # Удаление полных дубликатов строк
    initial_count = len(df)
    df = df.drop_duplicates()
    logger.info(f"Удалено дубликатов строк: {initial_count - len(df)}")
    
    # Замена пустых строк и None на NaN
    df = df.replace(['', ' ', 'NULL', 'null', 'None'], np.nan)
    
    # Для числовых колонок: преобразование, если нужно
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', np.nan)
    
    logger.info(f"Размер после очистки: {len(df)}")
    return df

# ==================== Функции хеширования для Data Vault ====================
def generate_hash_key(*args) -> str:
    """Генерация хеш-ключа для Data Vault"""
    combined = '|'.join(str(arg) for arg in args if pd.notna(arg))
    return hashlib.md5(combined.encode('utf-8')).hexdigest()

def generate_hash_diff(row: pd.Series, columns: list) -> str:
    """Генерация хеш-разницы для Satellite"""
    combined = '|'.join(str(row[col]) if pd.notna(row[col]) else '' for col in columns)
    return hashlib.md5(combined.encode('utf-8')).hexdigest()

# ==================== Extract ====================
def extract_data(**context) -> dict:
    """
    Извлечение данных из CSV файлов
    """
    logger.info("=" * 50)
    logger.info("Начало извлечения данных (EXTRACT)")
    
    files = {
        'accidents': os.path.join(DATA_PATH, 'accidents_all_regions_126_v20260501.csv'),
        'participants': os.path.join(DATA_PATH, 'participants_all_regions_126_v20260501.csv'),
        'vehicles': os.path.join(DATA_PATH, 'vehicles_all_regions_126_v20260501.csv')
    }
    
    dataframes = {}
    
    try:
        for name, file_path in files.items():
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            
            logger.info(f"Чтение файла: {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            logger.info(f"Загружено {len(df)} записей из {name}")
            
            # Очистка данных
            df = clean_dataframe(df, name)
            dataframes[name] = df
            
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {e}")
        raise
    
    # Сохраняем данные в XCom для передачи в следующий таск
    context['task_instance'].xcom_push(key='extracted_data', value=dataframes)
    logger.info("Извлечение данных завершено успешно")
    return dataframes

# ==================== Transform ====================
def transform_data(**context) -> dict:
    """
    Трансформация данных для Data Vault
    """
    logger.info("=" * 50)
    logger.info("Начало трансформации данных (TRANSFORM)")
    
    dataframes = context['task_instance'].xcom_pull(key='extracted_data', task_ids='extract')
    
    # Преобразование типов данных для accidents
    accidents_df = dataframes['accidents'].copy()
    participants_df = dataframes['participants'].copy()
    vehicles_df = dataframes['vehicles'].copy()
    
    # === Трансформация accidents ===
    logger.info("Трансформация таблицы accidents")
    
    # Преобразование datetime
    accidents_df['datetime'] = pd.to_datetime(accidents_df['datetime'], errors='coerce')
    
    # Числовые колонки
    numeric_cols = ['id', 'participants_count', 'dead_count', 'injured_count']
    for col in numeric_cols:
        accidents_df[col] = pd.to_numeric(accidents_df[col], errors='coerce')
    
    # Float колонки
    float_cols = ['longitude', 'latitude']
    for col in float_cols:
        accidents_df[col] = pd.to_numeric(accidents_df[col], errors='coerce')
    
    # === Создание HUB и Satellite записей ===
    transformed = {
        'hub_accident': [],
        'sat_accident': [],
        'hub_participant': [],
        'sat_participant': [],
        'hub_vehicle': [],
        'sat_vehicle': [],
        'link_accident_participant': [],
        'link_accident_vehicle': []
    }
    
    # HUB_ACCIDENT
    for _, row in accidents_df.iterrows():
        if pd.notna(row['id']):
            hub_key = generate_hash_key('ACCIDENT', row['id'])
            transformed['hub_accident'].append({
                'accident_hk': hub_key,
                'accident_bk': str(int(row['id'])) if pd.notna(row['id']) else None,
                'load_date': datetime.now(),
                'record_source': 'csv_accidents'
            })
            
            # SAT_ACCIDENT
            hash_diff_columns = ['tags', 'category', 'region', 'county', 'address', 
                                  'nearby', 'light', 'weather', 'road_conditions',
                                  'severity', 'datetime', 'longitude', 'latitude',
                                  'participants_count', 'participant_categories', 
                                  'dead_count', 'injured_count']
            hash_diff = generate_hash_diff(row, hash_diff_columns)
            
            transformed['sat_accident'].append({
                'accident_hk': hub_key,
                'load_date': datetime.now(),
                'record_source': 'csv_accidents',
                'hash_diff': hash_diff,
                'tags': row['tags'] if pd.notna(row['tags']) else None,
                'category': row['category'] if pd.notna(row['category']) else None,
                'region': row['region'] if pd.notna(row['region']) else None,
                'county': row['county'] if pd.notna(row['county']) else None,
                'address': row['address'] if pd.notna(row['address']) else None,
                'longitude': float(row['longitude']) if pd.notna(row['longitude']) else None,
                'latitude': float(row['latitude']) if pd.notna(row['latitude']) else None,
                'nearby': row['nearby'] if pd.notna(row['nearby']) else None,
                'datetime_acc': row['datetime'] if pd.notna(row['datetime']) else None,
                'light': row['light'] if pd.notna(row['light']) else None,
                'weather': row['weather'] if pd.notna(row['weather']) else None,
                'road_conditions': row['road_conditions'] if pd.notna(row['road_conditions']) else None,
                'participants_count': int(row['participants_count']) if pd.notna(row['participants_count']) else 0,
                'participant_categories': row['participant_categories'] if pd.notna(row['participant_categories']) else None,
                'severity': row['severity'] if pd.notna(row['severity']) else None,
                'dead_count': int(row['dead_count']) if pd.notna(row['dead_count']) else 0,
                'injured_count': int(row['injured_count']) if pd.notna(row['injured_count']) else 0
            })
    
    # HUB_PARTICIPANT
    for _, row in participants_df.iterrows():
        if pd.notna(row['participant_id']):
            hub_key = generate_hash_key('PARTICIPANT', row['participant_id'])
            transformed['hub_participant'].append({
                'participant_hk': hub_key,
                'participant_bk': str(row['participant_id']),
                'load_date': datetime.now(),
                'record_source': 'csv_participants'
            })
            
            # SAT_PARTICIPANT
            hash_diff = generate_hash_diff(row, ['role', 'gender', 'violations', 'health_status', 'years_of_driving_experience'])
            
            transformed['sat_participant'].append({
                'participant_hk': hub_key,
                'load_date': datetime.now(),
                'record_source': 'csv_participants',
                'hash_diff': hash_diff,
                'role': row['role'] if pd.notna(row['role']) else None,
                'gender': row['gender'] if pd.notna(row['gender']) else None,
                'violations': row['violations'] if pd.notna(row['violations']) else None,
                'health_status': row['health_status'] if pd.notna(row['health_status']) else None,
                'years_of_driving_experience': int(row['years_of_driving_experience']) if pd.notna(row['years_of_driving_experience']) else None
            })
            
            # LINK_ACCIDENT_PARTICIPANT
            accident_hub_key = generate_hash_key('ACCIDENT', row['accident_id'])
            transformed['link_accident_participant'].append({
                'accident_participant_hk': generate_hash_key(accident_hub_key, hub_key),
                'accident_hk': accident_hub_key,
                'participant_hk': hub_key,
                'load_date': datetime.now(),
                'record_source': 'csv_participants'
            })
    
    # HUB_VEHICLE
    for _, row in vehicles_df.iterrows():
        if pd.notna(row['vehicle_id']):
            hub_key = generate_hash_key('VEHICLE', row['vehicle_id'])
            transformed['hub_vehicle'].append({
                'vehicle_hk': hub_key,
                'vehicle_bk': str(row['vehicle_id']),
                'load_date': datetime.now(),
                'record_source': 'csv_vehicles'
            })
            
            # SAT_VEHICLE
            hash_diff = generate_hash_diff(row, ['category', 'brand', 'model', 'color', 'year'])
            
            transformed['sat_vehicle'].append({
                'vehicle_hk': hub_key,
                'load_date': datetime.now(),
                'record_source': 'csv_vehicles',
                'hash_diff': hash_diff,
                'category': row['category'] if pd.notna(row['category']) else None,
                'brand': row['brand'] if pd.notna(row['brand']) else None,
                'model': row['model'] if pd.notna(row['model']) else None,
                'color': row['color'] if pd.notna(row['color']) else None,
                'year': int(row['year']) if pd.notna(row['year']) else None
            })
            
            # LINK_ACCIDENT_VEHICLE
            accident_hub_key = generate_hash_key('ACCIDENT', row['accident_id'])
            transformed['link_accident_vehicle'].append({
                'accident_vehicle_hk': generate_hash_key(accident_hub_key, hub_key),
                'accident_hk': accident_hub_key,
                'vehicle_hk': hub_key,
                'load_date': datetime.now(),
                'record_source': 'csv_vehicles'
            })
    
    logger.info(f"Трансформация завершена. Статистика:")
    for key, value in transformed.items():
        logger.info(f"  {key}: {len(value)} записей")
    
    context['task_instance'].xcom_push(key='transformed_data', value=transformed)
    return transformed

# ==================== Load (Data Vault) ====================
def create_datavault_schema(engine):
    """Создание схемы Data Vault в PostgreSQL"""
    
    drop_queries = [
        "DROP TABLE IF EXISTS dm_accident_by_region CASCADE;",
        "DROP TABLE IF EXISTS sat_vehicle CASCADE;",
        "DROP TABLE IF EXISTS sat_participant CASCADE;",
        "DROP TABLE IF EXISTS sat_accident CASCADE;",
        "DROP TABLE IF EXISTS link_accident_vehicle CASCADE;",
        "DROP TABLE IF EXISTS link_accident_participant CASCADE;",
        "DROP TABLE IF EXISTS hub_vehicle CASCADE;",
        "DROP TABLE IF EXISTS hub_participant CASCADE;",
        "DROP TABLE IF EXISTS hub_accident CASCADE;"
    ]
    
    create_queries = [
        # Hubs
        """
        CREATE TABLE IF NOT EXISTS hub_accident (
            accident_hk VARCHAR(32) PRIMARY KEY,
            accident_bk VARCHAR(255) NOT NULL,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS hub_participant (
            participant_hk VARCHAR(32) PRIMARY KEY,
            participant_bk VARCHAR(255) NOT NULL,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS hub_vehicle (
            vehicle_hk VARCHAR(32) PRIMARY KEY,
            vehicle_bk VARCHAR(255) NOT NULL,
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100)
        );
        """,
        # Links
        """
        CREATE TABLE IF NOT EXISTS link_accident_participant (
            accident_participant_hk VARCHAR(32) PRIMARY KEY,
            accident_hk VARCHAR(32) REFERENCES hub_accident(accident_hk),
            participant_hk VARCHAR(32) REFERENCES hub_participant(participant_hk),
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS link_accident_vehicle (
            accident_vehicle_hk VARCHAR(32) PRIMARY KEY,
            accident_hk VARCHAR(32) REFERENCES hub_accident(accident_hk),
            vehicle_hk VARCHAR(32) REFERENCES hub_vehicle(vehicle_hk),
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100)
        );
        """,
        # Satellites
        """
        CREATE TABLE IF NOT EXISTS sat_accident (
            accident_hk VARCHAR(32) REFERENCES hub_accident(accident_hk),
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100),
            hash_diff VARCHAR(32),
            tags TEXT,
            category VARCHAR(255),
            region VARCHAR(255),
            county VARCHAR(255),
            address TEXT,
            longitude FLOAT,
            latitude FLOAT,
            nearby TEXT,
            datetime_acc TIMESTAMP,
            light VARCHAR(255),
            weather VARCHAR(255),
            road_conditions VARCHAR(255),
            participants_count INTEGER,
            participant_categories VARCHAR(255),
            severity VARCHAR(100),
            dead_count INTEGER,
            injured_count INTEGER,
            PRIMARY KEY (accident_hk, load_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sat_participant (
            participant_hk VARCHAR(32) REFERENCES hub_participant(participant_hk),
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100),
            hash_diff VARCHAR(32),
            role VARCHAR(100),
            gender VARCHAR(50),
            violations TEXT,
            health_status VARCHAR(100),
            years_of_driving_experience INTEGER,
            PRIMARY KEY (participant_hk, load_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sat_vehicle (
            vehicle_hk VARCHAR(32) REFERENCES hub_vehicle(vehicle_hk),
            load_date TIMESTAMP NOT NULL,
            record_source VARCHAR(100),
            hash_diff VARCHAR(32),
            category VARCHAR(100),
            brand VARCHAR(255),
            model VARCHAR(255),
            color VARCHAR(50),
            year INTEGER,
            PRIMARY KEY (vehicle_hk, load_date)
        );
        """
    ]
    
    with engine.connect() as conn:
        # Удаление старых таблиц
        for query in drop_queries:
            conn.execute(text(query))
        
        # Создание новых таблиц
        for query in create_queries:
            conn.execute(text(query))
        
        conn.commit()
    
    logger.info("Схема Data Vault успешно создана")

def load_data(**context):
    """
    Загрузка данных в Data Vault
    """
    logger.info("=" * 50)
    logger.info("Начало загрузки данных (LOAD)")
    
    transformed = context['task_instance'].xcom_pull(key='transformed_data', task_ids='transform')
    
    engine = create_engine(DATABASE_URL)
    
    try:
        # Создание схемы
        create_datavault_schema(engine)
        
        with engine.begin() as conn:
            # Загрузка HUB таблиц
            if transformed['hub_accident']:
                pd.DataFrame(transformed['hub_accident']).to_sql('hub_accident', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['hub_accident'])} записей в hub_accident")
            
            if transformed['hub_participant']:
                pd.DataFrame(transformed['hub_participant']).to_sql('hub_participant', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['hub_participant'])} записей в hub_participant")
            
            if transformed['hub_vehicle']:
                pd.DataFrame(transformed['hub_vehicle']).to_sql('hub_vehicle', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['hub_vehicle'])} записей в hub_vehicle")
            
            # Загрузка LINK таблиц
            if transformed['link_accident_participant']:
                pd.DataFrame(transformed['link_accident_participant']).to_sql('link_accident_participant', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['link_accident_participant'])} записей в link_accident_participant")
            
            if transformed['link_accident_vehicle']:
                pd.DataFrame(transformed['link_accident_vehicle']).to_sql('link_accident_vehicle', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['link_accident_vehicle'])} записей в link_accident_vehicle")
            
            # Загрузка SATELLITE таблиц
            if transformed['sat_accident']:
                pd.DataFrame(transformed['sat_accident']).to_sql('sat_accident', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['sat_accident'])} записей в sat_accident")
            
            if transformed['sat_participant']:
                pd.DataFrame(transformed['sat_participant']).to_sql('sat_participant', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['sat_participant'])} записей в sat_participant")
            
            if transformed['sat_vehicle']:
                pd.DataFrame(transformed['sat_vehicle']).to_sql('sat_vehicle', conn, if_exists='append', index=False)
                logger.info(f"Загружено {len(transformed['sat_vehicle'])} записей в sat_vehicle")
        
        logger.info("Загрузка данных в Data Vault завершена успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

# ==================== Aggregate (Data Mart) ====================
def create_aggregate_mart(**context):
    """
    Создание витрины данных: агрегация по регионам
    """
    logger.info("=" * 50)
    logger.info("Создание витрины данных (Data Mart)")
    
    engine = create_engine(DATABASE_URL)
    
    # Запрос для создания витрины с агрегацией по регионам
    aggregate_query = """
    DROP TABLE IF EXISTS dm_accident_by_region;
    
    CREATE TABLE dm_accident_by_region AS
    SELECT 
        COALESCE(sa.region, 'Unknown') AS region,
        COUNT(DISTINCT ha.accident_hk) AS total_accidents,
        SUM(COALESCE(sa.dead_count, 0)) AS total_dead,
        SUM(COALESCE(sa.injured_count, 0)) AS total_injured,
        AVG(COALESCE(sa.participants_count, 0)) AS avg_participants_per_accident,
        COUNT(DISTINCT hp.participant_hk) AS total_participants,
        COUNT(DISTINCT hv.vehicle_hk) AS total_vehicles,
        MIN(sa.datetime_acc) AS first_accident_date,
        MAX(sa.datetime_acc) AS last_accident_date,
        CURRENT_TIMESTAMP AS mart_updated_at
    FROM hub_accident ha
    LEFT JOIN (
        SELECT DISTINCT ON (accident_hk) accident_hk, region, dead_count, 
               injured_count, participants_count, datetime_acc
        FROM sat_accident 
        ORDER BY accident_hk, load_date DESC
    ) sa ON ha.accident_hk = sa.accident_hk
    LEFT JOIN link_accident_participant lap ON ha.accident_hk = lap.accident_hk
    LEFT JOIN hub_participant hp ON lap.participant_hk = hp.participant_hk
    LEFT JOIN link_accident_vehicle lav ON ha.accident_hk = lav.accident_hk
    LEFT JOIN hub_vehicle hv ON lav.vehicle_hk = hv.vehicle_hk
    GROUP BY sa.region
    ORDER BY total_accidents DESC;
    
    -- Добавление комментариев
    COMMENT ON TABLE dm_accident_by_region IS 'Витрина данных: агрегация ДТП по регионам';
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(aggregate_query))
        
        # Проверка результата
        result_df = pd.read_sql("SELECT * FROM dm_accident_by_region LIMIT 10", engine)
        logger.info("Витрина данных dm_accident_by_region создана успешно")
        logger.info(f"Пример данных:\n{result_df.to_string()}")
        
        # Сохранение статистики для мониторинга
        stats_df = pd.read_sql("""
            SELECT COUNT(*) as total_regions, SUM(total_accidents) as grand_total_accidents
            FROM dm_accident_by_region
        """, engine)
        logger.info(f"Итоговая статистика: {stats_df.to_dict('records')[0]}")
        
    except Exception as e:
        logger.error(f"Ошибка при создании витрины данных: {e}")
        raise

# ==================== DAG определения для Airflow ====================
default_args = {
    'owner': 'Ilyaradaev',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 5,
    'catchup': False
}

dag = DAG(
    'etl_2transport_datavault',
    default_args=default_args,
    description='ETL пайплайн для загрузки данных о ДТП в Data Vault',
    schedule_interval='@daily',
    tags=['etl', 'datavault', 'transport']
)

# Определение задач
start_task = DummyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

aggregate_task = PythonOperator(
    task_id='aggregate_mart',
    python_callable=create_aggregate_mart,
    dag=dag
)

end_task = DummyOperator(task_id='end', dag=dag)

# Настройка зависимостей
start_task >> extract_task >> transform_task >> load_task >> aggregate_task >> end_task

# ==================== Точка входа для запуска вне Airflow ====================
if __name__ == "__main__":
    logger.info("Запуск ETL пайплайна в ручном режиме")
    try:
        # Имитация контекста для ручного запуска
        class MockContext:
            def __init__(self):
                self.xcom_data = {}
            def xcom_push(self, key, value):
                self.xcom_data[key] = value
            def xcom_pull(self, key, task_ids=None):
                return self.xcom_data.get(key)
        
        context = {'task_instance': MockContext()}
        
        # Выполнение ETL
        extract_data(**context)
        transform_data(**context)
        load_data(**context)
        create_aggregate_mart(**context)
        
        logger.info("ETL пайплайн выполнен успешно!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
