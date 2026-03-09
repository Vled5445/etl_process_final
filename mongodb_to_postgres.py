from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pymongo
import psycopg2
import pandas as pd
import os
import json

# Настройка аргументов по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Определение DAG
with DAG(
    'mongodb_to_postgres',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # Вспомогательная функция для конвертации numpy типов в Python native
    def convert_to_native(obj):
        """Конвертация numpy типов в нативные Python типы"""
        if hasattr(obj, 'item'):  # numpy scalar
            if obj.dtype in ['bool_']:
                return bool(obj)
            elif obj.dtype in ['int64', 'int32']:
                return int(obj)
            elif obj.dtype in ['float64', 'float32']:
                return float(obj)
            elif obj.dtype.name.startswith('datetime'):
                return obj.item()
            else:
                return obj.item()
        elif isinstance(obj, list):
            return [convert_to_native(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: convert_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        else:
            # Если тип не распознан, преобразуем в строку или None
            return str(obj) if obj is not None else None

    def extract_from_mongodb(**context):
        """Экстракция данных из MongoDB"""
        client = pymongo.MongoClient("mongodb://admin:admin@mongodb:27017/")
        db = client["etl_data"]
        
        collections = ["UserSessions", "EventLogs", "SupportTickets", 
                      "UserRecommendations", "ModerationQueue"]
        
        data = {}
        for coll in collections:
            if coll in db.list_collection_names():
                data[coll] = list(db[coll].find())
        
        # Создание папки для данных если её нет
        data_dir = '/opt/airflow/data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, mode=0o777)
        
        # Запись данных в JSON
        json_path = '/opt/airflow/data/mongodb_data.json'
        json_data = {}
        for key, value in data.items():
            for item in value:
                if '_id' in item:
                    item['_id'] = str(item['_id'])
            json_data[key] = value
        
        with open(json_path, 'w') as f:
            json.dump(json_data, f, default=str)
        
        return f"Данные из MongoDB успешно сохранены в {json_path}"
    
    def create_tables_if_not_exists():
        """Создание таблиц в PostgreSQL если их нет"""
        conn = psycopg2.connect(
            host="postgres", database="etl_db",
            user="etl_user", password="etl_password"
        )
        cur = conn.cursor()
        
        # Создаём таблицы если их нет
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                start_time TIMESTAMP WITH TIME ZONE,
                end_time TIMESTAMP WITH TIME ZONE,
                pages_visited TEXT[],
                device_mobile BOOLEAN,
                device_desktop BOOLEAN,
                device_tablet BOOLEAN,
                device_other BOOLEAN,
                actions TEXT[]
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_logs (
                event_id VARCHAR(50) PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                event_type VARCHAR(50),
                details JSONB
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                ticket_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                status VARCHAR(50),
                issue_type VARCHAR(50),
                messages JSONB,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_recommendations (
                user_id VARCHAR(50) PRIMARY KEY,
                recommended_products JSONB,
                last_updated TIMESTAMP WITH TIME ZONE
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS moderation_queue (
                review_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                product_id VARCHAR(50),
                review_text TEXT,
                rating INTEGER,
                moderation_status VARCHAR(50),
                flags JSONB,
                submitted_at TIMESTAMP WITH TIME ZONE
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
    
    def load_to_postgres(**context):
        """Загрузка данных в PostgreSQL"""
        conn = psycopg2.connect(
            host="postgres", database="etl_db",
            user="etl_user", password="etl_password"
        )
        cur = conn.cursor()
        
        # Чтение JSON файла
        json_path = '/opt/airflow/data/mongodb_data.json'
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Файл {json_path} не найден")
            
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Конвертация DataFrame для обработки numpy типов
        def convert_dataframe(df):
            for col in df.columns:
                df[col] = df[col].apply(lambda x: convert_to_native(x))
            return df
        
        def format_array_for_postgres(arr):
            """Форматирует список для вставки в поле массива PostgreSQL"""
            if arr is None:
                return None
            if isinstance(arr, list):
                if len(arr) == 0:
                    return '{}'
                # Экранируем кавычки и формируем правильный массив PostgreSQL
                formatted_items = []
                for item in arr:
                    if item is None:
                        formatted_items.append('NULL')
                    else:
                        # Экранируем двойные кавычки и обратную косую черту
                        item_str = str(item).replace('"', '\\"').replace('\\', '\\\\')
                        formatted_items.append(f'"{item_str}"')
                return '{' + ','.join(formatted_items) + '}'
            return arr
        
        def format_as_json(value):
            """Форматирует значение как JSON для PostgreSQL"""
            if value is None:
                return None
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False, default=str)
            elif isinstance(value, (int, float, bool)):
                return json.dumps(value)
            elif isinstance(value, str):
                # Проверяем, является ли строка валидным JSON объектом или массивом
                value_stripped = value.strip()
                if (value_stripped.startswith('{') and value_stripped.endswith('}')) or \
                   (value_stripped.startswith('[') and value_stripped.endswith(']')):
                    try:
                        # Пробуем распарсить как JSON
                        json.loads(value)
                        return value  # Строка уже является валидным JSON
                    except json.JSONDecodeError:
                        # Если не получилось, обрабатываем как обычную строку
                        return json.dumps(value)
                else:
                    # Обычная строка - оборачиваем в кавычки
                    return json.dumps(value)
            else:
                # Для других типов конвертируем в JSON
                return json.dumps(str(value))
        
        # Обработка каждой коллекции
        for collection in data:
            if not data[collection]:
                continue
                
            print(f"Обработка коллекции: {collection}")
            df = pd.json_normalize(data[collection])
            
            # Если есть колонка device, расширяем её в булевы флаги
            if 'device' in df.columns:
                df['device_mobile'] = df['device'].apply(lambda x: x == 'mobile')
                df['device_desktop'] = df['device'].apply(lambda x: x == 'desktop')
                df['device_tablet'] = df['device'].apply(lambda x: x == 'tablet')
                df['device_other'] = df['device'].apply(lambda x: x == 'other')
                df = df.drop('device', axis=1)
            
            # Конвертируем типы данных
            df = convert_dataframe(df)
            
            if collection == "UserSessions":
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    for key in row_dict:
                        row_dict[key] = convert_to_native(row_dict[key])
                    
                    # Форматируем массивы для PostgreSQL
                    pages_visited = format_array_for_postgres(row_dict.get('pages_visited'))
                    actions = format_array_for_postgres(row_dict.get('actions'))
                    
                    cur.execute("""
                        INSERT INTO user_sessions 
                        (session_id, user_id, start_time, end_time, pages_visited, 
                         device_mobile, device_desktop, device_tablet, device_other, actions)
                        VALUES (%s, %s, %s, %s, %s::text[], %s, %s, %s, %s, %s::text[])
                        ON CONFLICT (session_id) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            end_time = EXCLUDED.end_time,
                            pages_visited = EXCLUDED.pages_visited,
                            actions = EXCLUDED.actions
                    """, (
                        row_dict.get('session_id'), 
                        row_dict.get('user_id'), 
                        row_dict.get('start_time'), 
                        row_dict.get('end_time'),
                        pages_visited,
                        row_dict.get('device_mobile'), 
                        row_dict.get('device_desktop'),
                        row_dict.get('device_tablet'), 
                        row_dict.get('device_other'),
                        actions
                    ))
            
            elif collection == "EventLogs":
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    for key in row_dict:
                        row_dict[key] = convert_to_native(row_dict[key])
                    
                    # Форматируем details как JSON
                    details = row_dict.get('details')
                    details_json = format_as_json(details)
                    
                    cur.execute("""
                        INSERT INTO event_logs 
                        (event_id, timestamp, event_type, details)
                        VALUES (%s, %s, %s, %s::jsonb)
                        ON CONFLICT (event_id) DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            details = EXCLUDED.details
                    """, (
                        row_dict.get('event_id'), 
                        row_dict.get('timestamp'),
                        row_dict.get('event_type'), 
                        details_json
                    ))
            
            elif collection == "SupportTickets":
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    for key in row_dict:
                        row_dict[key] = convert_to_native(row_dict[key])
                    
                    # Форматируем messages как JSON
                    messages = row_dict.get('messages')
                    messages_json = format_as_json(messages)
                    
                    cur.execute("""
                        INSERT INTO support_tickets 
                        (ticket_id, user_id, status, issue_type, messages, 
                         created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                        ON CONFLICT (ticket_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            messages = EXCLUDED.messages,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        row_dict.get('ticket_id'), 
                        row_dict.get('user_id'),
                        row_dict.get('status'), 
                        row_dict.get('issue_type'),
                        messages_json, 
                        row_dict.get('created_at'),
                        row_dict.get('updated_at')
                    ))
            
            elif collection == "UserRecommendations":
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    for key in row_dict:
                        row_dict[key] = convert_to_native(row_dict[key])
                    
                    # Форматируем recommended_products как JSON
                    recommended_products = row_dict.get('recommended_products')
                    recommended_json = format_as_json(recommended_products)
                    
                    cur.execute("""
                        INSERT INTO user_recommendations 
                        (user_id, recommended_products, last_updated)
                        VALUES (%s, %s::jsonb, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            recommended_products = EXCLUDED.recommended_products,
                            last_updated = EXCLUDED.last_updated
                    """, (
                        row_dict.get('user_id'), 
                        recommended_json,
                        row_dict.get('last_updated')
                    ))
            
            elif collection == "ModerationQueue":
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    for key in row_dict:
                        row_dict[key] = convert_to_native(row_dict[key])
                    
                    # Форматируем flags как JSON
                    flags = row_dict.get('flags')
                    flags_json = format_as_json(flags)
                    
                    # Обработка rating
                    rating = row_dict.get('rating')
                    if rating is not None:
                        try:
                            rating = int(rating)
                        except (ValueError, TypeError):
                            rating = 0
                    else:
                        rating = 0
                    
                    cur.execute("""
                        INSERT INTO moderation_queue 
                        (review_id, user_id, product_id, review_text, rating, 
                         moderation_status, flags, submitted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                        ON CONFLICT (review_id) DO UPDATE SET
                            moderation_status = EXCLUDED.moderation_status,
                            flags = EXCLUDED.flags
                    """, (
                        row_dict.get('review_id'), 
                        row_dict.get('user_id'),
                        row_dict.get('product_id'), 
                        str(row_dict.get('review_text')) if row_dict.get('review_text') else None,
                        rating,
                        row_dict.get('moderation_status'), 
                        flags_json,
                        row_dict.get('submitted_at')
                    ))
        
        conn.commit()
        cur.close()
        conn.close()
        return f"Данные успешно загружены в PostgreSQL"
    
    # Создание таблиц перед загрузкой
    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables_if_not_exists
    )
    
    # Экстракция из MongoDB
    extract_mongodb = PythonOperator(
        task_id='extract_mongodb',
        python_callable=extract_from_mongodb
    )
    
    # Загрузка в PostgreSQL
    load_postgres = PythonOperator(
        task_id='load_postgres',
        python_callable=load_to_postgres
    )
    
    # Задаем порядок выполнения задач
    create_tables >> extract_mongodb >> load_postgres