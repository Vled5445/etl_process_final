from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'transform_etl_dat',
    default_args=default_args,
    schedule_interval=None,  # Запуск вручную
    catchup=False
) as dag:

    def clean_transform_data():
        conn = psycopg2.connect(
            host="postgres",
            database="etl_db",
            user="etl_user",
            password="etl_password"
        )
        cur = conn.cursor()
        
        try:
            # 1. Удаляем дубликаты в user_sessions (оставляем последнюю запись)
            cur.execute("""
                DELETE FROM user_sessions a
                USING user_sessions b
                WHERE a.session_id = b.session_id
                AND a.start_time < b.start_time
                AND a.session_id IS NOT NULL;
            """)
            
            # 2. Заполняем пустые значения
            cur.execute("""
                UPDATE user_sessions 
                SET start_time = end_time - INTERVAL '1 hour'
                WHERE start_time IS NULL AND end_time IS NOT NULL;
            """)
            
            # ✅ Исправление: Используем ARRAY[]::text[] для пустого массива
            cur.execute("""
                UPDATE user_sessions 
                SET pages_visited = ARRAY[]::text[]
                WHERE pages_visited IS NULL;
            """)
            
            # ✅ Исправление: Используем ARRAY[]::text[] для пустого массива
            cur.execute("""
                UPDATE user_sessions 
                SET actions = ARRAY[]::text[]
                WHERE actions IS NULL;
            """)
            
            # 3. Удаляем дубликаты в user_recommendations (оставляем с последней обновленной датой)
            cur.execute("""
                DELETE FROM user_recommendations a
                USING user_recommendations b
                WHERE a.user_id = b.user_id
                AND a.last_updated < b.last_updated;
            """)
            
            # 4. Создаем таблицу для логов трансформации
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transform_log (
                    log_id SERIAL PRIMARY KEY,
                    table_name VARCHAR(50),
                    rows_processed INTEGER,
                    transformation_status VARCHAR(50),
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 5. Фиксируем выполнение трансформации
            cur.execute("""
                INSERT INTO transform_log (table_name, rows_processed, transformation_status)
                SELECT 'user_sessions', COUNT(*), 'CLEANED' FROM user_sessions
                UNION ALL
                SELECT 'user_recommendations', COUNT(*), 'CLEANED' FROM user_recommendations;
            """)
            
            conn.commit()
            print("Трансформация выполнена успешно!")
            return "Трансформация выполнена успешно!"
            
        except Exception as e:
            conn.rollback()
            print(f"Ошибка трансформации: {e}")
            raise e
            
        finally:
            cur.close()
            conn.close()

    transform_task = PythonOperator(
        task_id='clean_transform_data',
        python_callable=clean_transform_data
    )

    transform_task
