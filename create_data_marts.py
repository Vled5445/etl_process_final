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
    'create_analytics_marts',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    def create_user_activity_mart():
        """
        Витрина 1: Активность пользователей
        """
        conn = psycopg2.connect(
            host="postgres", database="etl_db",
            user="etl_user", password="etl_password"
        )
        cur = conn.cursor()
        
        # Создаём таблицу витрины
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_activity_mart (
                user_id VARCHAR(50),
                session_count INTEGER,
                total_session_time INTEGER,
                avg_pages_visited DECIMAL(10, 2),
                total_actions INTEGER,
                most_visited_page VARCHAR(100),
                primary_device VARCHAR(50)
            )
        """)
        
        # Очищаем старые данные перед вставкой
        cur.execute("TRUNCATE TABLE user_activity_mart")
        
        # Вставляем данные из user_sessions (исправлено — без ссылки на саму таблицу)
        cur.execute("""
            INSERT INTO user_activity_mart
            SELECT 
                s.user_id,
                COUNT(*) as session_count,
                SUM(EXTRACT(EPOCH FROM (s.end_time - s.start_time))/60) as total_session_time,
                AVG(ARRAY_LENGTH(s.pages_visited, 1)) as avg_pages_visited,
                SUM(ARRAY_LENGTH(s.actions, 1)) as total_actions,
                (
                    SELECT UNNEST(pages_visited) 
                    FROM user_sessions s2
                    WHERE s2.user_id = s.user_id
                    GROUP BY UNNEST(pages_visited)
                    ORDER BY COUNT(*) DESC 
                    LIMIT 1
                ) as most_visited_page,
                CASE 
                    WHEN s.device_mobile = true THEN 'mobile'
                    WHEN s.device_desktop = true THEN 'desktop'
                    WHEN s.device_tablet = true THEN 'tablet'
                    ELSE 'other'
                END as primary_device
            FROM user_sessions s
            GROUP BY s.user_id, s.device_mobile, s.device_desktop, s.device_tablet, s.device_other
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        return "Витрина активности пользователей создана"

    def create_support_efficiency_mart():
        """
        Витрина 2: Эффективность работы поддержки
        """
        conn = psycopg2.connect(
            host="postgres", database="etl_db",
            user="etl_user", password="etl_password"
        )
        cur = conn.cursor()
        
        # Создаём таблицу витрины
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_efficiency_mart (
                ticket_id VARCHAR(50),
                user_id VARCHAR(50),
                issue_type VARCHAR(50),
                status VARCHAR(50),
                resolution_time_hours DECIMAL(10, 2),
                is_open BOOLEAN,
                days_created INTEGER
            )
        """)
        
        # Очищаем старые данные перед вставкой
        cur.execute("TRUNCATE TABLE support_efficiency_mart")
        
        # Вставляем данные из support_tickets
        cur.execute("""
            INSERT INTO support_efficiency_mart
            SELECT 
                ticket_id,
                user_id,
                issue_type,
                status,
                EXTRACT(EPOCH FROM (updated_at - created_at))/3600 as resolution_time_hours,
                status IN ('open', 'pending') as is_open,
                EXTRACT(DAY FROM (NOW() - created_at)) as days_created
            FROM support_tickets
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        return "Витрина эффективности поддержки создана"

    # Создаём задачи
    task1 = PythonOperator(
        task_id='create_user_activity_mart',
        python_callable=create_user_activity_mart
    )
    
    task2 = PythonOperator(
        task_id='create_support_efficiency_mart',
        python_callable=create_support_efficiency_mart
    )
    
    # Задаем зависимости между задачами
    task1 >> task2
