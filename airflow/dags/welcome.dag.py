from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.utils.dates import days_ago
from datetime import datetime


def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))



dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

# spark_task = PythonOperator(
#     task_id='word_count',
#     python_callable=spark_wordcount,
#     dag=dag
# )

# connect_postgres = PythonOperator(
#     task_id='connect_postgres',
#     python_callable=spark_postgres,
#     dag=dag
# )

query_postgres = SQLExecuteQueryOperator(
    task_id='query_postgres_database',
    conn_id = 'postgres_default',
    sql='./sql/connect-other-db.sql',
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)


# Set the dependencies between the tasks

print_welcome_task >> query_postgres >> print_date_task