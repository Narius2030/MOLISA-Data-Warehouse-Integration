from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def start():
    print('Starting to Integrate Data Warehouse...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))
    
with DAG(
    'Integrate_Data_Warehouse',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    print_start_task = PythonOperator(
        task_id='print_start',
        python_callable=start,
        dag=dag
    )
    
    refresh = SQLExecuteQueryOperator(
        task_id='refresh_stages',
        conn_id = 'postgres_ldtbxh_stage',
        sql="./sql/refresh.stage.sql",
        dag=dag
    )
    
    bronze_hongheo = SQLExecuteQueryOperator(
        task_id='bronze_hongheo',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('hongheo_data_dir')+"/load_hongheo.stage.sql",
        dag=dag
    )
    
    bronze_atvsld = SQLExecuteQueryOperator(
        task_id='bronze_atvsld',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('atvsld_data_dir')+"/load_atvsld.stage.sql",
        dag=dag
    )
    
    print_end_task = PythonOperator(
        task_id='print_end',
        python_callable=end,
        dag=dag
    )
    
print_start_task >> refresh >> [bronze_hongheo, bronze_atvsld] >> print_end_task