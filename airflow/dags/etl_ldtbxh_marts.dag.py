from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from medallion.datamart.gold_nd_mart import run_gold_nd

def start():
    print('Starting to Integrate Data Warehouse...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))
    
    
with DAG(
    'Integrate_Data_Mart',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    # START
    print_start_task = PythonOperator(
        task_id='print_start',
        python_callable=start,
        dag=dag
    )
    
    bronze_nd_mart = SQLExecuteQueryOperator(
        task_id='bronze_nd_mart',
        conn_id = 'postgres_ngdan_datamart',
        sql=Variable.get('nd_datamart_dir')+"/load_ngdan.mart.sql",
        dag=dag
    )
    
    # Gold Stage
    gold_ngdan_mart = PythonOperator(
        task_id='gold_ngdan_mart',
        python_callable=run_gold_nd,
        dag=dag
    )
    
    # END
    print_end_task = PythonOperator(
        task_id='print_end',
        python_callable=end,
        dag=dag
    )
    
    print_start_task >> bronze_nd_mart >> gold_ngdan_mart >> print_end_task