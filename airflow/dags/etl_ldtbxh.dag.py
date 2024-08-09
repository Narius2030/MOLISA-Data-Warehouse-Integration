from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from gold.hongheo_gold import run_gold_hongheo
from gold.atvsld_gold import run_gold_atvsld


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
    # START
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
    
    # Bronze Stage
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
    
    # Gold Stage
    gold_hongheo = PythonOperator(
        task_id='gold_hongheo',
        python_callable=run_gold_hongheo,
        dag=dag
    )
    
    gold_atvsld = PythonOperator(
        task_id='gold_atvsld',
        python_callable=run_gold_atvsld,
        dag=dag
    )
    
    #END
    print_end_task = PythonOperator(
        task_id='print_end',
        python_callable=end,
        dag=dag
    )
    
    print_start_task >> refresh 
    refresh >> bronze_hongheo >> gold_hongheo >> print_end_task
    refresh >> bronze_atvsld >> gold_atvsld >> print_end_task