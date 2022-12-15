from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'Sean', 
    'start_date': datetime(2022, 12, 10), 
    'schedule_interval': '@daily'
}
    
def _check_data_status(ti):
    the_date = datetime.strftime(ti.execution_date, '%Y/%m/%d')
    url = f"https://boxoffice.tfi.org.tw/api/export?start={the_date}&end={the_date}"
    r = requests.get(url)
    if r.status_code==200:
        return 'download_data'
    else:
        return 'end mission'

def _download_data(ti):
    pass

def _end_mission(ti):
    pass
    
def _process_data():pass

def _store_data():pass

with DAG('box_office_processing', catchup=True, default_args=default_args) as dag:
    # check_box_office info
    # accessable or not, decide what to do (branch Operator)
        # end dag
        # fetch data
    # load to elasticsearch
    


    check_data_status = BranchPythonOperator(
        task_id='check_data_status',
        python_callable=_check_data_status
    )

