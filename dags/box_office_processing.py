from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime
import requests
import json

default_args = {
    'owner': 'Sean', 
    'start_date': datetime(2022, 12, 10), 
    'schedule_interval': '@daily'
}
    
def _check_data_info(ti):
    the_date = datetime.strftime(ti.execution_date, '%Y/%m/%d')
    url = f"https://boxoffice.tfi.org.tw/api/export?start={the_date}&end={the_date}"
    r = requests.get(url)
    if r.status_code == 200:
        return 'fetch_data'
    else:
        return 'end_mission'
    
def _decide_next_step(ti):
    metadata = ti.xcom_pull(task_ids='check_data_info')
    if metadata == 'fetch_data':
        return 'fetch_data' # return task_id
    else:
        return 'end_mission'
    
def _fetch_data(ti):
    the_date = datetime.strftime(ti.execution_date, '%Y/%m/%d')
    url = f"https://boxoffice.tfi.org.tw/api/export?start={the_date}&end={the_date}"
    r = requests.get(url)
    return r.text
    
def _end_mission():
    # to-do: send notification at this step
    print('fail to fetch data')
    
def _process_data(ti):
    metadata = ti.xcom_pull(task_ids='fetch_data')
    context = json.loads(metadata)
    updateDate = context['end']
    actions = []
    for data in context['list']:
        data["date"] = updateDate
        actions.append({
            "_op_type": "index", 
            "_index": "movie_box_office",
            "_source": data
        })
    return actions

def _store_data():pass

with DAG('box_office_processing', catchup=False, default_args=default_args) as dag:
    
    check_data_info = PythonOperator(
        task_id='check_data_info',
        python_callable=_check_data_info,
    )

    decide_next_step = BranchPythonOperator(
        task_id='decide_next_step',
        python_callable=_decide_next_step,
    )

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=_fetch_data,
    )

    end_mission = PythonOperator(
        task_id='end_mission',
        python_callable=_end_mission,
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=_process_data,
    )

    # store_data = 

    # send_notification = 

    check_data_info >> decide_next_step >> [fetch_data, end_mission]
    fetch_data >> process_data