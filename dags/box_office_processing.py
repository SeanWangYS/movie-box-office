from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from hooks.elastic.elastic_hook import ElasticHook

from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'Sean',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 10, 7, 0), 
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def _create_index():
    index = 'movie_box_office'
    body = {
        "mappings":{
            "properties": {
                "country": {"type": "keyword"},          # 國別
                "name": {"type": "keyword"},             # 中文片名
                "releaseDate": {"type": "date"},         # 上映日期
                "issue": {"type": "keyword"},            # 申請人
                "produce": {"type": "keyword"},          # 出品
                "theaterCount": {"type": "integer"},     # 上映院數
                "tickets": {"type": "integer"},          # 銷售票數
                "ticketChangeRate": {"type": "float"},   # 週票數變動率
                "amounts": {"type": "float"},            # 銷售金額
                "totalTickets": {"type": "integer"},     # 累計銷售票數
                "totalAmounts": {"type": "float"},       # 累計銷售金額
                "date": {"type": "date"},                # 資料更新日期
            }
        }
    }
    hook = ElasticHook()
    if not hook.index_exists(index):
        return hook.create_index(index=index, body=body)

def _fetch_data(ti):
    the_date = datetime.strftime(ti.execution_date, '%Y/%m/%d')
    url = f"https://boxoffice.tfi.org.tw/api/export?start={the_date}&end={the_date}"
    r = requests.get(url)
    return r.text

def _decide_next_step(ti):
    metadata = ti.xcom_pull(task_ids='fetch_data')
    context = json.loads(metadata)
    box_office = context['list']
    if box_office:
        return 'process_data' # return task_id
    else:
        return 'end_mission'

def _end_mission(ti):
    the_date = datetime.strftime(ti.execution_date, '%Y/%m/%d')
    print(f"office box hasn't been update: {the_date}")
    
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

def _store_data(ti):
    actions = ti.xcom_pull(task_ids='process_data')
    hook = ElasticHook()
    return hook.add_docs(actions)

with DAG('box_office_processing',
            default_args=default_args,
            schedule_interval='0 7 * * *',
            catchup=True) as dag:
    
    create_index = PythonOperator(
        task_id='create_index',
        python_callable=_create_index,
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='boxoffice_api',
        endpoint='',
        poke_interval=5, # 5 seconds
    )

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=_fetch_data,
        retries=2,
        retry_delay=timedelta(seconds=1),
    )

    decide_next_step = BranchPythonOperator(
        task_id='decide_next_step',
        python_callable=_decide_next_step,
    )

    end_mission = PythonOperator(
        task_id='end_mission',
        python_callable=_end_mission,
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=_process_data,
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=_store_data,
    )

    create_index >> is_api_available >> fetch_data >> decide_next_step >> [process_data, end_mission]
    process_data >> store_data