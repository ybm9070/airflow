
    #import libs.
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum  # for local time zone +9time

#Set timezone to Asia/Seoul
local_tz = pendulum.timezone("Asia/Seoul")

#default args
default_args = {
    "owner": "han",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 4, tzinfo=local_tz),  # utc->local
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ML_Airflow_Practice",
    default_args=default_args,
    description="my airflow",
    schedule_interval=timedelta(days=1),
)



def feature_engineering(**kwargs): # **kwargs =디폴트 펑션 타입 이건 무조건 넣어야함
    pass

def train_model(model_name,**kawrgs):
    pass

def select_best_model(**kwargs):
    pass


with dag:
    t1 = PythonOperator(
        task_id = "feature_engineering",
        python_callable = feature_engineering,


    )
    t2 = PythonOperator(
        task_id = "train_rf" #아이디 반드시들어가야함
        python_callable = train_model, # 함수
        op_kwargs={"model_name":"RandomFroest"}, # 이건 그냥 반드시포함되는문장, 다른애들은 그냥넣어도된다?
        provide_context=True, # 요즘 이거 안넣어줘도 넘어간다. 이유찾아보자


    )

    t3 = PythonOperator(
        task_id = "train_gb"
        python_callable = train_model,
        op_kwargs={"model_name":"GradientBoosting"}, # 이건 그냥 반드시포함되는문장
        provide_context=True,


    )

    t4= PythonOperator(
        task_id = "select_best_model",
        python_callable = select_best_model,



    )

    t1 >> [t2,t3] >> t4