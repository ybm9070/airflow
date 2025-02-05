
    #import libs.
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum  # for local time zone +9time
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score
import pandas as pd


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
    iris = load_iris() # 원래는 디스크나 데이터베이스에서 가져와야함
    x = pd.DataFrame(iris["data"], columns = iris['feature_names'])
    y = pd.Series(iris["target"])

    x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=47)

    # 이 데이터를 넘겨줘야함 -> xcom 을 활용하여 데이터 저장
    ti = kwargs["ti"] #ti 는 테스크 인스턴스
    ti.xcom_push(key="x_train", value=x_train.to_json(orient='columns')) 
    # to_joson 은 컬럼의 값을 일렬로 쭉 늘어세운다, 컬럼단위로 일렬로쪼개서 달라
    ti.xcom_push(key="x_test", value=x_test.to_json(orient='columns')) # 컬럼 단위로 기본 적용이라 굳이 안써도됨
    ti.xcom_push(key="y_train", value=y_train.to_json(orient="records")) #y값은 한줄이라 레코드라 입력
    ti.xcom_push(key="y_test", value=y_test.to_json(orient="records"))

#op_kwargs={"model_name":"GradientBoosting"},
#op_kwargs={"model_name":"RandomFroest"} d이렇게 넘어온다
def train_model(model_name,**kwargs):
    ti = kwargs["ti"]
    x_train = pd.read_json(ti.xcom_pull(key="x_train",task_ids="feature_engineering"))
    x_test = pd.read_json(ti.xcom_pull(key="x_test",task_ids="feature_engineering"))
    y_train = pd.read_json(ti.xcom_pull(key="y_train",task_ids="feature_engineering"),typ="series") # 디폴트가 위에 컬럼으로 되어있어서 일부러 넣어야함
    y_test = pd.read_json(ti.xcom_pull(key="y_test",task_ids="feature_engineering"),typ="series")

    if model_name == "RandomForest":
        model = RandomForestClassifier()

    elif model_name == "GrandientBoosting":
        model = GradientBoostingClassifier()
    else:
        raise ValueError("Unsupported model: " + model_name)
    
    model.fit(x_train,y_train)
    y_pred = model.predict(x_test)
    acc = accuracy_score(y_test, y_pred)

    ti.xcom_push(key=f"acc_{model_name}", value=acc)





def select_best_model(**kwargs):
    ti = kwargs["ti"]
    rf_acc = ti.xcom_pull(key="acc_RandomForest", task_ids = "train_rf")
    gb_acc = ti.xcom_pull(key="GradientBoosting", task_ids = "train_gb")

    best_model = "RandomForest" if rf_acc > gb_acc else "GradientBoosting"
    print(f"best model is {best_model} with accuracy {max(rf_acc, gb_acc)}")

    return best_model

with dag:
    t1 = PythonOperator(
        task_id = "feature_engineering",
        python_callable = feature_engineering,


    )
    t2 = PythonOperator(
        task_id = "train_rf", #아이디 반드시들어가야함
        python_callable = train_model, # 함수
        op_kwargs={"model_name":"RandomFroest"}, # 이건 그냥 반드시포함되는문장, 다른애들은 그냥넣어도된다?
        provide_context=True, # 요즘 이거 안넣어줘도 넘어간다. 이유찾아보자


    )

    t3 = PythonOperator(
        task_id = "train_gb",
        python_callable = train_model,
        op_kwargs={"model_name":"GradientBoosting"}, # 이건 그냥 반드시포함되는문장
        provide_context=True,


    )

    t4= PythonOperator(
        task_id = "select_best_model",
        python_callable = select_best_model,



    )

    t1 >> [t2,t3] >> t4