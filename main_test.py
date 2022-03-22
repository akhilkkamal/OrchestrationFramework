from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from functools import reduce
from airflow.utils.dates import days_ago

default_args = {'owner': 'airflow','start_date': days_ago(2),'retries': 0}

dag = DAG('dynamic_dag', schedule_interval='0 0 * * *', default_args= default_args)

with dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    layers = []
    for object_name, layer_configs in [("obj1", ["config1", "config2", "config3"]), ("obj2", ["config1", "config2"]), ("obj3", ["config1"])]:
        with TaskGroup(object_name + '_group') as layer_group:
            objects = []
            for layer_config in layer_configs:
                objects.append(DummyOperator(task_id=object_name+layer_config))
            reduce(lambda t1, t2: t1 >> t2, objects)
        layers.append(layer_group)
    start >> layers >> end