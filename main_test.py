from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from functools import reduce


def create_dag():
    # 1. Operator factory is needed.
    # 2. Task creator is needed.
    # 3. Initial and end operators should be moved to a common class.
    dag = DAG('dynamic_dag', schedule_interval='0 0 * * *', default_args={'retries': 2})

    with dag:
        start = DummyOperator('start')
        end = DummyOperator('end')
        layers = []
        for object_name, layer_configs in [("obj1", ["config1", "config2", "config3"]), ("obj2", ["config1", "config2"]), ("obj3", ["config1"])]:
            with TaskGroup(object_name + '_group') as layer_group:
                objects = []
                for layer_config in layer_configs:
                    objects.append(DummyOperator(object_name+layer_config))
                reduce(lambda t1, t2: t1 >> t2, objects)
            layers.append(layer_group)
        start >> layers >> end

    return dag


globals()["dynamic_dag"] = create_dag()
