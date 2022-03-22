from api.IDagTemplate import IDagTemplate
from airflow.models import Pool, Variable
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from functools import reduce


class ObjectWiseDagTemplate(IDagTemplate):
    def __init__(self, context):
        self._context = context
        self._df = context.get_configurator.get_configuration(self._query)

    def create_dag(self):
        # 1. Operator factory is needed.
        # 2. Task creator is needed.
        # 3. Initial and end operators should be moved to a common class.
        dag = DAG('dag_id', schedule_interval='sch_interval', default_args='default_args')

        with dag:
            start = DummyOperator('start')
            end = DummyOperator('end')
            layers = []
            for object_name, layer_configs in self._df.groupby(['object_name']):
                with TaskGroup(object_name + '_group') as layer_group:
                    objects = [ ]
                    for layer_config in layer_configs:
                        objects.append(self.create_task(object_name, layer_config))
                    reduce(lambda t1, t2: t1 >> t2, objects)
                layers.append(layer_group)
            start >> layers >> end

        return dag
