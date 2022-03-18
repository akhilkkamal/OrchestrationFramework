from api.IDagTemplate import IDagTemplate
from airflow.models import Pool, Variable
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup


class ObjectWiseDagTemplate(IDagTemplate):
    def __init__(self, context):
        self._context = context
        self._df = context.get_configurator.get_configuration(self._query)

    def create_dag(self):
        # 1. Operator factory is needed.
        # 2. Task creator is needed
        dag = DAG('dag_id', schedule_interval='sch_interval', default_args='default_args')

        with dag:
            start = DummyOperator('start')
            end = DummyOperator('end')
            for object_name, layer_configs in self._df.groupby(['object_name']):

                with TaskGroup(object_name + '_group') as layer_group:
                    for layer_config in layer_configs:
                        object_task = self.create_task(object_name, layer_config)
                start.set_downstream(layer_group)
                end.set_upstream(layer_group)

        return dag
