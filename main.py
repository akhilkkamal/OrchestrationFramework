import json
from airflow import DAG, settings
from airflow.models import Pool, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from constants import OrchestratorConstants as OC
from context.OrchestratorContext import OrchestratorContext

from factory import OrchestratorFactory


def orchestrate():
    configurator = OrchestratorFactory.\
        get_configurator(str(Variable.get(OC.CONFIG_TYPE)))

    dag_id_list = str(Variable.get(OC.DAG_ID_LIST)).split(OC.DELIMITER)
    [execute(OrchestratorContext(configurator, dag_id)) for dag_id in dag_id_list]


def execute(context):
    dag_template = OrchestratorFactory. \
        get_dag_template(str(Variable.get(OC.TEMPLATE_TYPE)))

    globals()[context.get_dag_id] = dag_template.create_dag(context)


orchestrate()
