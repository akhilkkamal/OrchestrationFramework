import json
from airflow import DAG, settings
from airflow.models import Pool, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from constants import OrchestratorConstants as OC

from factory import OrchestratorFactory


def orchestrate():
    dag_id = str(Variable.get(OC.DAG_ID))

    configurator = OrchestratorFactory. \
        get_configurator(str(Variable.get(OC.CONFIG_TYPE)))

    dag_template = OrchestratorFactory. \
        get_dag_template(str(Variable.get(OC.TEMPLATE_TYPE)))

    config_list = configurator.get_configuration()
    globals()[dag_id] = dag_template.create_dag(config_list)



orchestrate()
