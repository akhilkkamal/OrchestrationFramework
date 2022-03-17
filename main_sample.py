import sys, os, pathlib, subprocess
import json
from airflow import DAG, settings
from airflow.models import Pool, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


def create_dag(dag_id, sch_interval, source, source_dict, default_args):
    def create_pool(pool_name, slots):
        session = settings.Session()
        pool_obj = session.query(Pool).filter(Pool.pool == pool_name).first()
        if not pool_obj:
            pool = Pool(pool=pool_name, slots=slots)
            session.add(pool)
            session.commit()
        elif pool_obj.slots != slots:
            session.query(Pool).filter(Pool.pool == pool_name).update({'slots': slots})
            session.commit()

    def create_task(source, source_dict, layer, entity, pool):
        scripts_directory = Variable.get("SCRIPTS_DIRECTORY")
        operator = source_dict[layer][entity]['operator']
        if operator['type'] == 'spark':
            return SparkSubmitOperator(
                task_id=source + '_' + layer + '_' + entity,
                conn_id=operator['conn_id'],
                name=operator['name'],
                application=operator['application'],
                packages=operator['packages'],
                application_args=[entity],
                pool=pool
            )
        elif operator['type'] == 'python':
            return PythonOperator(
                task_id=source + '_' + layer + '_' + entity,
                python_callable=pythonCallableMethod,
                pool=pool,
                op_kwargs={
                    'source': source,
                    'layer': layer,
                    'entity': entity,
                    'operator': operator,
                    'scripts_directory': scripts_directory
                }
            )
        elif operator['type'] == 'bash':
            return BashOperator(
                task_id=source + '_' + layer + '_' + entity,
                bash_command=operator['bash_command'],
                pool=pool
            )
        return DummyOperator(task_id=source + '_' + layer + '_' + entity, pool=pool)

    def _rest_operator(source, layer, entity):
        print('Task - source: {} | layer: {} | entity: {}'.format(str(source), str(layer), str(entity)))
        layer_params = str(Variable.get(source + '_' + layer + '_PARAMS', default_var='{}'))
        layer_params = json.loads(layer_params)
        print('Layer Params: {}'.format(str(layer_params)))

    def pythonCallableMethod(scripts_directory, operator, source, layer, entity):
        # PACKAGE_PARENT = pathlib.Path(__file__).parent
        # print(PACKAGE_PARENT)
        # SCRIPT_DIR = PACKAGE_PARENT/"my_Folder_where_the_package_lives"
        SCRIPT_DIR = scripts_directory
        sys.path.append(str(SCRIPT_DIR))
        imports = operator['imports']
        for i in imports:
            try:
                from_import = i.split(':')
                if len(from_import) == 1:
                    exec(f"import {i}")
                else:
                    exec(f"from {from_import[0]} import {from_import[1]}")
                    print(f"from {from_import[0]} import {from_import[1]}")
            except Exception as e:
                print(f"Exception : {e}")
        print(operator['python_callable_method'])
        for i in operator['python_callable_method']:
            try:
                print(f"Calling Method : {i}")
                eval(i)
            except Exception as e:
                print(f"Exception : {e}")
                return e

    dag = DAG(dag_id, schedule_interval=sch_interval, default_args=default_args)

    with dag:
        layers = [k for k in source_dict.keys()]

        start = DummyOperator(
            task_id='start_' + source
        )

        for layer in layers:
            layer_end = DummyOperator(
                task_id=layer + '_end'
            )

            entities = [k for k in source_dict[layer].keys()]
            with TaskGroup(layer + '_group') as layer_group:
                for entity in entities:
                    pool = source + '_' + layer + entity + '_pool'
                    slots = int(source_dict[layer][entity]['max_batch'])
                    create_pool(pool, slots)
                    entity_task = create_task(source, source_dict, layer, entity, pool)
                start.set_downstream(layer_group)
                layer_end.set_upstream(layer_group)
            start = layer_end

    return dag


source_list_var = str(Variable.get('SOURCE_LIST'))
source_list = [x.strip() for x in source_list_var.split(',')]

for source in source_list:
    dag_id = '{}_dag'.format(str(source))
    source_dict = json.loads(Variable.get(dag_id))
    print(source_dict)

    default_args = {
        'owner': 'airflow',
        'start_date': days_ago(2),
        'retries': 0
    }

    sch_interval = None

    globals()[dag_id] = create_dag(dag_id, sch_interval, source, source_dict, default_args)
