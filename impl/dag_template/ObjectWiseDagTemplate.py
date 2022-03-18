from api.IDagTemplate import IDagTemplate
from airflow.models import Pool, Variable
from itertools import groupby

class ObjectWiseDagTemplate(IDagTemplate):
    def __init__(self, context):
        self._context = context
        self._query = ""
        self._config_list = context.get_configurator.get_configuration(self._query)
    def create_dag(self):
        for object_config in self._config_list:
            with TaskGroup(o + '_group') as layer_group:
                for layer_config in config:



        pass
