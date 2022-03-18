class OrchestratorContext:
    def __init__(self, configurator, dag_id):
        self._configurator = configurator
        self._dag_id = dag_id

    @property
    def get_configurator(self):
        return self._configurator

    @property
    def get_dag_id(self):
        return self._dag_id
