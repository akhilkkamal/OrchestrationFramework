from api.IIngestionOperator import IIngestionOperator


class DummyOperator(IIngestionOperator):
    def __init__(self,name):
        self._name=name;

    def get_operator(self):
        return DummyOperator(
            task_id=self._name
        )
