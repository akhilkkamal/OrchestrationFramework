import pip
from pip._internal import main as pipmain


def install_whl(path):
    pipmain(['install', path])


install_whl('C:/Users/Akash.B1/Downloads/apache_airflow-2.2.4-py3-none-any.whl')