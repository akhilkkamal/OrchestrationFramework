import importlib


def get_configurator(config_type):
    """Load the configurations."""
    return get_class_instance('impl.config.' + config_type)


def get_dag_template(template_type):
    """Load the configurations."""
    return get_class_instance('impl.dag_template.' + template_type)


def get_class_instance(class_name):
    module = importlib.import_module(class_name)
    print(module.__name__)
    name_array = class_name.split('.')
    cls = getattr(module, name_array[len(name_array) - 1])
    return cls()
