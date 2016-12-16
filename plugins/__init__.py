from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from plugins import operators


# Defining the plugin class
class ExecHqlPlugin(AirflowPlugin):
    name = "exechql_plugin"
    operators = [
        operators.ExecHqlOperator
    ]
