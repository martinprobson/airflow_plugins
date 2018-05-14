# Python - Airflow - Job Scheduler Plugins

## Summary
This repo contains an [Apache Airflow](https://airflow.apache.org/) plugin to make it easier to execute Hql scripts via an Airflow task.

### Installation
Drop the `airflow_plugins` folder into the location defined by `plugins_foler` in `airflow.cfg`

### Example Usage
Set up an example DAG with `ExecHqlOperator` provided by this plugin.

```python
"""
### Test Hive from Airflow
Run a simple Hive workflow from Airflow for testing purposes.
"""
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( ExecHqlOperator )
from datetime import datetime, timedelta

default_args = {
        'owner': 'airflow',
        'depends_on_past': True,
        'start_date': datetime(2015, 6, 1),
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
}

# Create a DAG with the default_args with a schedule interval of one day -
# called HiveTest2.
dag = DAG('HiveTest2', default_args=default_args, 
          schedule_interval=timedelta(1),
          max_active_runs=1,
          user_defined_macros=dict(measure_date='20161125'))
dag.doc_md = __doc__

start = DummyOperator(task_id='Start',dag=dag)

default_args = {
    'verbose': True,
    'hqlfilepath' : '/home/martinr/Python_Devel/airflow_test/sample_hql'
}
    
task1 =  ExecHqlOperator(
    default_args=default_args,
    task_id='Hive_task_1',
    wait_for_downstream=True,
    hqlfile='sample.hql',
    dag=dag)

task2 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_2',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task3 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_3',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task4 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_4',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task5 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_5',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task6 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_6',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task7 =  ExecHqlOperator(
            default_args=default_args,
            task_id='Hive_task_7',
            wait_for_downstream=True,
            hqlfile='sample_hql2.hql',
            dag=dag)

task1.set_upstream(start)
task2.set_upstream(task1)
task3.set_upstream(start)
task4.set_upstream(task3)
task5.set_upstream(task4)
task6.set_upstream(task5)
task7.set_upstream(task6)
```

### ExecHqlOperator module.

#### ExecHqlOperator
```python
op = ExecHqlOperator(hqlfile,hqlfilepath=None,verbose=True,hiveconf={},hivevar={},**kwargs)
```

Additional parameters, over and above standard Airflow operator params are: -
- `hqlfile` - Name of hql script file.
- `hqlfilepath` - optional file path, if not supplied then hqlfile is assumed to be absolute path.
- `verbose` - verbose logging (to stdout)
- `hiveconf` - A dictionary of hiveconf values, that will be passed to hive.
- `hivevars` - A dictionary of hivevars that will be passed to hive.
