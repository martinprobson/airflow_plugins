#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
from __future__ import print_function
from airflow import utils as airflow_utils
#from airflow.operators.hive_operator import HiveOperator
from airflow import models
import logging
import subprocess
import os.path

from airflow.exceptions import AirflowException



class ExecHqlOperator(models.BaseOperator):
    @airflow_utils.apply_defaults
    def __init__(self, 
                 hqlfile , 
                 hqlfilepath=None, 
                 verbose=True, 
                 hiveconf={},
                 hivevar={},
                 **kwargs):
        self.verbose = verbose
        self.hqlfile = hqlfile
        self.hqlfilepath = hqlfilepath
        self.hiveconf=hiveconf
        self.hivevar=hivevar
        #
        # If hqlfilepath supplied, assume this is the location of hqlfile, otherwise
        # assume hqlfile contains a absolute path already.
        #
        if self.hqlfilepath:
            if self.hqlfilepath.endswith(os.path.sep):
                self.hqlfile = self.hqlfilepath + self.hqlfile
            else:
                self.hqlfile = self.hqlfilepath + os.path.sep + self.hqlfile
        if not os.path.isfile(self.hqlfile):
            raise AirflowException(self.hqlfile + " does not exist")
        super(ExecHqlOperator, self).__init__(**kwargs)

    def execute(self, context):
        #
        # Deal with hiveconf and hivevar key/value pairs
        hiveconfs = []
        for key in self.hiveconf:
            hiveconfs.append('--hiveconf')
            hiveconfs.append(key + '=' + self.hiveconf[key])
        hivevars = []
        for key in self.hivevar:
            hivevars.append('--hivevar')
            hivevars.append(key + '=' + self.hivevar[key])

        cmdline = ['ExecHql', '-f', self.hqlfile]
        cmdline = cmdline + hiveconfs + hivevars
        if self.verbose == True:
            cmdline.append('--verbose')
            cmdline.append('true')
            logging.info('About to call ExecHql:' + " ".join(cmdline))
        sp = subprocess.Popen(
            cmdline,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        stdout = ''
        while True:
            line = sp.stdout.readline()
            if not line:
                break
                stdout += line.decode('UTF-8')
            if self.verbose:
                logging.info(line.decode('UTF-8').strip())
        sp.wait()
        if sp.returncode:
            raise AirflowException(stdout)
        return stdout


if __name__ == '__main__':
    from airflow import DAG
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
    # called MyTutorial.
    
    dag = DAG('ExecHqlTest', default_args=default_args, 
              schedule_interval=timedelta(1),
              max_active_runs=1,
              user_defined_macros=dict(measure_date='20161125'))
    dag.doc_md = __doc__
    
    default_args = {
        'verbose': True,
        'hiveconf':{'hiveconf1':'valhiveconf1'},
        'hivevar':{'hivevar1':'valhivevar1','hivevar2':'valhivevar2','hivevar3':'valhivevar3'},        
        'hqlfilepath' : '/home/martinr/Python_Devel/airflow_test/sample_hql'
    }
    task1 =  ExecHqlOperator(
        default_args=default_args,
        task_id='Hive_task_1',
        wait_for_downstream=True,
        hqlfile='sample.hql',
        dag=dag)    
    task1.execute("")
