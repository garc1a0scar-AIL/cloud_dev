import os
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.utils import dates
from google.cloud import bigquery 
from airflow.operators.dummy import DummyOperator 
from airflow.operators.bash import BashOperator
from bigquery_dv_operator import BigQueryDVOperator

COUNTRY = 'mx'
SRC_SYSTEM='sbo'
CCP_DAGS_FOLDER = os.getenv('DAGS_FOLDER')
PROJECT_ID=Variable.get("SINGULARITY_PROJECT_ID")
SRC_SYSTEM_CD=f"{SRC_SYSTEM}_{COUNTRY}"
DAG_ID = os.path.basename(__file__).replace(".py", "")
CONFIG_PARAMS=pd.read_csv(CCP_DAGS_FOLDER + f'/exec_conf/{SRC_SYSTEM}/{COUNTRY}/{DAG_ID}.conf', sep=',', header = 0)
client = bigquery.Client(project=PROJECT_ID)


with DAG(
    dag_id=DAG_ID,
    description='Link dataflow for SBO MX',
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
    tags=["rdv", SRC_SYSTEM_CD, "datadog : not-monitoring"],
) as dag:

    start_workflow = DummyOperator(
        task_id=f'start_{DAG_ID}',
    )

    end_workflow = DummyOperator(
        task_id=f'end_{DAG_ID}',
    )
    
    for index, row in CONFIG_PARAMS.iterrows():
        if index>0:
            locals()['sleep_'+str(index-1)] = BashOperator(
                    task_id=str('run_sleep'+str(index-1)),
                    depends_on_past=False,
                    bash_command='sleep 10',
                    retries=1,
            )    
    
    for index, row in CONFIG_PARAMS.iterrows():
        for col in list(CONFIG_PARAMS.columns):
            project_id=str(CONFIG_PARAMS['project'][index])
            src_dataset_nm=str(CONFIG_PARAMS['src_dataset_nm'][index])
            src_datastore_nm=str(CONFIG_PARAMS['src_datastore_nm'][index])
            src_datastore_hashkey_nm=str(CONFIG_PARAMS['src_datastore_hashkey_nm'][index])
            tgt_dataset_nm=str(CONFIG_PARAMS['tgt_dataset_nm'][index])
            tgt_datastore_nm=str(CONFIG_PARAMS['tgt_datastore_nm'][index])
            tgt_datastore_hashkey_nm=str(CONFIG_PARAMS['tgt_datastore_hashkey_nm'][index])
            tgt_partition_id=str(CONFIG_PARAMS['tgt_partition_id'][index])
            table_type=str(CONFIG_PARAMS['table_type'][index])

        print(project_id)

        # Python Operator constructor
        locals()['bq_merge_task_'+str(index)] = BigQueryDVOperator (
            task_id=f"bq_{tgt_datastore_nm}_merge",
            project_id=project_id,
            src_dataset_nm=src_dataset_nm,
            src_datastore_nm=src_datastore_nm,
            src_datastore_hashkey_nm=src_datastore_hashkey_nm,
            tgt_dataset_nm=tgt_dataset_nm,
            tgt_datastore_nm=tgt_datastore_nm,
            tgt_datastore_hashkey_nm=tgt_datastore_hashkey_nm,
            tgt_partition_id=tgt_partition_id,
            table_type=table_type
        )
    
    #start_workflow>>(locals()['bq_merge_task_'+str(0)]) #Nodo inicial del workflow
    for index, row in CONFIG_PARAMS.iterrows():
        if index==0 and (CONFIG_PARAMS.shape[0])==1:
            start_workflow>>(locals()['bq_merge_task_'+str(index)])
        if index==0 and (CONFIG_PARAMS.shape[0])==2:
            start_workflow>>(locals()['bq_merge_task_'+str(index)])
            start_workflow>>(locals()['sleep_'+str(index)])
            locals()['sleep_'+str(index)]>>(locals()['bq_merge_task_'+str(index+1)])
        if index==0 and (CONFIG_PARAMS.shape[0])>2:
            start_workflow>>(locals()['sleep_'+str(index)])
            locals()['sleep_'+str(index)]>>(locals()['bq_merge_task_'+str(index+1)])
            locals()['sleep_'+str(index)]>>(locals()['sleep_'+str(index+1)])
            start_workflow>>(locals()['bq_merge_task_'+str(index)])
        elif 0<index<(CONFIG_PARAMS.shape[0])-2:
            locals()['sleep_'+str(index)]>>(locals()['bq_merge_task_'+str(index+1)])
            locals()['sleep_'+str(index)]>>(locals()['sleep_'+str(index+1)])
        elif index==(CONFIG_PARAMS.shape[0])-2:
            #Workflow main tasks
            locals()['sleep_'+str(index)]>>(locals()['bq_merge_task_'+str(index+1)]) 
        # connect with workflow ending task
        locals()['bq_merge_task_'+str(index)]>>(end_workflow)