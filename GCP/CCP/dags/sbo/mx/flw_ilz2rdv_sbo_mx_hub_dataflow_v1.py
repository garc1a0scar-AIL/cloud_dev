import os
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.utils import dates
from google.cloud import bigquery 
from airflow.operators.dummy import DummyOperator 
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator 

COUNTRY = 'mx'
SRC_SYSTEM='sbo'
CCP_DAGS_FOLDER = os.getenv('DAGS_FOLDER')
PROJECT_ID=Variable.get("SINGULARITY_PROJECT_ID")
SRC_SYSTEM_CD=f"{SRC_SYSTEM}_{COUNTRY}"
CONFIG_PARAMS=pd.read_csv(CCP_DAGS_FOLDER + f'/exec_conf/{SRC_SYSTEM}/{COUNTRY}/flw_ilz2rdv_{SRC_SYSTEM_CD}_hub_dataflow.conf', sep=',', header = 0)
SQL_FILES=["tmp2rdv_src_get_columns_nm.sql","tmp2rdv_tgt_get_columns_nm.sql","tmp2rdv_hub_generic.sql"]
client = bigquery.Client(project=PROJECT_ID)
DAG_ID = os.path.basename(__file__).replace(".py", "")

def hub_bq_merge(sql_src_params_ins, sql_tgt_params_ins, sql_hub_pattern_ins):
    print(f" Starting hub dataflow ")
    
    print(f"    Invoke source schema extraction ")
    rs_src_params = client.query(sql_src_params_ins,location="US",job_id_prefix="src_schema_extraction_")
    #storing the query result in a variable
    for row in rs_src_params:
        src_datastore_columns_nm = row
    src_datastore_columns_nm = str(src_datastore_columns_nm)[6:-37]
    print(f"        Extracted column names: {src_datastore_columns_nm} ")
    
    print(f"    Invoke target schema extraction ")
    rs_tgt_params = client.query(sql_tgt_params_ins,location="US",job_id_prefix="tgt_schema_extraction_")
    #storing the query result in a variable
    for row in rs_tgt_params:
        tgt_datastore_columns_nm = row
    tgt_datastore_columns_nm = str(tgt_datastore_columns_nm)[6:-37]
    print(f"        Extracted column names: {tgt_datastore_columns_nm} ")
    
    sql_hub_pattern=sql_hub_pattern_ins.replace("{src_datastore_columns_nm}",src_datastore_columns_nm).replace("{tgt_datastore_columns_nm}",tgt_datastore_columns_nm)
    
    print(f"    Merge prepared statement: {sql_hub_pattern}")
    merge_job=client.query(sql_hub_pattern,location="US",job_id_prefix="hub_merge_")
    # Wait for merge_job to finish.
    merge_job.result()
    
    print(f"    Merge statement modified {merge_job.num_dml_affected_rows} rows.")

with DAG(
    f"flw_ilz2rdv_{SRC_SYSTEM_CD}_hub_dataflow",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
    tags=["rdv", SRC_SYSTEM_CD],
) as dag:

    start_workflow = DummyOperator(
        task_id='start_wflw_tmp2rdv_'+SRC_SYSTEM_CD+'_hub_dataflow',
    )

    end_workflow = DummyOperator(
        task_id='end_wflw_tmp2rdv_'+SRC_SYSTEM_CD+'_hub_dataflow',
    )
    ## source params SQL script
    with open(CCP_DAGS_FOLDER + '/sql/'+SQL_FILES[0], 'r') as file:
        sql_scr_params_file=file.read().replace('\n',' ')
    ## target params SQL script
    with open(CCP_DAGS_FOLDER + '/sql/'+SQL_FILES[1], 'r') as file:
        sql_tgt_params_file=file.read().replace('\n',' ')
    ## hub generic pattern SQL script
    with open(CCP_DAGS_FOLDER + '/sql/'+SQL_FILES[2], 'r') as file:
        sql_hub_pattern_file=file.read().replace('\n',' ')
    
    for index, row in CONFIG_PARAMS.iterrows():
        if index>0:
            locals()['sleep_'+str(index-1)] = BashOperator(
                    task_id=str('run_sleep'+str(index-1)),
                    depends_on_past=False,
                    bash_command='sleep 10',
                    retries=1,
            )    
    
    for index, row in CONFIG_PARAMS.iterrows():
        ## instantiate base SQL scripts
        sql_src_params_ins=sql_scr_params_file
        sql_tgt_params_ins=sql_tgt_params_file
        sql_hub_pattern_ins=sql_hub_pattern_file
    
        for col in list(CONFIG_PARAMS.columns):
            sql_src_params_ins=sql_src_params_ins.replace("{"+str(col)+"}",str(CONFIG_PARAMS[col][index])) 
            sql_tgt_params_ins=sql_tgt_params_ins.replace("{"+str(col)+"}",str(CONFIG_PARAMS[col][index]))  
            sql_hub_pattern_ins=sql_hub_pattern_ins.replace("{"+str(col)+"}",str(CONFIG_PARAMS[col][index])) 
        
        # Python Operator task_id
        datastore_nm=str(CONFIG_PARAMS["tgt_datastore_nm"][index])
        # Python Operator constructor
        locals()['bq_merge_task_'+str(index)] = PythonOperator(
            task_id=f"bq_{datastore_nm}_merge",
            python_callable=hub_bq_merge, 
            op_args=[sql_src_params_ins, sql_tgt_params_ins, sql_hub_pattern_ins]
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