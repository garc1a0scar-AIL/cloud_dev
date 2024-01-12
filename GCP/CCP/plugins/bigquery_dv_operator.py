from builtins import str
from typing import Any
from google.cloud import bigquery
from airflow.models import BaseOperator
from functools import partial

def hub_merge(self) -> str:

    sql_pattern_ins=f"""MERGE {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} T 
        USING( SELECT * FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} 
        ) S ON S.{self.src_datastore_hashkey_nm} = T.{self.tgt_datastore_hashkey_nm} AND T.ai_src_system = {self.tgt_partition_id} 
        WHEN NOT MATCHED THEN INSERT (tgt_datastore_columns_nm) VALUES(src_datastore_columns_nm) 
        ;"""
    
    return sql_pattern_ins

def lnk_merge(self) -> str:

    sql_pattern_ins=f"""MERGE {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} T 
        USING( SELECT * FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} 
        ) S ON S.{self.src_datastore_hashkey_nm} = T.{self.tgt_datastore_hashkey_nm} AND T.ai_src_system = {self.tgt_partition_id} 
        WHEN NOT MATCHED THEN INSERT (tgt_datastore_columns_nm) VALUES(src_datastore_columns_nm)
        ;"""
    
    return sql_pattern_ins

def sat_merge(self) -> str:

    sql_pattern_ins=f"""MERGE {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} T
        USING(
        SELECT C.{self.src_datastore_hashkey_nm} as hash_key , C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C
        UNION ALL 
        SELECT NULL, C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C
        JOIN {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} E ON C.{self.src_datastore_hashkey_nm} = E.{self.tgt_datastore_hashkey_nm} 
        WHERE (E.ai_current_flag = 1 AND C.ah_checksum <> E.ah_checksum )) S ON T.{self.tgt_datastore_hashkey_nm} = S.hash_key AND T.ai_current_flag = 1 
        WHEN MATCHED AND S.ah_checksum <> T.ah_checksum THEN UPDATE SET T.ai_current_flag = 0
        WHEN NOT MATCHED THEN INSERT (tgt_datastore_columns_nm) VALUES(src_datastore_columns_nm)
        ;"""
    
    return sql_pattern_ins

def sat_subseq_merge(self) -> str:

    sql_pattern_ins=f"""MERGE {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} T 
        USING( 
        SELECT C.{self.src_datastore_hashkey_nm} as hash_key , C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C 
        UNION ALL 
        SELECT NULL, C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C 
        JOIN {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} E ON C.{self.src_datastore_hashkey_nm} = E.{self.tgt_datastore_hashkey_nm} AND C.ax_sub_sequence = E.ax_sub_sequence 
        WHERE (E.an_current_flag = 1 AND C.ah_checksum <> E.ah_checksum )) S ON T.{self.tgt_datastore_hashkey_nm} = S.hash_key AND T.ax_sub_sequence = S.ax_sub_sequence AND T.ai_current_flag = 1 
        WHEN MATCHED AND S.ah_checksum <> T.ah_checksum THEN UPDATE SET T.ai_current_flag = 0 
        WHEN NOT MATCHED THEN INSERT (tgt_datastore_columns_nm) VALUES(src_datastore_columns_nm) 
        ;"""
    
    return sql_pattern_ins

def sat_lnk_merge(self) -> str:

    sql_pattern_ins=f"""MERGE {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} T 
        USING(
        SELECT C.{self.src_datastore_hashkey_nm} as hash_key , C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C 
        UNION ALL 
        SELECT NULL, C.* FROM {self.project_id}.{self.src_dataset_nm}.{self.src_datastore_nm} C 
        JOIN {self.project_id}.{self.tgt_dataset_nm}.{self.tgt_datastore_nm} E ON C.{self.src_datastore_hashkey_nm} = E.{self.tgt_datastore_hashkey_nm} 
        WHERE (E.ai_current_flag = 1 AND C.ah_checksum <> E.ah_checksum )) S ON T.{self.tgt_datastore_hashkey_nm} = S.hash_key AND T.ai_current_flag = 1 
        WHEN MATCHED AND S.ah_checksum <> T.ah_checksum THEN UPDATE SET T.ai_current_flag =0 
        WHEN NOT MATCHED BY SOURCE AND T.ai_current_flag=1 THEN UPDATE SET T.ai_current_flag =0 
        WHEN NOT MATCHED THEN INSERT (tgt_datastore_columns_nm) VALUES(src_datastore_columns_nm) 
        ;"""
    
    return sql_pattern_ins


class BigQueryDVOperator(BaseOperator):

    ui_color = '#a1d9df'

    def __init__(
        self,
        project_id: str = None,
        src_dataset_nm: str = None,
        src_datastore_nm: str = None,
        src_datastore_hashkey_nm: str = None,
        tgt_dataset_nm: str = None,
        tgt_datastore_nm: str = None,
        tgt_datastore_hashkey_nm: str = None,
        tgt_partition_id: str = None,
        table_type: str = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id=project_id
        self.src_dataset_nm=src_dataset_nm
        self.src_datastore_nm=src_datastore_nm
        self.src_datastore_hashkey_nm=src_datastore_hashkey_nm
        self.tgt_dataset_nm=tgt_dataset_nm
        self.tgt_datastore_nm=tgt_datastore_nm
        self.tgt_datastore_hashkey_nm=tgt_datastore_hashkey_nm
        self.tgt_partition_id=tgt_partition_id
        self.table_type=table_type


    def execute(self, context: Any) -> None:

        table_type=self.table_type.lower()

        client = bigquery.Client(project=self.project_id)

        sql_src_params_ins=f"""SELECT STRING_AGG(DISTINCT CONCAT ('S.',CAST(COLUMN_NAME AS STRING))) 
        AS src_datastore_columns_nm 
        FROM {self.project_id}.{self.src_dataset_nm}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA="{self.src_dataset_nm}" AND TABLE_NAME="{self.src_datastore_nm}";"""

        sql_tgt_params_ins=f"""SELECT STRING_AGG(DISTINCT CAST(COLUMN_NAME AS STRING)) 
        AS tgt_datastore_columns_nm FROM {self.project_id}.{self.tgt_dataset_nm}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA="{self.tgt_dataset_nm}" AND TABLE_NAME="{self.tgt_datastore_nm}";"""


        print(f" Starting {self.table_type} dataflow ")

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

        table_type_map = {
            'hub': hub_merge,
            'lnk': lnk_merge,
            'same_as_lnk': lnk_merge,
            'sat': sat_merge,
            'sat_subseq': sat_subseq_merge,
            'sat_lnk': sat_lnk_merge
        }

        if table_type.lower() in table_type_map:
            sql_pattern_ins_func=table_type_map.get(table_type.lower(), partial(lambda: "Default Function"))
            sql_pattern_ins= sql_pattern_ins_func(self)
    
        sql_pattern=sql_pattern_ins.replace("src_datastore_columns_nm",src_datastore_columns_nm).replace("tgt_datastore_columns_nm",tgt_datastore_columns_nm)
    
        print(f"    Merge prepared statement: {sql_pattern}")
        merge_job=client.query(sql_pattern,location="US",job_id_prefix=f"{table_type}_merge")
        # Wait for merge_job to finish.
        merge_job.result()
    
        print(f"    Merge statement modified {merge_job.num_dml_affected_rows} rows.")