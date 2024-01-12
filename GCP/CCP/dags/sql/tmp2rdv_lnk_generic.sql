MERGE {project}.{tgt_dataset_nm}.{tgt_datastore_nm} T 
USING( SELECT * FROM {project}.{src_dataset_nm}.{src_datastore_nm} 
) S ON S.{src_datastore_hashkey_nm} = T.{tgt_datastore_hashkey_nm} AND T.an_src_system = {tgt_partition_id} 
WHEN NOT MATCHED THEN INSERT ({tgt_datastore_columns_nm}) VALUES({src_datastore_columns_nm}) 
;