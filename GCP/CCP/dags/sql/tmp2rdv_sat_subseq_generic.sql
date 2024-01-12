MERGE {project}.{tgt_dataset_nm}.{tgt_datastore_nm} T 
USING( 
SELECT C.{src_datastore_hashkey_nm} as hash_key , C.* FROM {project}.{src_dataset_nm}.{src_datastore_nm} C 
UNION ALL 
SELECT NULL, C.* FROM {project}.{src_dataset_nm}.{src_datastore_nm} C 
JOIN {project}.{tgt_dataset_nm}.{tgt_datastore_nm} E ON C.{src_datastore_hashkey_nm} = E.{tgt_datastore_hashkey_nm} AND C.ai_sub_sequence = E.ai_sub_sequence 
WHERE (E.an_current_flag = 1 AND C.ah_checksum <> E.ah_checksum )) S ON T.{tgt_datastore_hashkey_nm} = S.hash_key AND T.ai_sub_sequence = S.ai_sub_sequence AND T.an_current_flag = 1 
WHEN MATCHED AND S.ah_checksum <> T.ah_checksum THEN UPDATE SET T.an_current_flag = 0 
WHEN NOT MATCHED THEN INSERT ({tgt_datastore_columns_nm}) VALUES({src_datastore_columns_nm}) 
;