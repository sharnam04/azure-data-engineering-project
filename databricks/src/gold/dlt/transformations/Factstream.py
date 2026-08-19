import dlt
 
@dlt.table
def factstream_stg():
    df = spark.readStream.table('spotify_cata.silver.factstream')
    return df


dlt.create_streaming_table( 'factstream')

dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  system_sequence_by = None, # optional
  ignore_null_updates = False, # optional
  ignore_null_updates_column_list = None, # optional
  ignore_null_updates_except_column_list = None, # optional
  columns_to_update = None, # optional
  apply_as_deletes = None, # optional
  apply_as_truncates = None, # optional
  column_list = None, # optional
  stored_as_scd_type = "1", # optional
  name = None, # optional
  once = False # optional
)