import dlt
 
@dlt.table
def dimdate_stg():
    df = spark.readStream.table('spotify_cata.silver.dimdate')
    return df


dlt.create_streaming_table( 'dimdate')

dlt.create_auto_cdc_flow(
  target = "dimdate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = "date",
  system_sequence_by = None, # optional
  ignore_null_updates = False, # optional
  ignore_null_updates_column_list = None, # optional
  ignore_null_updates_except_column_list = None, # optional
  columns_to_update = None, # optional
  apply_as_deletes = None, # optional
  apply_as_truncates = None, # optional
  column_list = None, # optional
  stored_as_scd_type = "2", # optional
  name = None, # optional
  once = False # optional
)