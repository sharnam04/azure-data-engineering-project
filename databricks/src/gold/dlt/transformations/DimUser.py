import dlt

expectations = {
  "rule_1":"user_id IS NOT NULL"
}
 
@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table('spotify_cata.silver.dimuser')
    return df


dlt.create_streaming_table( 
name='dimuser',
expect_all_or_drop= expectations
)

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
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