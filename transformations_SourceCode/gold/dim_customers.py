import dlt

dlt.create_streaming_table(
    name = "dim_customers"
)

dlt.create_auto_cdc_flow(
    target = "dim_customers",
    source = "customer_transform",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = "2",
    track_history_except_column_list = None
)