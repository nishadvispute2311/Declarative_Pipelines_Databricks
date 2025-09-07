import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "customer_transform"
)
def customer_transform():
  
  df = spark.readStream.table("customers_stg")
  df= df.withColumn("customer_name", upper(col("customer_name")))
  return df

dlt.create_streaming_table(
    name = "customer_enriched"
)

dlt.create_auto_cdc_flow(
    target = "customer_enriched",
    source = "customer_transform",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = "1"
)