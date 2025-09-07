import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "sales_transform"
)
def sales_transform():
  
  df = spark.readStream.table("sales_stg")
  df= df.withColumn("total_amount", col("quantity") * col("amount"))
  return df

dlt.create_streaming_table(
    name = "sales_enriched"
)

dlt.create_auto_cdc_flow(
    target = "sales_enriched",
    source = "sales_transform",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    stored_as_scd_type = "1"
)