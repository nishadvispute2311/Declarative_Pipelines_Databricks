import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "products_transform"
)
def products_transform():
  
  df = spark.readStream.table("products_stg")
  df= df.withColumn("price", col("price").cast("int"))
  return df

dlt.create_streaming_table(
    name = "products_enriched"
)

dlt.create_auto_cdc_flow(
    target = "products_enriched",
    source = "products_transform",
    keys = ["product_id"],
    sequence_by = "last_updated",
    stored_as_scd_type = "1"
)