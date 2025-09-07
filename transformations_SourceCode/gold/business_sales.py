import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = "business_sales"
)
def business_sales():
    df_fact = spark.read.table("fact_sales")
    dim_prod = spark.read.table("dim_products")
    dim_cust = spark.read.table("dim_customers")

    df_join = df_fact.join(dim_prod, df_fact.product_id == dim_prod.product_id, "inner") \
                     .join(dim_cust, df_fact.customer_id == dim_cust.customer_id, "inner")
    
    df_prun = df_join.select("region","category","total_amount")
    df_agg = df_prun.groupBy("region","category").agg(sum("total_amount").alias("total_sales"))
    return df_agg