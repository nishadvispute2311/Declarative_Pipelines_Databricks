import dlt

customer_rules = {
    "rule_1": "customer_id is not null"
}

@dlt.table(
    name="customers_stg"
)
@dlt.expect_all_or_drop(customer_rules)
def products_stg():
    df = spark.readStream.table("dltnishad.source.customers")
    return df