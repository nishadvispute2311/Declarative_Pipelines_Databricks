import dlt

product_rules = {
    "rule_1" : "product_id IS NOT NULL",
    "rule_2" : "price > 0"
}

@dlt.table(
    name="products_stg"
)
@dlt.expect_all_or_drop(product_rules)
def products_stg():
    df = spark.readStream.table("dltnishad.source.products")
    return df