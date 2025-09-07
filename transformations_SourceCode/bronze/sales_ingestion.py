import dlt

sales_rules = {
    "rule_1" : "sales_id is not null",
    "rule_2" : "amount > 0"
}

dlt.create_streaming_table(
    name="sales_stg",
    expect_all_or_drop=sales_rules)

@dlt.append_flow(target="sales_stg")
def east_sales():
    df = spark.readStream.table("dltnishad.source.sales_east")
    return df

@dlt.append_flow(target="sales_stg")
def west_sales():
    df = spark.readStream.table("dltnishad.source.sales_west")
    return df