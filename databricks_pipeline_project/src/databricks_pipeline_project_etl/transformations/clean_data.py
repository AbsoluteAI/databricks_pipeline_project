from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_replace, col

@dp.table
def read_table():
    df = spark.read.table("sales_data.sales_db.customers_master")
    df = df.dropDuplicates()
    df = df.dropna(how="all")
    df = df.withColumn("phone",
        regexp_replace(
            regexp_replace(col("phone"), r"^0+", ""), "[^0-9]", "")
        )
    return df