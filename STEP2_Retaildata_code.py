#step1: read the data from branze layer

data = '/Volumes/company/retaildatabase/retailvolume/bronze/retail_dataset.csv'

retail_df = spark.read.csv(data, header=True, inferSchema=True)
display(retail_df)

#step2: to read the schema of dataframe

  retail_df.printSchema()

#step3: Retail data in csv format we need to do the transformation to cleaning the data using the silver layer;.

from pyspark.sql.functions import *


order_ts = coalesce(
    expr("try_to_timestamp(trim(order_date), 'yyyy-MM-dd')"),
    expr("try_to_timestamp(trim(order_date), 'dd-MM-yyyy')"),
    expr("try_to_timestamp(trim(order_date), 'dd-MMM-yyyy')"),
    expr("try_to_timestamp(trim(order_date), 'MM-dd-yyyy')"),
    expr("try_to_timestamp(trim(order_date), 'dd/MM/yyyy')")
)



clean_retail_df = (retail_df.withColumn("order_id", trim(col("order_id")).cast("long"))
                           .withColumn("order_date", order_ts)
                           .withColumn("customer_id", trim(col("customer_id")))
                           .withColumn("customer_name", trim(col("customer_name")))
                           .withColumn("product_id", trim(col("product_id")))
                           .withColumn("product_name", lower(trim(col("product_name"))))
                           .withColumn("category", lower(trim(col("category"))))
                           .withColumn("quantity", when(trim(col("quantity")).isNull() | (trim(col("quantity")) == ""), lit(1)).otherwise(trim(col("quantity")))) #quantity
                           .withColumn("quantity", col("quantity").cast("int"))
                           .withColumn("quantity", when(col("quantity") <=0, lit(1)).otherwise(trim(col("quantity"))))
                        #    .withColumn("price", trim(col("price")))
                        #    .withColumn("price_clean", regexp_replace(trim(col("price")), "$,", ""))
                           .withColumn("price_clean", translate(trim(col("price")), "$,", ""))
                           .withColumn("price", col("price_clean").cast("double")).drop("price_clean")
                           .withColumn("payment_type", lower(trim(col("payment_type"))))
                           .withColumn("order_status", lower(trim(col("order_status"))))
                           .withColumn("returned", lower(trim(col("returned"))))
                           .withColumn("total_amount", 
                                        (coalesce(col("quantity"), lit(0)) * coalesce(col("price"), lit(0.0))).cast("double"))

)

clean_retail =  clean_retail_df.dropDuplicates(["order_id", "product_id"]).filter(col("order_id").isNotNull() & col("product_id").isNotNull())
display(clean_retail)

clean_retail.write.format("delta").mode("overwrite").saveAsTable("company.retaildatabase.retailtab")

