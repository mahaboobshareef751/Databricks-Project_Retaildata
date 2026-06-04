#step1:
    
select * from company.retaildatabase.retailtab;
select * from company.retaildatabase.customertab;

#step2:
    
from pyspark.sql.functions import *

silver_retail = spark.table("company.retaildatabase.retailtab")
silver_customer = spark.table("company.retaildatabase.customertab")

orders_enriched = (silver_retail.join(
    silver_customer.drop("customer_id"), 
    silver_retail.customer_id == silver_customer.customer_id, 
    "left"
))

orders_enriched = orders_enriched.withColumn("order_date", to_timestamp(col("order_date"))) \
                                 .withColumn("year", year(col("order_date"))) \
                                 .withColumn("month", month(col("order_date")))

gold_df = (
    orders_enriched.groupBy("product_id", "product_name", "customer_id", "category", "year", "month", "order_date", "gender", "age", "city", "loyalty_tier")
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum(coalesce(col("quantity"), lit(0))).alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_unit_price"),
        sum(when(col("returned") == "yes", 1).otherwise(0)).alias("returned_count"),
        sum(when(col("returned") == "yes", col("total_amount")).otherwise(0.0)).alias("returned_amount")
    )
)

gold_df = gold_df.withColumn("avg_order_val", when(col("total_orders") > 0, col("total_revenue") / col("total_orders")).otherwise(lit(0.0))) \
                 .withColumn("avg_revenue_per_item", when(col("total_quantity") > 0, col("total_revenue") / col("total_quantity")).otherwise(lit(0.0))) \
                 .withColumn("refreshed", current_timestamp())

gold_df.write.format("delta").mode("overwrite").saveAsTable("company.retaildatabase.gold_data")

#step3:
    

select * from company.retaildatabase.gold_data;