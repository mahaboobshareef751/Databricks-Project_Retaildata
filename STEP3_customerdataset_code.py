#step1: read the customer_dataset using the branz later
data = '/Volumes/company/retaildatabase/retailvolume/bronze/customer_dataset.json'

customer_df = spark.read.json(data)
display(customer_df.limit(18))

#step2:
    customer_df.printSchema()

#step3: customer_dataset in json read the data and clean the data using the required transformations using the silver layer

from pyspark.sql.functions import *

------------------------------------silver customer 

order_tss = coalesce(
    expr("try_to_timestamp(trim(signup_date), 'yyyy-MM-dd')"),
    expr("try_to_timestamp(trim(signup_date), 'dd-MM-yyyy')"),
    expr("try_to_timestamp(trim(signup_date), 'dd-MMM-yyyy')"),
    expr("try_to_timestamp(trim(signup_date), 'MM-dd-yyyy')"),
    expr("try_to_timestamp(trim(signup_date), 'dd/MM/yyyy')")
)

clean_customer_df = (customer_df.withColumn('age', col('age').cast('int'))
                     .withColumn("age", when(trim(col("age")).isNull() | (col("age") <=0), lit(1)).otherwise(trim(col("age"))))
                     .withColumn('age', when(trim(col('age')) <= 0, lit(1)).otherwise(trim(col('age'))))
                    #  .withColumn('age', col('age').cast('int'))
                     .withColumn('city', lower(trim(col('city'))))
                     .withColumn('customer_id', trim(col('customer_id')))
                     .withColumn('gender', lower(trim(col('gender'))))
                     .withColumn('loyalty_tier', lower(trim(col('loyalty_tier'))))
                     .withColumn('signup_date', order_tss)
           
)
display(clean_customer_df)  
clean_customer_df.write.format("delta").mode("overwrite").saveAsTable("company.retaildatabase.customertab")
