from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date, hour, dayofmonth, month, year

# ✅ Initialize Spark session
spark=SparkSession.builder \
    .appName("EcomBatchProcessing") \
    .master("local[*]") \
    .getOrCreate() 

#read data from CSV files

df_users = spark.read.csv("/app/batch_processing/data/users.csv", header=True)
df_products=spark.read.csv("batch_processing/data/products.csv",header=True)
df_orders=spark.read.csv("batch_processing/data/orders.csv",header=True)

# ✅ Transformations
df_orders= df_orders.withColumn("order_date",to_date(col("timestamp")))

#df_time transformation
df_time=df_orders.select(
    col("timestamp"),
    hour(col("timestamp")).alias("hour"),
    dayofmonth(col("timestamp")).alias("day"),
    month(col("timestamp")).alias("month"), 
    year(col("timestamp")).alias("year")
).distinct()

def write_to_postgres(df,table_name):
    df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/aiflow") \
    .option("dbtable", table_name) \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

write_to_postgres(df_users, "dim_users")
write_to_postgres(df_products, "dim_products")
write_to_postgres(df_time, "dim_time")
write_to_postgres(df_orders.drop("order_date"), "fact_orders")
    
print("ETL job completed successfully.")

# ✅ Stop the Spark session
spark.stop()