from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test connect to Postgresql") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()
        
postgredf = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://127.0.0.1:5432/hongheovna") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "locations.province") \
    .option("user", "postgres") \
    .option("password", "nhanbui") \
    .load()
    
print("============================")
print(postgredf.printSchema())
print(postgredf.show(5))
print("============================")