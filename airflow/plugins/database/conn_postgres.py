from pyspark.sql import SparkSession

def spark_postgres():
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()
        
    postgredf = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/hongheovna") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "locations.province") \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()
       
    print("============================")
    print(postgredf.printSchema())
    print(postgredf.show(5))
    print("============================")
    
    spark.stop()