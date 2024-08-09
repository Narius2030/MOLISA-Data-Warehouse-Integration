from pyspark.sql import SparkSession

# host.docker.internal
def spark_postgres():
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()
        
    postgredf = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5434/LdtbxhStage") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public."stgPovertyStatusFact"') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()
       
    print("============================")
    print(postgredf.printSchema())
    print(postgredf.show(5))
    print("============================")
    
    spark.stop()
    
if __name__ == '__main__':
    spark_postgres()