from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("PythonWordCount") \
                    .master("spark://localhost:7077") \
                    .config("spark.shuffle.service.enabled", "false") \
                    .config("spark.dynamicAllocation.enabled", "false") \
                    .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
                    .getOrCreate()

dimFamilyMember = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5434/LdtbxhDWH") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'hongheo."DimFamilyMember"') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()

print(dimFamilyMember.printSchema())

spark.stop()