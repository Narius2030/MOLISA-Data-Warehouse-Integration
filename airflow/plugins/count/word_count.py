from pyspark.sql import SparkSession


def spark_wordcount():
    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

    words = spark.sparkContext.parallelize(text.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    print("============================")

    for wc in wordCounts.collect():
        print(wc[0], wc[1])
    # print(wordCounts)

    print("============================")

    spark.stop()