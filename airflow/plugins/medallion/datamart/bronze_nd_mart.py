from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import date
import psycopg2 
import json


def run_time():
        with open("/opt/airflow/config.json", "r") as file:
                config = json.load(file)
        
        spark = SparkSession.builder.appName("Test connect to Postgresql") \
                .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
                .getOrCreate()
                
        dimdate = spark.read.format("jdbc") \
                .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhDWH") \
                .option("driver", f"{config['DRIVER']}") \
                .option("dbtable", 'public."DimDate"') \
                .option("user", f"{config['USER']}") \
                .option("password", f"{config['PASSWORD']}") \
                .load()

        dimdate.write.format("jdbc") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/NguoiDanDM") \
        .option("driver", f"{config['DRIVER']}") \
        .option("dbtable", 'public."DimDate"') \
        .option("user", f"{config['USER']}") \
        .option("password", f"{config['PASSWORD']}") \
        .mode('append') \
        .save()
    