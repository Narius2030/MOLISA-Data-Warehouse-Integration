from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import psycopg2 
import json


def read_time_excel():
        time_df = pd.read_excel('./data/SampleDateDim.xls', sheet_name='LoadDates')
        
        print('==========================')
        print(time_df.columns)
        print(time_df.head())
        print('==========================')
        # Connect to PostgreSQL
        conn = psycopg2.connect(
                database="LdtbxhStage",
                user="postgres",
                password="nhanbui",
                host="host.docker.internal",
                port="5434"
        )
        cur = conn.cursor()
        # Insert data into PostgreSQL
        print('start to insert...')
        try:
                for row in time_df.to_numpy():
                        cols = ','.join(list(time_df.columns))
                        # print(type(row[1]))
                        for idx, val in enumerate(row):
                                if isinstance(val, pd.Timestamp):
                                        row[idx] = str(val)
                        cur.execute(f'''INSERT INTO "stgDate"({cols}) VALUES {tuple(row)}''')
                print('inserted sucessfully!')
        except Exception as exc:
                print(str(exc))
                
        conn.commit()
        cur.close()
        conn.close()


def load_nd_mart():
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

        try:
                dimdate.write.format("jdbc") \
                        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/NguoiDanDM") \
                        .option("driver", f"{config['DRIVER']}") \
                        .option("dbtable", 'public."DimDate"') \
                        .option("user", f"{config['USER']}") \
                        .option("password", f"{config['PASSWORD']}") \
                        .mode('append') \
                        .save()
        except Exception as exc:
                print(str(exc))
        
        spark.stop()
        
if __name__=='__main__':
        load_nd_mart()