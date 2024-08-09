from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date
import json
import psycopg2 

def find_age_member(memberSurveyFact_df):
    final_df = memberSurveyFact_df.withColumn('age', date.today().year - col('year_of_birth'))
    
    return final_df


def run_silver_hongheo():
    with open("/opt/airflow/config.json", "r") as file:
        config = json.load(file)
    
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
            .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
            .getOrCreate()
        
    memberSurveyFact_df = spark.read.format("jdbc") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhStage") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public."stgMemberSurveyFact"') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()
    
    finalfact_df = find_age_member(memberSurveyFact_df)
    
    print("====================")
    print(finalfact_df.show(5))
    print("====================")
    
    with psycopg2.connect(
        database="LdtbxhStage",
        user="postgres",
        password="nhanbui",
        host="host.docker.internal",
        port="5434"
    ) as conn:
        with conn.cursor() as cur:
            for row in finalfact_df.collect():
                cur.execute(f"""UPDATE public."stgMemberSurveyFact"
                                SET age={row['age']}
                                WHERE member_id='{row['member_id']}'""")
    
    spark.stop()
    
if __name__ == '__main__':
    print("")