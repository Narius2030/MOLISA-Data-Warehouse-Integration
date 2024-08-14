from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
import json
import psycopg2



def ncc_difference(stgSubsidyReportFact_df):
    df = stgSubsidyReportFact_df.withColumn("recieve_days", datediff(col("recieve_date"),col("start_subsidize")))
    final_df = df.withColumn("spend_diff", col("actual_spending")-col("subsidy_money"))
    return final_df

def run_silver_ncc():
    with open("/opt/airflow/config.json", "r") as file:
        config = json.load(file)
    
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
            .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
            .getOrCreate()
        
    stgSubsidyReportFact_df = spark.read.format("jdbc") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhStage") \
        .option("driver", f"{config['DRIVER']}") \
        .option("dbtable", 'public."stgSubsidyReportFact"') \
        .option("user", f"{config['USER']}") \
        .option("password", f"{config['PASSWORD']}") \
        .load()
    
    finalfact_df = ncc_difference(stgSubsidyReportFact_df)

    print("====================")
    print(finalfact_df.show(5))
    print("====================")
            

    with psycopg2.connect(
       database = "LdtbxhStage",
        user = f"{config['USER']}",
        password = f"{config['PASSWORD']}",
        host = f"{config['HOST_DOCKER']}",
        port = f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            for row in finalfact_df.collect():
                cur.execute(f"""UPDATE public."stgSubsidyReportFact"
                                SET 
                                    recieve_days={row['recieve_days']},
                                    spend_diff={row['spend_diff']}
                                WHERE profile_code='{row['profile_code']}' 
                                    AND subsidy_code='{row['subsidy_code']}'
                                    AND year={row['year']}""")
    
    spark.stop()
    
    
if __name__ == '__main__':
    run_silver_ncc()