from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag
import json


def grade_difference(povertyfact_df):
    # create window for ordered dataframe
    # create previous-value columns by "lag" function
    windowSpec = Window.partitionBy("family_code").orderBy(["family_code", "year", "b1_grade", "b2_grade"])
    temp_b1 = lag("b1_grade").over(windowSpec).cast("integer")
    temp_b2 = lag("b2_grade").over(windowSpec).cast("integer")

    # Calculate the difference between current and previous b1_grade, b2_grade
    df = povertyfact_df.withColumn("b1_diff", col('b1_grade') - temp_b1)
    df = df.withColumn("b2_diff", col('b2_grade') - temp_b2)
    return df

def count_member(povertyfact_df, member_df):
    # create a dataframe for number of member each family_id
    count_df = member_df.groupBy("family_id").count()

    # join povertyfact to count_df and assign "count" value to "member_num" -> drop "count" column at the end
    joined_df = povertyfact_df.join(count_df, on="family_id", how="left")
    updated_df = joined_df.withColumn("member_num", joined_df["count"])
    final_df = updated_df.drop("count")
    return final_df

def run_gold_hongheo():
    with open("/opt/airflow/config.json", "r") as file:
        config = json.load(file)
    
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()
        
    povertyfact_df = spark.read.format("jdbc") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhStage") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public."stgPovertyStatusFact"') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()
        
    member_df = spark.read.format("jdbc") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/hongheovna") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public.family_member_info') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .load()

    temp_df = grade_difference(povertyfact_df)
    finalfact_df = count_member(temp_df, member_df)
    
    print("====================")
    print(finalfact_df.show(5))
    print("====================")

    (finalfact_df.write.format("jdbc").mode("overwrite") \
        .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhStage") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public."stgPovertyStatusFact"') \
        .option("user", "postgres") \
        .option("password", "nhanbui") \
        .save())

    spark.stop()
    
if __name__ == "__main__":
    with open("./airflow/config.json", "r") as file:
        config = json.load(file)