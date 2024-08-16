from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, lit, year, month, count
from datetime import date
import json
import psycopg2 


def join_ncc_hongheo(dimSurvey, dimFamilyMember, dimNCC, dimSubsidy):
    hongheo_df = dimSurvey.join(dimFamilyMember, on="family_id", how="right").select('member_id','full_name','identity_card_number','family_id',
                                                                                    'a_grade','b1_grade','b2_grade','final_result')

    dimNCC_tmp = dimNCC.where("rowiscurrent=True")
    dimSubsidy_tmp = dimSubsidy.where("rowiscurrent=True")
    ncc_df = dimNCC_tmp.join(dimSubsidy_tmp, on="profile_code", how="left").select('profile_code',dimNCC_tmp.ncc_code,dimNCC.full_name,'identity_number',
                                                                                   'subsidy_code','year','spend_type','subsidy_name','subsidy_money','submoney','recieve_date')

    final_df = ncc_df.join(hongheo_df, hongheo_df.identity_card_number==ncc_df.identity_number, how="left") \
                     .select('profile_code','ncc_code',ncc_df.full_name,'identity_number',
                             'subsidy_code','year','subsidy_money','recieve_date',
                             'member_id','family_id','a_grade','b1_grade','b2_grade','final_result')
    return final_df


def find_keys(spark, config, finalfact_df, businesskeys:dict): 
    dimMember = spark.read.format("jdbc") \
                    .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/NguoiDanDM") \
                    .option("driver", f"{config['DRIVER']}") \
                    .option("dbtable", 'public."DimFamilyMember"') \
                    .option("user", f"{config['USER']}") \
                    .option("password", f"{config['PASSWORD']}") \
                    .load()

    dimSurvey = spark.read.format("jdbc") \
                    .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/NguoiDanDM") \
                    .option("driver", f"{config['DRIVER']}") \
                    .option("dbtable", 'public."DimSurvey"') \
                    .option("user", f"{config['USER']}") \
                    .option("password", f"{config['PASSWORD']}") \
                    .load()

    dimNCC = spark.read.format("jdbc") \
                    .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/NguoiDanDM") \
                    .option("driver", f"{config['DRIVER']}") \
                    .option("dbtable", 'public."DimNCC"') \
                    .option("user", f"{config['USER']}") \
                    .option("password", f"{config['PASSWORD']}") \
                    .load()
    
    
    # Filter dimNCC based on businesskeys and rowiscurrent
    key_dimNCC = dimNCC.where((dimNCC.profile_code.isin(businesskeys['ncc'])) & (dimNCC.rowiscurrent == True)) \
                            .select('profile_code', 'profilekey')
    key_dimMember = dimMember.where(dimMember.member_id.isin(businesskeys['member'])) \
                            .select('member_id', 'memberkey')
    key_dimSurvey = dimSurvey.where((dimSurvey.family_id.isin(businesskeys['survey'])) & (dimSurvey.rowiscurrent == True)) \
                            .select('family_id', 'surveykey')
            
    # Join finalfact_df with filtered_dimNCC on profile_code
    joined_df = finalfact_df.join(key_dimNCC, on='profile_code', how='left')
    joined_df = joined_df.join(key_dimMember, on='member_id', how='left')
    joined_df = joined_df.join(key_dimSurvey, on='family_id', how='left')
    
    # Select all columns and rename the joined profilekey column (optional)
    joined_df = joined_df.withColumn('year', year('recieve_date'))
    joined_df = joined_df.withColumn('month', month('recieve_date'))
    final_df = joined_df.withColumn('datekey', lit(year('recieve_date').cast('integer')*100 + month('recieve_date').cast('string')))
    
    return final_df


def value_of_subsidy(fact_df):
    temp_df = fact_df.select('profilekey','memberkey','surveykey','datekey','year','month',
                              'profile_code','ncc_code',fact_df.full_name,'identity_number',
                              'member_id','family_id','a_grade','b1_grade','b2_grade','final_result')
    
    # Count the total subsidy of each NCC
    grouped_df = temp_df.groupBy(['year','month','identity_number']).count()
    joined_df = temp_df.join(grouped_df, on=['identity_number','year','month'], how='left')
    count_df = joined_df.withColumn('total_subsidy', joined_df['count'])
    final_df = count_df.drop("count")
    
    # Sum the value of subsidy for each NCC
    grouped_df = fact_df.groupBy(['year','month','identity_number']).sum('subsidy_money')
    joined_df = temp_df.join(grouped_df, on=['identity_number','year','month'], how='right')
    sum_df = joined_df.withColumn('total_money', joined_df['sum(subsidy_money)']).drop('sum(subsidy_money)')
    
    # Combine
    final_df = final_df.join(sum_df, on='profilekey', how='left').select([final_df.profilekey,final_df.memberkey,final_df.surveykey,final_df.datekey,final_df.year,final_df.month,
                                                                           final_df.profile_code,final_df.ncc_code,final_df.full_name,final_df.identity_number,
                                                                           final_df.member_id,final_df.family_id,final_df.a_grade,final_df.b1_grade,final_df.b2_grade,final_df.final_result,
                                                                           'total_subsidy','total_money'])
    
    return final_df.distinct()


def run_gold_nd():
    with open("/opt/airflow/config.json", "r") as file:
            config = json.load(file)
        
    spark = SparkSession.builder.appName("Test connect to Postgresql") \
            .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
            .getOrCreate()
            
    dimFamilyMember = spark.read.format("jdbc") \
            .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhDWH") \
            .option("driver", f"{config['DRIVER']}") \
            .option("dbtable", 'hongheo."DimFamilyMember"') \
            .option("user", f"{config['USER']}") \
            .option("password", f"{config['PASSWORD']}") \
            .load()

    dimSurvey = spark.read.format("jdbc") \
            .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhDWH") \
            .option("driver", f"{config['DRIVER']}") \
            .option("dbtable", 'hongheo."DimSurvey"') \
            .option("user", f"{config['USER']}") \
            .option("password", f"{config['PASSWORD']}") \
            .load()

    dimNCC = spark.read.format("jdbc") \
            .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhDWH") \
            .option("driver", f"{config['DRIVER']}") \
            .option("dbtable", 'ncc."DimNCC"') \
            .option("user", f"{config['USER']}") \
            .option("password", f"{config['PASSWORD']}") \
            .load()
            
    dimSubsidy = spark.read.format("jdbc") \
            .option("url", f"{config['URL_BASE_DOCKER']}:{config['PORT']}/LdtbxhDWH") \
            .option("driver", f"{config['DRIVER']}") \
            .option("dbtable", 'ncc."DimSubsidy"') \
            .option("user", f"{config['USER']}") \
            .option("password", f"{config['PASSWORD']}") \
            .load()

    finalfact_df = join_ncc_hongheo(dimSurvey, dimFamilyMember, dimNCC, dimSubsidy)

    businesskeys = {
        'ncc': list(finalfact_df.select('profile_code').toPandas()['profile_code']),
        'survey': list(finalfact_df.select('family_id').toPandas()['family_id']),
        'member': list(finalfact_df.select('member_id').toPandas()['member_id']),
        'subsidy': {
            'year': finalfact_df.year,
            'code': finalfact_df.subsidy_code
        }
    }

    finalfact_df = find_keys(spark, config, finalfact_df, businesskeys=businesskeys)
    finalfact_df = value_of_subsidy(finalfact_df)

    for col in finalfact_df.dtypes:
        if col[0] in ['profilekey','memberkey','surveykey','member_id','family_id']:
            finalfact_df = finalfact_df.fillna('00000000-0000-0000-0000-000000000000', subset=[col[0]])
        elif col[1] == 'string':
            finalfact_df = finalfact_df.fillna('N/A', subset=[col[0]])
        elif col[1] == 'boolean':
            finalfact_df = finalfact_df.fillna(False, subset=[col[0]])
        else:
            finalfact_df = finalfact_df.fillna(-1, subset=[col[0]])

    print("====================")
    print(finalfact_df.show(5))
    print(finalfact_df.dtypes)
    print("====================")

    with psycopg2.connect(
        database="NguoiDanDM",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_DOCKER']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            for row in finalfact_df.collect():
                cur.execute(f"""
                                INSERT INTO "NccPovertyFact"(profilekey,memberkey,surveykey,datekey,year,month,
                                                        profile_code,ncc_code,full_name,identity_card,
                                                        member_id,family_id,a_grade,b1_grade,b2_grade,final_result,
                                                        total_subsidy,total_money)
                                VALUES('{row['profilekey']}','{row['memberkey']}','{row['surveykey']}',{row['datekey']},{row['year']},{row['month']},
                                        '{row['profile_code']}','{row['ncc_code']}','{row['full_name']}','{row['identity_number']}','{row['member_id']}',
                                        '{row['family_id']}',{row['a_grade']},{row['b1_grade']},{row['b2_grade']},'{row['final_result']}',
                                        {row['total_subsidy']},{row['total_money']})
                            """)