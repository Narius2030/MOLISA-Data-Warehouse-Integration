import requests
import json
import psycopg2

with open("/opt/airflow/config.json", "r") as file:
        config = json.load(file)


def check_time(value, latest_sync):
    for k, v in value.items():
        if (k!= 'data' and v is not None) and v >= latest_sync:
            return True
    return False


def insert_postgres(insert_stmt, values):
    with psycopg2.connect(
        database="LdtbxhStage",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_DOCKER']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'""")
            latest_sync = cur.fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')
            for value in values:
                if check_time(value, latest_sync):
                    try:
                        cur.execute(insert_stmt, value['data'])
                    except Exception as exc:
                        cur.execute("ROLLBACK")
                        print(str(exc))

def loadStageSurvey(insert_stmt, endpoint, headers:str=None):
    req = requests.get(endpoint, headers=headers)
    if req.status_code == 200:
        families = req.json()['data']
        values = [{"a_created_date": row['a_created_date'], "b1_created_date":row['b1_created_date'], 
                   "rs_created_date":row['rs_created_date'], "data":tuple(row.values())[:-3]} for row in families]
        insert_postgres(insert_stmt, values)


def loadStageFamily(insert_stmt, endpoint, headers:str=None):
    req = requests.get(endpoint, headers=headers)
    if req.status_code == 200:
        families = req.json()['data']
        values = [{"created_date": row['created_date'], "data":tuple(row.values())[:-1]} for row in families]
        insert_postgres(insert_stmt, values)
        

def loadStageMemberSurvey(insert_stmt, endpoint, headers:str=None):
    req = requests.get(endpoint, headers=headers)
    if req.status_code == 200:
        families = req.json()['data']
        values = [{"member_created_date": row['member_created_date'], 
                   "rs_created_date": row['rs_created_date'], "data":tuple(row.values())[:-2]} for row in families]
        insert_postgres(insert_stmt, values)


def run_bronze_hongheo():
        bearer_token = config['BEARER_TOKEN']
        headers = {
            "Authorization": f"Bearer {bearer_token}"
        }
        
        # StgDimFamily
        endpoint = f"http://{config['HOST_DOCKER']}:3000/family/stage/dim_family"
        insert_stmt = """
            INSERT INTO "stgDimFamily"(family_id, family_code, family_type, years, 
                                    province_code, province_name, district_code,
                                    district_name, ward_code, ward_name, 
                                    family_number, nation_in_place)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        loadStageFamily(insert_stmt, endpoint, headers)
        
        # StgDimFamilyMember
        endpoint = f"http://{config['HOST_DOCKER']}:3000/family/stage/dim_familymember"
        insert_stmt = """
            INSERT INTO "stgDimFamilyMember"(member_id, family_id, full_name, owner_relationship, 
                                    year_of_birth, month_of_birth, day_of_birth, 
                                    identity_card_number, nation, sex, height, weight,
                                    education_status, education_level, culture_level, training_level, has_medical_insurance,
                                    social_assistance, has_job, job_type, has_contract, has_pension)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        loadStageFamily(insert_stmt, endpoint, headers)
        
        # StgDimSurvey
        endpoint = f"http://{config['HOST_DOCKER']}:3000/family/stage/dim_survey"
        insert_stmt = """
            INSERT INTO "stgDimSurvey"(family_id,  a_id, fast_classify_person, year, month,
                                            condition_codes, condition_names, b1_id, is_aquaculture, 
                                            electricity_source, water_source, reason_names, get_policy_names,
                                            need_policy_names, a_grade, b1_grade, b2_grade,
                                            final_result, classify_person)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        loadStageSurvey(insert_stmt, endpoint, headers)
        
        # StgPovertyStatusFact
        endpoint = f"http://{config['HOST_DOCKER']}:3000/family/stage/poverty_fact"
        insert_stmt = """
            INSERT INTO "stgPovertyStatusFact"(family_id, year, province_name, district_name,
                                            family_code, owner_name, hard_reasons, 
                                            get_policies, need_policies, 
                                            a_grade, b1_grade, b2_grade, final_result)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        loadStageSurvey(insert_stmt, endpoint, headers)
        
        # stgMemberSurveyFact
        endpoint = f"http://{config['HOST_DOCKER']}:3000/family/stage/member_survey_fact"
        insert_stmt = """
            INSERT INTO "stgMemberSurveyFact"(member_id, family_id, year, month, province_name, district_name,
                                            member_name, owner_relationship, year_of_birth, month_of_birth, day_of_birth,
                                            identity_card_number, nation, final_result)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        loadStageMemberSurvey(insert_stmt, endpoint, headers)
        

if __name__=='__main__':
    run_bronze_hongheo()