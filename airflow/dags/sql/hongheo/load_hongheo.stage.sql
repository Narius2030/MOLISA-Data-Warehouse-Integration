-- CREATE EXTENSION dblink;

/*** Load stage for dimensions ***/

DO
$$
DECLARE
	vinformation VARCHAR(255);
	vfinished_at TIMESTAMP;
	vstatus VARCHAR(10);
BEGIN
    /***
        Load Source to stgDimFamily (Bronze)
    **/
    INSERT INTO "stgDimFamily"(family_id, family_code, family_type, years, 
                            province_code, province_name, district_code,
                            district_name, ward_code, ward_name, 
                            family_number, nation_in_place)
    SELECT 
        family_id, family_code, family_type, years,
		province_code, province_name, district_code,
        district_name, ward_code, ward_name, 
        family_number, nation_in_place
    FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgdimfamily') 
    AS (
        family_id uuid,
        family_code VARCHAR(10),
        family_type VARCHAR(25),
        years SMALLINT,
        province_code CHAR(5),
        province_name VARCHAR(35),
        district_code CHAR(5),
        district_name VARCHAR(35),
        ward_code CHAR(5),
        ward_name VARCHAR(35),
        family_number VARCHAR(20),
        nation_in_place BOOL,
        created_date TIMESTAMP
    )
    WHERE created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS');

    /***
        Load Source to stgDimFamilyMember (Bronze)
    ***/
    INSERT INTO "stgDimFamilyMember"(member_id, family_id, full_name, owner_relationship, 
                                    year_of_birth, month_of_birth, day_of_birth, 
                                    identity_card_number, nation, sex, height, weight,
                                    education_status, education_level, culture_level, training_level, has_medical_insurance,
                                    social_assistance, has_job, job_type, has_contract, has_pension)
    SELECT
        member_id, family_id, full_name, owner_relationship, 
        year_of_birth, month_of_birth, day_of_birth, 
        identity_card_number, nation, sex, height, weight,
        education_status, education_level, culture_level, training_level, has_medical_insurance,
        social_assistance, has_job, job_type, has_contract, has_pension
    FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434',
                'select * from public.vw_stgdimfamilymember')
    AS (
        member_id uuid,
        family_id uuid,
        full_name VARCHAR(35),
        owner_relationship VARCHAR(15),
        year_of_birth SMALLINT,
        month_of_birth SMALLINT,
        day_of_birth SMALLINT,
        identity_card_number VARCHAR(12),
        nation VARCHAR(15),
        sex BOOL,
        height INT,
        weight INT,
        education_status BOOL,
        education_level public.EDU_LEVEL,
        culture_level public.CUL_LEVEL,
        training_level public.TRAIN_LEVEL,
        has_medical_insurance BOOL,
        social_assistance public.TCXH,
        has_job public.JOB_STATUS,
        job_type public.JOB_CATE,
        has_contract public.CONTRACT_TYPE,
        has_pension public.PENSION_TYPE,
        created_date TIMESTAMP
    )
    WHERE created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS');


    /***
        Load Source to stgDimFamilyMember (Bronze)
    ***/
    INSERT INTO "stgDimSurvey"(family_id, year, month,
						   a_id, fast_classify_person,
						   condition_codes, condition_names, b1_id, is_aquaculture, 
						   electricity_source, water_source, reason_names, get_policy_names,
						   need_policy_names, a_grade, b1_grade, b2_grade,
						   final_result, classify_person)
    SELECT
        family_id, year, month,
        a_id, fast_classify_person,
        condition_codes, condition_names, b1_id, is_aquaculture, 
        electricity_source, water_source, reason_names, get_policy_names,
        need_policy_names, a_grade, b1_grade, b2_grade,
        final_result, classify_person
    FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgdimsurvey')
    AS (
        family_id uuid,
        a_id uuid,
        fast_classify_person VARCHAR(35),
        year INT,
        month INT,
        condition_codes CHAR(5)[],
        condition_names VARCHAR(255)[],
        b1_id uuid,
        is_aquaculture BOOL,
        electricity_source CHAR(5),
        water_source CHAR(5),
        reason_names VARCHAR(255)[],
        get_policy_names VARCHAR(255)[],
        need_policy_names VARCHAR(255)[],
        a_grade BOOL,
        b1_grade SMALLINT,
        b2_grade SMALLINT,
        final_result public.CLASSIFICATION,
        classify_person VARCHAR(35),
        a_created_date TIMESTAMP,
        b1_created_date TIMESTAMP,
        rs_created_date TIMESTAMP
    )
    WHERE (a_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (b1_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (rs_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));
    
    -- UPDATE "DimAuditForeigned" 
    -- SET 
    --     information = 'Hongheo Dimension: integrating succefully',
    --     status = 'PENDING',
    --     finished_at = NOW()
    -- WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned" WHERE status IS NULL);

EXCEPTION
-- 	RAISE EXCEPTION 'Something went wrong with selected columns' USING HINT = 'Check on "SELECT" or "INSERT" statement';
	WHEN OTHERS THEN
		UPDATE "DimAuditForeigned" 
		SET 
			information = 'Hongheo Dimension: Something went wrong on running statements. 
							HINT: Check carefully the syntax or selected columns at "SELECT" or "INSERT" clauses',
			status = 'ERROR',
			finished_at = NOW()
		WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned" WHERE status='ERROR');

END;
$$;


/*** Load stage for facts ***/

DO
$$
BEGIN
    /***
        Load Source to stgPovertyStatusFact (Bronze)
    ***/
    INSERT INTO "stgPovertyStatusFact"(family_id, year, province_name, district_name,
                                    family_code, owner_name, hard_reasons, 
                                    get_policies, need_policies, 
                                    a_grade, b1_grade, b2_grade, final_result)
    SELECT
        family_id, year, province_name, district_name,
        family_code, owner_name, hard_reasons, 
        get_policies, need_policies, 
        a_grade, b1_grade, b2_grade, final_result
    FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgpovertystatusfact')
    AS (
        family_id uuid,
        year SMALLINT,
        province_name VARCHAR(35),
        district_name VARCHAR(35),
        family_code VARCHAR(10),
        owner_name VARCHAR(35),
        hard_reasons VARCHAR(255)[],
        get_policies VARCHAR(255)[],
        need_policies VARCHAR(255)[],
        a_grade BOOL,
        b1_grade SMALLINT,
        b2_grade SMALLINT,
        final_result public.CLASSIFICATION,
        a_created_date TIMESTAMP,
        b1_created_date TIMESTAMP,
        rs_created_date TIMESTAMP
    )
    WHERE (a_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (b1_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (rs_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));



    /***
        Load Source to stgMemberSurveyFact (Bronze)
    ***/
    INSERT INTO "stgMemberSurveyFact"(member_id, family_id, year, month,
                                    province_name, district_name,
                                    member_name, owner_relationship,
                                    year_of_birth, month_of_birth, day_of_birth,
                                    identity_card_number, nation, final_result)
    SELECT
        member_id, family_id, year, month,
        province_name, district_name,
        full_name, owner_relationship,
        year_of_birth, month_of_birth, day_of_birth,
        identity_card_number, nation, final_result
    FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgmembersurveyfact')
    AS (
        member_id uuid,
        family_id uuid,
        year INT,
        month INT,
        province_name VARCHAR(35),
        district_name VARCHAR(35),
        full_name VARCHAR(35),
        owner_relationship VARCHAR(15),
        year_of_birth SMALLINT,
        month_of_birth SMALLINT,
        day_of_birth SMALLINT,
        identity_card_number VARCHAR(12),
        nation VARCHAR(15),
        final_result public.CLASSIFICATION,
        member_created_date TIMESTAMP,
        rs_created_date TIMESTAMP
    )
    WHERE (member_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (rs_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));

EXCEPTION
	WHEN OTHERS THEN
		UPDATE "DimAuditForeigned" 
		SET 
			information = 'Hongheo Fact: Something went wrong on running statements. 
							HINT: Check carefully the syntax or selected columns at "SELECT" or "INSERT" clauses',
			status = 'ERROR',
			finished_at = NOW()
		WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned" WHERE status='ERROR');

END;
$$;