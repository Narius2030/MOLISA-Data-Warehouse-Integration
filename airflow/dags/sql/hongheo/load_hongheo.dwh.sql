/*** Hộ Nghèo ***/

BEGIN;
	/***
        Load Source to stgDimFamily (Bronze)
    ***/
	BEGIN;
		/***
			Load DimFamily to DWH with SCD Type 2
		***/
		UPDATE hongheo."DimFamily"
		SET 
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimFamily"') 
		AS stgdimfamily(
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
			nation_in_place BOOL
		)
		WHERE (stgdimfamily.family_id = "DimFamily".family_id) AND ("DimFamily".rowenddate IS NULL) 
			AND	((stgdimfamily.province_code <> "DimFamily".province_code)
				OR (stgdimfamily.province_name <> "DimFamily".province_name)
				OR (stgdimfamily.district_code <> "DimFamily".district_code)
				OR (stgdimfamily.district_name <> "DimFamily".district_name) 
				OR (stgdimfamily.family_number <> "DimFamily".family_number)
				OR (stgdimfamily.nation_in_place <> "DimFamily".nation_in_place));
		
		/***
			Insert into DWH with SCD Type 2
		***/
		INSERT INTO hongheo."DimFamily"(family_id, family_code, family_type, years, 
								province_code, province_name, district_code,
								district_name, ward_code, ward_name,
								family_number, nation_in_place, rowiscurrent, rowstartdate, rowenddate)
		SELECT 
			family_id, family_code, family_type, years,
			province_code, province_name, district_code,
			district_name, ward_code, ward_name,
			family_number, nation_in_place,
			'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434',
					'select * from public."stgDimFamily"') 
		AS stgdimfamily (
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
			nation_in_place BOOL
		)
		WHERE (family_id, province_code, district_code, 
			   province_name, district_name,
			   family_number, nation_in_place) NOT IN (SELECT family_id, province_code, district_code, 
													  		province_name, district_name,
														   	family_number, nation_in_place
													 FROM hongheo."DimFamily");
	END;
	
	
	/***
        Load DimFamilyMember to DimFamilyMember SCD Type 1
    ***/
	BEGIN;
		/***
			Update DimFamilyMember from stgDimFamilyMember (SCD Type 1)
		***/
		UPDATE hongheo."DimFamilyMember"
		SET
			education_status = stgmember.education_status, 
			education_level = stgmember.education_level, 
			culture_level = stgmember.culture_level,
			training_level = stgmember.training_level, 
			has_medical_insurance = stgmember.has_medical_insurance,
			has_job=stgmember.has_job, job_type=stgmember.job_type, 
			has_contract=stgmember.has_contract
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434',
					'select * from public."stgDimFamilyMember"')
		AS stgmember(
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
			education_level hongheo.EDU_LEVEL,
			culture_level hongheo.CUL_LEVEL,
			training_level hongheo.TRAIN_LEVEL,
			has_medical_insurance BOOL,
			social_assistance hongheo.TCXH,
			has_job hongheo.JOB_STATUS,
			job_type hongheo.JOB_CATE,
			has_contract hongheo.CONTRACT_TYPE,
			has_pension hongheo.PENSION_TYPE
		)
		WHERE (stgmember.member_id = "DimFamilyMember".member_id) AND
			(("DimFamilyMember".education_status <> stgmember.education_status) 
			OR ("DimFamilyMember".education_level <> stgmember.education_level) 
			OR ("DimFamilyMember".culture_level <> stgmember.culture_level)
			OR ("DimFamilyMember".training_level <> stgmember.training_level)
			OR ("DimFamilyMember".has_medical_insurance <> stgmember.has_medical_insurance)
			OR ("DimFamilyMember".has_job <> stgmember.has_job)
			OR ("DimFamilyMember".job_type <> stgmember.job_type)
			OR ("DimFamilyMember".has_contract <> stgmember.has_contract));
			
		/***
			Insert DimFamilyMember from stgDimFamilyMember (SCD Type 1)
		***/
		INSERT INTO hongheo."DimFamilyMember"(member_id, family_id, full_name, owner_relationship, 
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
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434',
					'select * from public."stgDimFamilyMember"')
		AS stgmember(
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
			education_level hongheo.EDU_LEVEL,
			culture_level hongheo.CUL_LEVEL,
			training_level hongheo.TRAIN_LEVEL,
			has_medical_insurance BOOL,
			social_assistance hongheo.TCXH,
			has_job hongheo.JOB_STATUS,
			job_type hongheo.JOB_CATE,
			has_contract hongheo.CONTRACT_TYPE,
			has_pension hongheo.PENSION_TYPE
		)
		WHERE (stgmember.member_id) NOT IN (SELECT member_id FROM hongheo."DimFamilyMember");
	END;
	
	/***
        Load stgDimSurvey to DimFamilyMember SCD Type 2
    ***/
	BEGIN;
		/***
			Update DimSurvey from stgDimSurvey (SCD Type 2)
		***/
		UPDATE hongheo."DimSurvey"
		SET 
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimSurvey"') 
		AS stgsurvey(
			family_id uuid,
			year INT,
			month INT,
			a_id uuid,
			fast_classify_person VARCHAR(35),
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
			final_result hongheo.CLASSIFICATION,
			classify_person VARCHAR(35)
		)
		WHERE ((stgsurvey.family_id = "DimSurvey".family_id)
			AND ("DimSurvey".rowenddate IS NULL)) AND
			((stgsurvey.a_grade <> "DimSurvey".a_grade)
			OR (stgsurvey.b1_grade <> "DimSurvey".b1_grade)
			OR (stgsurvey.b2_grade <> "DimSurvey".b2_grade)
			OR (stgsurvey.final_result <> "DimSurvey".final_result));
			
		/***
			Insert DimSurvey from stgDimSurvey (SCD Type 2)
		***/
		INSERT INTO hongheo."DimSurvey"(family_id, year, month, a_id, fast_classify_person,
										condition_codes, condition_names, b1_id, is_aquaculture, 
										electricity_source, water_source, reason_names, get_policy_names,
										need_policy_names, a_grade, b1_grade, b2_grade,
										final_result, classify_person, 
										rowiscurrent, rowstartdate, rowenddate)
		SELECT 
			family_id, year, month, a_id, fast_classify_person,
			condition_codes, condition_names, b1_id, is_aquaculture, 
			electricity_source, water_source, reason_names, get_policy_names,
			need_policy_names, a_grade, b1_grade, b2_grade,
			final_result, classify_person,
			'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434',
					'select * from public."stgDimSurvey"') 
		AS stgsurvey (
			family_id uuid,
			year INT,
			month INT,
			a_id uuid,
			fast_classify_person VARCHAR(35),
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
			final_result hongheo.CLASSIFICATION,
			classify_person VARCHAR(35)
		)
		WHERE (family_id, a_grade, b1_grade, b2_grade, final_result) NOT IN (SELECT family_id, a_grade, b1_grade, 
																			 	b2_grade, final_result
																			FROM hongheo."DimSurvey");
	END;
END;



----------- FACT -----------

BEGIN;
	/*** 
		Load Stage to PovertyStatusFact
	***/
	BEGIN;
		INSERT INTO hongheo."PovertyStatusFact"(familykey, surveykey, family_id, year, province_name, district_name,
											family_code, owner_name, hard_reasons, get_policies, need_policies, member_num,
											a_grade, b1_grade, b2_grade, b1_diff, b2_diff, final_result)
		SELECT
			(SELECT familykey FROM hongheo."DimFamily" WHERE family_id=stgpovertyfact.family_id AND rowiscurrent='TRUE') AS familykey,
			(SELECT surveykey FROM hongheo."DimSurvey" WHERE family_id=stgpovertyfact.family_id AND rowiscurrent='TRUE') AS surveykey,
			*
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgPovertyStatusFact"')
		AS stgpovertyfact (
			family_id uuid,
			year SMALLINT,
			province_name VARCHAR(35),
			district_name VARCHAR(35),
			family_code VARCHAR(10),
			owner_name VARCHAR(35),
			hard_reasons VARCHAR(255)[],
			get_policies VARCHAR(255)[],
			need_policies VARCHAR(255)[],
			member_num SMALLINT,
			a_grade BOOL,
			b1_grade SMALLINT,
			b2_grade SMALLINT,
			b1_diff SMALLINT,
			b2_diff SMALLINT,
			final_result hongheo.CLASSIFICATION
		) WHERE ((SELECT familykey FROM hongheo."DimFamily" WHERE family_id=stgpovertyfact.family_id AND rowiscurrent='TRUE'),
				 (SELECT surveykey FROM hongheo."DimSurvey" WHERE family_id=stgpovertyfact.family_id AND rowiscurrent='TRUE'),
				 year) NOT IN(SELECT familykey, surveykey, year FROM hongheo."PovertyStatusFact");
	END;
	
	
	/*** 
		Load Stage to MemberSurveyFact
	***/
	BEGIN;
		INSERT INTO hongheo."MemberSurveyFact"(familykey, memberkey, datekey, isdeleted, member_id, family_id, year, month,
											   province_name, district_name, member_name, owner_relationship, 
											   year_of_birth, month_of_birth, day_of_birth, age, identity_card_number, nation, final_result)
		SELECT
			(SELECT familykey FROM hongheo."DimFamily" WHERE family_id=stgmember.family_id AND rowiscurrent='TRUE') AS familykey,
			(SELECT memberkey FROM hongheo."DimFamilyMember" WHERE member_id=stgmember.member_id) AS memberkey,
			(year*100 + month) AS datekey, 'FALSE',
			*
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgMemberSurveyFact"')
		AS stgmember (
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
			age SMALLINT,
			identity_card_number VARCHAR(12),
			nation VARCHAR(15),
			final_result hongheo.CLASSIFICATION
		)  WHERE ((SELECT familykey FROM hongheo."DimFamily" WHERE family_id=stgmember.family_id AND rowiscurrent='TRUE'),
				 (SELECT memberkey FROM hongheo."DimFamilyMember" WHERE member_id=stgmember.member_id),
				 (year*100 + month)) NOT IN(SELECT familykey, memberkey, datekey FROM hongheo."MemberSurveyFact");
	END;
END;