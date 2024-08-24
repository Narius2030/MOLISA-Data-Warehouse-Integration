BEGIN;
    BEGIN;
        /***
            Update DWH to DimNCC Mart (SCD Type 2)
        ***/
        UPDATE public."DimNCC"
        SET
            rowiscurrent = 'FALSE',
            rowenddate = CURRENT_TIMESTAMP
        FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434', 
                    'select * from ncc."DimNCC"')
        AS stgdimncc (
            profilekey uuid,
            profile_code CHAR(10),
            ncc_code CHAR(10),
            full_name VARCHAR(35),
            birth_of_date DATE,
            sex BOOL,
            ethnic VARCHAR(15),
            identity_number VARCHAR(12),
            identity_place VARCHAR(35),
            home_town VARCHAR(35),
            province_code CHAR(5),
            district_code CHAR(5),
            decided_monthly_date DATE,
            decided_once_date DATE,
            decided_monthly_num CHAR(15),
            decided_once_num CHAR(15),
            start_subsidize DATE,
            support_bhyt BOOL,
            status BOOL,
            RowIsCurrent BOOL,
            RowStartDate TIMESTAMP,
            RowEndDate TIMESTAMP
        ) 
        WHERE (stgdimncc.profile_code = "DimNCC".profile_code AND stgdimncc.RowIsCurrent='t') AND ("DimNCC".RowIsCurrent='t')
            AND((stgdimncc.start_subsidize <> "DimNCC".start_subsidize)
            OR (stgdimncc.support_bhyt <> "DimNCC".support_bhyt)
            OR (stgdimncc.home_town <> "DimNCC".home_town)
            OR (stgdimncc.ncc_code <> "DimNCC".ncc_code));


        /***
            Insert DWH to DimNCC Mart (SCD Type 2)
        ***/
        INSERT INTO "DimNCC"(profile_code, ncc_code, full_name,
                            birth_of_date, sex, ethnic, identity_number,
                            identity_place, home_town, province_code,
                            district_code, decided_monthly_date, decided_once_date,
                            decided_monthly_num, decided_once_num,
                            start_subsidize, support_bhyt, status,
                            rowiscurrent, rowstartdate, rowenddate)
        SELECT
            profile_code, ncc_code, full_name,
            birth_of_date, sex, ethnic, identity_number,
            identity_place, home_town, province_code,
            district_code, decided_monthly_date, decided_once_date,
            decided_monthly_num, decided_once_num,
            start_subsidize, support_bhyt, status,
            'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
        FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434', 
                    'select * from ncc."DimNCC"')
        AS stgdimncc (
            profilekey uuid,
            profile_code CHAR(10),
            ncc_code CHAR(10),
            full_name VARCHAR(35),
            birth_of_date DATE,
            sex BOOL,
            ethnic VARCHAR(15),
            identity_number VARCHAR(12),
            identity_place VARCHAR(35),
            home_town VARCHAR(35),
            province_code CHAR(5),
            district_code CHAR(5),
            decided_monthly_date DATE,
            decided_once_date DATE,
            decided_monthly_num CHAR(15),
            decided_once_num CHAR(15),
            start_subsidize DATE,
            support_bhyt BOOL,
            status BOOL,
            RowIsCurrent BOOL,
            RowStartDate TIMESTAMP,
            RowEndDate TIMESTAMP
        ) 
        WHERE RowIsCurrent='t' AND (profile_code, ncc_code, start_subsidize, support_bhyt, home_town) NOT IN(SELECT profile_code, ncc_code, 
                                                                                                                    start_subsidize, support_bhyt, home_town 
                                                                                                            FROM public."DimNCC");
    END;

    BEGIN;
		/***
			Update DWH to DimSubsidy Mart (SCD Type 2)
		***/
		UPDATE public."DimSubsidy"
		SET
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434', 
					'select * from ncc."DimSubsidy"')
		AS stgdimsubsidy (
			subsidykey uuid,
			province_code CHAR(5),
			district_code CHAR(5),
			profile_code CHAR(10),
			ncc_code CHAR(10),
			full_name VARCHAR(35),
			birth_of_date DATE,
			sex BOOL,
			ethnic VARCHAR(15),
			decided_monthly_date DATE,
			decided_once_date DATE,
			subsidy_code CHAR(10),
			year INT,
			spend_type public.spendtype,
			subsidy_name VARCHAR(255),
			subsidy_money FLOAT8,
			submoney FLOAT8,
			recieve_date DATE,
			RowIsCurrent BOOL,
			RowStartDate TIMESTAMP,
			RowEndDate TIMESTAMP
		) 
		WHERE (stgdimsubsidy.profile_code = "DimSubsidy".profile_code AND stgdimsubsidy.RowIsCurrent='t') 
			AND (stgdimsubsidy.subsidy_code = "DimSubsidy".subsidy_code) 
			AND (stgdimsubsidy.year = "DimSubsidy".year) AND ("DimSubsidy".RowIsCurrent='t')
			AND((stgdimsubsidy.recieve_date <> "DimSubsidy".recieve_date)
			   OR (stgdimsubsidy.subsidy_name <> "DimSubsidy".subsidy_name)
			   OR (stgdimsubsidy.subsidy_money <> "DimSubsidy".subsidy_money)
			   OR (stgdimsubsidy.submoney <> "DimSubsidy".submoney));
			   
		/***
			Insert DWH to DimSubsidy Mart (SCD Type 2)
		***/
		INSERT INTO public."DimSubsidy"(province_code, district_code, profile_code,
                                    subsidy_code, year, ncc_code, decided_monthly_date,
                                    decided_once_date, spend_type, subsidy_name, 
                                    subsidy_money, submoney, recieve_date,
									rowiscurrent, rowstartdate, rowenddate)
		SELECT
			province_code, district_code, profile_code,
			subsidy_code, year, ncc_code, decided_monthly_date,
			decided_once_date, spend_type, subsidy_name, 
			subsidy_money, submoney, recieve_date,
			'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434', 
					'select * from ncc."DimSubsidy"')
		AS stgdimsubsidy(
			subsidykey uuid,
			province_code CHAR(5),
			district_code CHAR(5),
			profile_code CHAR(10),
			ncc_code CHAR(10),
			full_name VARCHAR(35),
			birth_of_date DATE,
			sex BOOL,
			ethnic VARCHAR(15),
			decided_monthly_date DATE,
			decided_once_date DATE,
			subsidy_code CHAR(10),
			year INT,
			spend_type public.spendtype,
			subsidy_name VARCHAR(255),
			subsidy_money FLOAT8,
			submoney FLOAT8,
			recieve_date DATE,
			RowIsCurrent BOOL,
			RowStartDate TIMESTAMP,
			RowEndDate TIMESTAMP
		) 
		WHERE RowIsCurrent='t' AND (profile_code, subsidy_code, year, recieve_date, subsidy_name, subsidy_money, submoney) NOT IN(SELECT profile_code, subsidy_code, year, recieve_date,
																																		subsidy_name, subsidy_money, submoney
																																	FROM public."DimSubsidy");
    END;
        

    BEGIN;
        /***
			Update DWH from DimSurvey Mart (SCD Type 2)
		***/
		UPDATE public."DimSurvey"
		SET 
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434', 
					'select * from hongheo."DimSurvey"') 
		AS stgsurvey(
			surveykey uuid,
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
			final_result public.CLASSIFICATION,
			classify_person VARCHAR(35),
			RowIsCurrent BOOL,
			RowStartDate TIMESTAMP,
			RowEndDate TIMESTAMP
		)
		WHERE ((stgsurvey.family_id = "DimSurvey".family_id AND stgsurvey.RowIsCurrent='t')
			AND ("DimSurvey".RowIsCurrent='t')) AND
			((stgsurvey.a_grade <> "DimSurvey".a_grade)
			OR (stgsurvey.b1_grade <> "DimSurvey".b1_grade)
			OR (stgsurvey.b2_grade <> "DimSurvey".b2_grade)
			OR (stgsurvey.final_result <> "DimSurvey".final_result));
			
		/***
			Insert DWH from DimSurvey Mart (SCD Type 2)
		***/
		INSERT INTO public."DimSurvey"(family_id, years, a_id, fast_classify_person,
										condition_codes, condition_names, b1_id, is_aquaculture, 
										electricity_source, water_source, reason_names, get_policy_names,
										need_policy_names, a_grade, b1_grade, b2_grade,
										final_result, classify_person, 
										rowiscurrent, rowstartdate, rowenddate)
		SELECT 
			family_id, year, a_id, fast_classify_person,
			condition_codes, condition_names, b1_id, is_aquaculture, 
			electricity_source, water_source, reason_names, get_policy_names,
			need_policy_names, a_grade, b1_grade, b2_grade,
			final_result, classify_person,
			'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434',
					'select * from hongheo."DimSurvey"') 
		AS stgsurvey (
			surveykey uuid,
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
			final_result public.CLASSIFICATION,
			classify_person VARCHAR(35),
			RowIsCurrent BOOL,
			RowStartDate TIMESTAMP,
			RowEndDate TIMESTAMP
		)
		WHERE RowIsCurrent='t' AND (family_id, a_grade, b1_grade, b2_grade, final_result) NOT IN (SELECT family_id, a_grade, b1_grade, 
																					b2_grade, final_result
																				FROM public."DimSurvey");
    END;

    BEGIN;
        /***
            Update DWH from DimFamilyMember Mart (SCD Type 1)
        ***/
        UPDATE public."DimFamilyMember"
        SET
            education_status = stgmember.education_status, 
            education_level = stgmember.education_level, 
            culture_level = stgmember.culture_level,
            training_level = stgmember.training_level, 
            has_medical_insurance = stgmember.has_medical_insurance,
            has_job=stgmember.has_job, job_type=stgmember.job_type, 
            has_contract=stgmember.has_contract
        FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434',
                    'select * from hongheo."DimFamilyMember"')
        AS stgmember(
            memberkey uuid,
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
            has_pension public.PENSION_TYPE
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
            Insert DWH from stgDimFamilyMember Mart (SCD Type 1)
        ***/
        INSERT INTO public."DimFamilyMember"(member_id, family_id, full_name, owner_relationship, 
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
        FROM dblink('host=host.docker.internal dbname=LdtbxhDWH password=nhanbui user=postgres port=5434',
                    'select * from hongheo."DimFamilyMember"')
        AS stgmember(
            memberkey uuid,
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
            has_pension public.PENSION_TYPE
        )
        WHERE (stgmember.member_id) NOT IN (SELECT member_id FROM public."DimFamilyMember");
    END;
END;