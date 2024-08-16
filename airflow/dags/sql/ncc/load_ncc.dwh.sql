
----------- DIMENSION -----------

BEGIN;
	/***
		Load Stage to DimNCC (SCD Type 2)
	***/
	BEGIN;
		/***
			Update Stage to DimNCC (SCD Type 2)
		***/
		UPDATE ncc."DimNCC"
		SET
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimNCC"')
		AS stgdimncc (
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
			status BOOL
		) 
		WHERE (stgdimncc.profile_code = "DimNCC".profile_code) AND ("DimNCC".rowenddate IS NULL)
			AND((stgdimncc.start_subsidize <> "DimNCC".start_subsidize)
			   OR (stgdimncc.support_bhyt <> "DimNCC".support_bhyt)
			   OR (stgdimncc.home_town <> "DimNCC".home_town)
			   OR (stgdimncc.ncc_code <> "DimNCC".ncc_code));
			   
		/***
			Insert Stage to DimNCC (SCD Type 2)
		***/
		INSERT INTO ncc."DimNCC"(profile_code, ncc_code, full_name,
                                birth_of_date, sex, ethnic, identity_number,
                                identity_place, home_town, province_code,
                                district_code, decided_monthly_date, decided_once_date,
                                decided_monthly_num, decided_once_num,
                                start_subsidize, support_bhyt, status,
								rowiscurrent, rowstartdate, rowenddate)
		SELECT
			*, 'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimNCC"')
		AS stgdimncc (
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
			status BOOL
		) 
		WHERE (profile_code, ncc_code, start_subsidize, support_bhyt, home_town) NOT IN(SELECT profile_code, ncc_code, start_subsidize, support_bhyt, home_town 
																			  			FROM ncc."DimNCC");
	END;
	
	/***
		Load Stage to DimSubsidy (SCD Type 2)
	***/
	BEGIN;
		/***
			Update Stage to DimSubsidy (SCD Type 2)
		***/
		UPDATE ncc."DimSubsidy"
		SET
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimSubsidy"')
		AS stgdimsubsidy (
			province_code CHAR(5),
			district_code CHAR(5),
			profile_code CHAR(10),
			subsidy_code CHAR(10),
			year INT,
			ncc_code CHAR(10),
			full_name VARCHAR(35),
			birth_of_date DATE,
			sex BOOL,
			ethnic VARCHAR(15),
			decided_monthly_date DATE,
			decided_once_date DATE,
			spend_type ncc.spendtype,
			subsidy_name VARCHAR(255),
			subsidy_money FLOAT8,
			submoney FLOAT8,
			recieve_date DATE
		) 
		WHERE (stgdimsubsidy.profile_code = "DimSubsidy".profile_code) 
			AND (stgdimsubsidy.subsidy_code = "DimSubsidy".subsidy_code) 
			AND (stgdimsubsidy.year = "DimSubsidy".year) AND ("DimSubsidy".rowenddate IS NULL)
			AND((stgdimsubsidy.recieve_date <> "DimSubsidy".recieve_date)
			   OR (stgdimsubsidy.subsidy_name <> "DimSubsidy".subsidy_name)
			   OR (stgdimsubsidy.subsidy_money <> "DimSubsidy".subsidy_money)
			   OR (stgdimsubsidy.submoney <> "DimSubsidy".submoney));
			   
		/***
			Insert Stage to DimSubsidy (SCD Type 2)
		***/
		INSERT INTO ncc."DimSubsidy"(province_code, district_code, profile_code,
                                    subsidy_code, year, ncc_code, full_name,
                                    birth_of_date, sex, ethnic, decided_monthly_date,
                                    decided_once_date, spend_type, subsidy_name, 
                                    subsidy_money, submoney, recieve_date,
									rowiscurrent, rowstartdate, rowenddate)
		SELECT
			*, 'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimSubsidy"')
		AS stgdimsubsidy(
			province_code CHAR(5),
			district_code CHAR(5),
			profile_code CHAR(10),
			subsidy_code CHAR(10),
			year INT,
			ncc_code CHAR(10),
			full_name VARCHAR(35),
			birth_of_date DATE,
			sex BOOL,
			ethnic VARCHAR(15),
			decided_monthly_date DATE,
			decided_once_date DATE,
			spend_type ncc.spendtype,
			subsidy_name VARCHAR(255),
			subsidy_money FLOAT8,
			submoney FLOAT8,
			recieve_date DATE
		) WHERE (profile_code, subsidy_code, year, recieve_date, subsidy_name, subsidy_money, submoney) NOT IN(SELECT profile_code, subsidy_code, year, recieve_date,
																											   		subsidy_name, subsidy_money, submoney
																											   FROM ncc."DimSubsidy");
	END;
END;


----------- FACT -----------


/*** 
    Load Stage to SubsidyReportFact
***/
BEGIN;
    INSERT INTO ncc."SubsidyReportFact"(profilekey, subsidykey, datekey, 
                                        province_code, district_code, profile_code,
                                        ncc_code, full_name, ethnic, subsidy_code,
                                        year, spend_type, subsidy_name, subsidy_money, submoney,
                                        spend_diff, recieve_days)
    SELECT
        (SELECT profilekey FROM ncc."DimNCC" WHERE profile_code=stgsubsidyfact.profile_code AND rowiscurrent='TRUE') AS profilekey,
        (SELECT subsidykey FROM ncc."DimSubsidy" WHERE profile_code=stgsubsidyfact.profile_code 
                                                        AND subsidy_code=stgsubsidyfact.subsidy_code 
                                                        AND year=stgsubsidyfact.year AND rowiscurrent='TRUE') AS subsidykey,
        (EXTRACT(YEAR FROM recieve_date)*100 + EXTRACT(MONTH FROM recieve_date)) AS datekey,
        province_code, district_code, profile_code,
        ncc_code, full_name, ethnic, subsidy_code,
        year, spend_type, subsidy_name, subsidy_money, submoney,
        spend_diff, recieve_days
    FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
                'select * from public."stgSubsidyReportFact"')
    AS stgsubsidyfact (
        province_code CHAR(5),
        district_code CHAR(5),
        profile_code CHAR(10),
        ncc_code CHAR(10),
        full_name VARCHAR(35),
        ethnic VARCHAR(15),
        subsidy_code CHAR(10),
        year INT,
        spend_type ncc.spendtype,
        subsidy_name VARCHAR(255),
        subsidy_money FLOAT8,
        submoney FLOAT8,
        recieve_days INT,
        recieve_date DATE,
        start_subsidize DATE,
        spend_diff FLOAT8,
        actual_spending FLOAT8
    ) WHERE ((SELECT profilekey FROM ncc."DimNCC" WHERE profile_code=stgsubsidyfact.profile_code AND rowiscurrent='TRUE'),
            (SELECT subsidykey FROM ncc."DimSubsidy" WHERE profile_code=stgsubsidyfact.profile_code 
                                                        AND subsidy_code=stgsubsidyfact.subsidy_code 
                                                        AND year=stgsubsidyfact.year AND rowiscurrent='TRUE')) NOT IN(SELECT profilekey, subsidykey FROM ncc."SubsidyReportFact");
END;