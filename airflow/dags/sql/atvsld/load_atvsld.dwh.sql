/*** ATVSLD ***/

BEGIN;
	/***
        Load stgDimAccident to DimAccident SCD Type 2
    ***/
	BEGIN;
		/***
			Update stgDimAccident from DimAccident (SCD Type 2)
		***/
		UPDATE atvsld."DimAccident"
		SET
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimAccident"')
		AS stgaccident (
			accident_id uuid,
			reason VARCHAR(255),
			factor VARCHAR(255),
			career VARCHAR(255),
			year CHAR(10),
			from_date DATE,
			to_date DATE,
			period CHAR(10),
			report_type VARCHAR(255),
			has_support BOOL,
			total_people INT,
			total_hasdead INT,
			total_female INT,
			total_hard_case INT,
			total_not_managed INT,
			total_female_not_managed INT,
			total_dead_not_managed INT,
			total_hard_not_managed INT
		) 
		WHERE (stgaccident.accident_id = "DimAccident".accident_id) AND ("DimAccident".rowenddate IS NULL)
			AND ((stgaccident.reason <> "DimAccident".reason)
				OR (stgaccident.factor <> "DimAccident".factor)
				OR (stgaccident.career <> "DimAccident".career));
		
		/***
			Insert stgDimAccident from DimAccident (SCD Type 2)
		***/
		INSERT INTO atvsld."DimAccident"(accident_id, reason, factor, career,
									year, from_date, to_date, 
									period, report_type, has_support,
									total_people, total_hasdead, total_female,
									total_hard_case, total_not_managed, total_female_not_managed,
									total_dead_not_managed, total_hard_not_managed,
									rowiscurrent, rowstartdate, rowenddate)
		SELECT
			*, 'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimAccident"')
		AS stgaccident (
			accident_id uuid,
			reason VARCHAR(255),
			factor VARCHAR(255),
			career VARCHAR(255),
			year CHAR(10),
			from_date DATE,
			to_date DATE,
			period CHAR(10),
			report_type VARCHAR(255),
			has_support BOOL,
			total_people INT,
			total_hasdead INT,
			total_female INT,
			total_hard_case INT,
			total_not_managed INT,
			total_female_not_managed INT,
			total_dead_not_managed INT,
			total_hard_not_managed INT
		) 
		WHERE (accident_id, reason, factor, career) NOT IN(SELECT accident_id, reason, factor, career FROM atvsld."DimAccident");
	END;
	
	
	/***
        Load stgDimCompany to DimCompany SCD Type 2
    ***/
	BEGIN;
		/***
			Update Stage to DimCompany (SCD Type 2)
		***/
		UPDATE atvsld."DimCompany"
		SET
			rowiscurrent = 'FALSE',
			rowenddate = CURRENT_TIMESTAMP
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimCompany"')
		AS stgcompany (
			company_id uuid,
			name VARCHAR(35),
			tax_number VARCHAR(15),
			unit atvsld.unit_type,
			business_name VARCHAR(35),
			major_name VARCHAR(35),
			ngaycap_GPKD DATE,
			province_code CHAR(5),
			district_code CHAR(5),
			ward_code CHAR(5),
			town_code CHAR(5),
			foreign_name VARCHAR(35),
			email VARCHAR(255),
			contact_province CHAR(5),
			contact_district CHAR(5),
			contact_ward CHAR(5),
			deputy VARCHAR(255)
		) 
		WHERE (stgcompany.company_id = "DimCompany".company_id) AND ("DimCompany".rowenddate IS NULL)
			AND((stgcompany.business_name <> "DimCompany".business_name)
			   OR (stgcompany.major_name <> "DimCompany".major_name)
			   OR (stgcompany.email <> "DimCompany".email));
		
		/***
			Insert Stage to DimCompany (SCD Type 2)
		***/
		INSERT INTO atvsld."DimCompany"(company_id, name, tax_number,
									unit, business_name, major_name, 
									ngaycap_GPKD, province_code, district_code, 
									ward_code, town_code, foreign_name, 
									email, contact_province, contact_district, 
									contact_ward, deputy, rowiscurrent, rowstartdate, rowenddate)
		SELECT
			*, 'TRUE' AS rowiscurrent, CURRENT_TIMESTAMP AS rowstartdate, NULL AS rowenddate
		FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDimCompany"')
		AS stgcompany (
			company_id uuid,
			name VARCHAR(35),
			tax_number VARCHAR(15),
			unit atvsld.unit_type,
			business_name VARCHAR(35),
			major_name VARCHAR(35),
			ngaycap_GPKD DATE,
			province_code CHAR(5),
			district_code CHAR(5),
			ward_code CHAR(5),
			town_code CHAR(5),
			foreign_name VARCHAR(35),
			email VARCHAR(255),
			contact_province CHAR(5),
			contact_district CHAR(5),
			contact_ward CHAR(5),
			deputy VARCHAR(255)
		) 
		WHERE (business_name, major_name, unit, email) NOT IN(SELECT business_name, major_name, unit, email FROM atvsld."DimCompany");
	END;
END;




----------- FACT -----------

/***
	Load Stage to PeriodicalAccidentFact
***/
BEGIN;
	INSERT INTO atvsld."PeriodicalAccidentFact"(accidentkey, companykey, datekey, isdeleted, accident_id, year, month, 
												company_name, compay_foreign_name, major_name, reason, factor, career, total_people, total_hasdead, total_female,
												medical_expense, treatment_expense, indemnify_expense, total_expense, day_off, assest_harm)
	SELECT
		(SELECT accidentkey FROM atvsld."DimAccident" WHERE accident_id=stgaccident.accident_id AND rowiscurrent='TRUE') AS accidentkey,
		(SELECT companykey FROM atvsld."DimCompany" WHERE company_name=stgaccident.company_name AND rowiscurrent='TRUE') AS companykey,
		(year*100 + month) AS datekey, 'FALSE',
		*
	FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
				'select * from public."stgPeriodicalAccidentFact"')
	AS stgaccident(
		accident_id uuid,
		year INT,
		month INT,
		company_name VARCHAR(35),
		compay_foreign_name VARCHAR(35),
		major_name VARCHAR(35),
		reason VARCHAR(255),
		factor VARCHAR(255),
		career VARCHAR(255),
		total_people INT,
		total_hasdead INT,
		total_female INT,
		medical_expense FLOAT8,
		treatment_expense FLOAT8,
		indemnify_expense FLOAT8,
		total_expense FLOAT8,
		day_off INT,
		assest_harm FLOAT8
	);
END;