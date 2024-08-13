/*** Load stage for dimensions ***/
DO
$$
BEGIN
    /***
        Load Source to stgDimAccident (Bronze)
    ***/
    INSERT INTO "stgDimAccident"(accident_id, reason, factor, career,
                                year, from_date, to_date, 
                                period, report_type, has_support,
                                total_people, total_hasdead, total_female,
                                total_hard_case, total_not_managed, total_female_not_managed,
                                total_dead_not_managed, total_hard_not_managed)
    SELECT
        accident_id, reason, factor, career,
        year, from_date, to_date, 
        period, report_type, has_support,
        total_people, total_hasdead, total_female,
        total_hard_case, total_not_managed, total_female_not_managed,
        total_dead_not_managed, total_hard_not_managed
    FROM dblink('host=host.docker.internal dbname=atvsld password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgdimaccident')
    AS (
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
        total_hard_not_managed INT,
        created_date TIMESTAMP
    ) 
    WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));


    /***
        Load Source to stgDimCompany (Bronze)
    ***/
    INSERT INTO "stgDimCompany"(company_id, name, tax_number,
                                unit, business_name, major_name, 
                                ngaycap_GPKD, province_code, district_code, 
                                ward_code, town_code, foreign_name, 
                                email, contact_province, contact_district, 
                                contact_ward, deputy)
    SELECT
        company_id, name, tax_number,
        unit, business_name, major_name, 
        ngaycap_GPKD, province_code, district_code, 
        ward_code, town_code, foreign_name, 
        email, contact_province, contact_district, 
        contact_ward, deputy
    FROM dblink('host=host.docker.internal dbname=atvsld password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgdimcompany')
    AS (
        company_id uuid,
        name VARCHAR(35),
        tax_number VARCHAR(15),
        unit public.unit_type,
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
        deputy VARCHAR(255),
        created_date TIMESTAMP
    ) 
    WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));


    /***
        Load Source to stgDimEuqipment (Bronze)
    ***/
    INSERT INTO "stgEquipment"(device_id, year, month,
                            quality_info_id, company_id,
                            device_name, hs_code, technical_feature, 
                            origin, quantity, unit, contract_number, 
                            contract_date, category, management_cerf,
                            issuance_organize, issuance_date, issuance_place,
                            profile_code, page_of_co, import_gate, import_date)
    SELECT
        device_id, year, month,
        quality_info_id, company_id,
        device_name, hs_code, technical_feature, 
        origin, quantity, unit, contract_number, 
        contract_date, category, management_cerf,
        issuance_organize, issuance_date, issuance_place,
        profile_code, page_of_co, import_gate, import_date
    FROM dblink('host=host.docker.internal dbname=atvsld password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgdimequipment')
    AS (
        device_id uuid,	-- PK
        year INT,
        month INT,
        quality_info_id uuid,	-- PK
        company_id uuid,
        device_name VARCHAR(35),
        hs_code CHAR(15),
        technical_feature VARCHAR(35),
        origin VARCHAR(35),
        quantity INT,
        unit CHAR(10),
        contract_number VARCHAR(15),
        contract_date DATE,
        category VARCHAR(10),
        management_cerf VARCHAR(15),
        issuance_organize VARCHAR(35),
        issuance_date DATE,
        issuance_place VARCHAR(255),
        profile_code VARCHAR(15),
        page_of_co INT,
        import_gate VARCHAR(255),
        import_date DATE,
        created_date TIMESTAMP
    )
    WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));
    
EXCEPTION
	WHEN OTHERS THEN
		UPDATE "DimAuditForeigned" 
		SET 
			information = 'ATVSLD Dimension: Something went wrong on running statements. 
							HINT: Check carefully the syntax or selected columns at "SELECT" or "INSERT" clauses',
			status = 'ERROR',
			finished_at = NOW()
		WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned" WHERE status='ERROR');
-- 		RAISE EXCEPTION  'when wrong with selected columns' USING HINT = 'Check on "SELECT" or "INSERT" statement';

END;
$$;


/*** Load stage for facts ***/
DO
$$
BEGIN
    /***
        Load Source to stgPeriodicalAccidentFact (Bronze)
    ***/
    INSERT INTO "stgPeriodicalAccidentFact"(accident_id, year, month, company_name, compay_foreign_name,
                                            major_name, reason, factor, career, 
                                            total_people, total_hasdead, total_female,
                                            medical_expense, treatment_expense,
                                            indemnify_expense, total_expense,
                                            day_off, assest_harm)
    SELECT
        accident_id, year, month, company_name, compay_foreign_name,
        major_name, reason, factor, career,
        total_people, total_hasdead, total_female,
        medical_expense, treatment_expense,
        indemnify_expense, total_expense,
        day_off, assest_harm
    FROM dblink('host=host.docker.internal dbname=atvsld password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgperiodicalaccidentfact')
    AS (
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
        assest_harm FLOAT8,
        accident_created_date TIMESTAMP,
        expense_created_date TIMESTAMP
    )
    WHERE (accident_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'))
        OR (expense_created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));

EXCEPTION
	WHEN OTHERS THEN
		UPDATE "DimAuditForeigned" 
		SET 
			information = 'ATVSLD Fact: Something went wrong on running statements. 
							HINT: Check carefully the syntax or selected columns at "SELECT" or "INSERT" clauses',
			status = 'ERROR',
			finished_at = NOW()
		WHERE start_at = (SELECT MAX(start_at) FROM "DimAuditForeigned");

END;
$$;