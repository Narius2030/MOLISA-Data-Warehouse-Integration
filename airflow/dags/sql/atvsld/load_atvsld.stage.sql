/*** Load stage for dimensions ***/

BEGIN TRANSACTION;
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
        *
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
        total_hard_not_managed INT
    );


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
        *
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
        deputy VARCHAR(255)
    );


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
        *
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
        import_date DATE
    );
COMMIT;


/*** Load stage for facts ***/