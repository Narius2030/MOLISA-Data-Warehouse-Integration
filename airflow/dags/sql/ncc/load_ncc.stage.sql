
/****** --- DIMENSION --- ******/


BEGIN;
    BEGIN;
        /***
            Load Source to stgDimNccProfile (Bronze)
        ***/
        INSERT INTO "stgDimNCC"(profile_code, ncc_code, full_name, 
                                birth_of_date, sex, ethnic, identity_number, 
                                identity_place, home_town, province_code, 
                                district_code, decided_monthly_date, decided_once_date, 
                                decided_monthly_num, decided_once_num, 
                                start_subsidize, support_bhyt, status)
        SELECT
            profile_code, ncc_code, full_name, 
            birth_of_date, sex, ethnic, identity_number, 
            identity_place, home_town, province_code, 
            district_code, decided_monthly_date, decided_once_date, 
            decided_monthly_num, decided_once_num, 
            start_subsidize, support_bhyt, status
        FROM dblink('host=host.docker.internal dbname=ncc password=nhanbui user=postgres port=5434', 
                    'select * from profile.vw_stgnccprofile')
        AS (
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
            created_date TIMESTAMP
        )
        WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));
    END;


    BEGIN;
        /***
            Load Source to stgDimSubsidy (Bronze)
        ***/
        INSERT INTO "stgDimSubsidy"(province_code, district_code, profile_code,
                                    subsidy_code, year, ncc_code, full_name,
                                    birth_of_date, sex, ethnic, decided_monthly_date,
                                    decided_once_date, spend_type, subsidy_name, 
                                    subsidy_money, submoney, recieve_date)
        SELECT
            province_code, district_code, profile_code,
            subsidy_code, year, ncc_code, full_name,
            birth_of_date, sex, ethnic, decided_monthly_date,
            decided_once_date, spend_type, subsidy_name, 
            subsidy_money, submoney, recieve_date
        FROM dblink('host=host.docker.internal dbname=ncc password=nhanbui user=postgres port=5434', 
                    'select * from public.vw_stgsubsidy')
        AS (
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
            spend_type public.spendtype,
            subsidy_name VARCHAR(255),
            subsidy_money FLOAT8,
            submoney FLOAT8,
            recieve_date DATE,
            created_date TIMESTAMP
        )
        WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));
    END;
END;


/****** --- FACT --- ******/


BEGIN;
    /***
        Load Source to stgSubsidyReportFact (Bronze)
    ***/
    INSERT INTO "stgSubsidyReportFact"(province_code, district_code, profile_code,
                                    ncc_code, full_name, ethnic, subsidy_code,
                                    year, spend_type, subsidy_name, subsidy_money, submoney,
                                    start_subsidize, recieve_date, actual_spending, spend_diff, recieve_days)
    SELECT
        province_code, district_code, profile_code,
        ncc_code, full_name, ethnic, subsidy_code,
        year, spend_type, subsidy_name, subsidy_money, submoney,
        start_subsidize, recieve_date, actual_spending, NULL, NULL
    FROM dblink('host=host.docker.internal dbname=ncc password=nhanbui user=postgres port=5434', 
                'select * from public.vw_stgsubsidyfact')
    AS (
        province_code CHAR(5),
        district_code CHAR(5),
        profile_code CHAR(10),
        ncc_code CHAR(10),
        full_name VARCHAR(35),
        ethnic VARCHAR(15),
        subsidy_code CHAR(10),
        year INT,
        spend_type public.spendtype,
        subsidy_name VARCHAR(255),
        subsidy_money FLOAT8,
        submoney FLOAT8,
        start_subsidize DATE,
        recieve_date DATE,
        actual_spending FLOAT8,
        created_date TIMESTAMP
    )
    WHERE (created_date >= (SELECT MAX(finished_at) FROM "DimAuditForeigned" WHERE status='SUCCESS'));
END;