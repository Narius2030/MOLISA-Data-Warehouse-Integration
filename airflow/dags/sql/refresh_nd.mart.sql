TRUNCATE TABLE public."DimNCC" CASCADE;
TRUNCATE TABLE public."DimFamilyMember" CASCADE;
TRUNCATE TABLE public."DimSurvey" CASCADE;
TRUNCATE TABLE public."DimSubsidy";
TRUNCATE TABLE public."DimDate" CASCADE;
TRUNCATE TABLE public."DimLocation";
TRUNCATE TABLE public."NccPovertyFact" CASCADE;

BEGIN;
    INSERT INTO "DimNCC"(profilekey,profile_code, start_subsidize, support_bhyt, home_town) VALUES('00000000-0000-0000-0000-000000000000','N/A','2020-01-01','FALSE','N/A');
    INSERT INTO "DimSurvey"(surveykey,family_id, a_grade, b1_grade, b2_grade, final_result) VALUES('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','FALSE',-1,-1,'N/A');
    INSERT INTO "DimFamilyMember"(memberkey,member_id) VALUES('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000');
END;