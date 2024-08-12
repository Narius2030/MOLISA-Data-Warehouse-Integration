TRUNCATE TABLE public."stgDimFamily";
TRUNCATE TABLE public."stgDimFamilyMember";
TRUNCATE TABLE public."stgDimSurvey";
TRUNCATE TABLE public."stgPovertyStatusFact";
TRUNCATE TABLE public."stgMemberSurveyFact";
-- TRUNCATE TABLE public."stgDimCompany";
-- TRUNCATE TABLE public."stgDimAccident";
-- TRUNCATE TABLE public."stgDimEquipment";

BEGIN TRANSACTION;
    INSERT INTO "DimAuditForeigned"(process_name, start_at, finished_at, information, status) VALUES
    ('data integration', NOW(), NOW(), 'refreshed data', 'PENDING');
COMMIT;