TRUNCATE TABLE public."stgDate";
TRUNCATE TABLE public."stgDimLocation";
TRUNCATE TABLE public."stgDimFamily";
TRUNCATE TABLE public."stgDimFamilyMember";
TRUNCATE TABLE public."stgDimSurvey";

BEGIN TRANSACTION;
    INSERT INTO "DimAuditForeigned"(process_name, start_at, finished_at) VALUES
    ('data integration', NOW(), NULL);
COMMIT;