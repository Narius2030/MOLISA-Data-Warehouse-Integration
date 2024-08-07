BEGIN TRANSACTION;
	INSERT INTO "stgDimLocation"(LocationKey, region_code, region_name,
								province_code, province_name,
								district_code, district_name,
								ward_code, ward_name)
	SELECT
		*
	FROM dblink('host=host.docker.internal dbname=hongheovna password=nhanbui user=postgres port=5434', 
				'select * from public.vw_stgdimlocation')
	AS (
		LocationKey BIGINT,
		region_code CHAR(5),
		region_name VARCHAR(50),
		province_code CHAR(5),
		province_name VARCHAR(50),
		district_code CHAR(5),
		district_name VARCHAR(50),
		ward_code CHAR(5),
		ward_name VARCHAR(50)
	);
COMMIT;