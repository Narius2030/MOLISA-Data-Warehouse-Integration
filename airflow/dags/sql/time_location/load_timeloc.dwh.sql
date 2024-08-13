BEGIN;
	INSERT INTO "DimDate"(datekey, month, month_num_overall, month_name,
						month_abbrev, quarter, year, fiscal_month,
						fiscal_quarter, fiscal_year)
	SELECT
		DISTINCT(year*100 + month) AS datekey,
		month, month_num_overall, month_name,
		month_abbrev, quarter, year, fiscal_month,
		fiscal_quarter, fiscal_year
	FROM dblink('host=host.docker.internal dbname=LdtbxhStage password=nhanbui user=postgres port=5434', 
					'select * from public."stgDate"') 
	AS stgdate (
		date_key INT, -- PK: month + year
		full_date DATE,
		day_of_week INT,
		day_num_in_month INT,
		day_num_overall INT,
		day_name VARCHAR(255),
		day_abbrev VARCHAR(255),
		weekday_flag VARCHAR(255),
		week_num_in_year INT,
		week_num_overall INT,
		week_begin_date DATE,
		week_begin_date_key INT,
		month INT,
		month_num_overall INT,
		month_name VARCHAR(255),
		month_abbrev VARCHAR(255),
		quarter INT,
		year INT,
		yearmo INT,
		fiscal_month INT,
		fiscal_quarter INT,
		fiscal_year INT,
		last_day_in_month_flag CHAR(20),
		same_day_year_ago_date DATE
	)
	ORDER BY year, month
END;