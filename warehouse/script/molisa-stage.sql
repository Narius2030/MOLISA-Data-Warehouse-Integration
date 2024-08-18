-- CREATE DATABASE "LdtbxhStage" WITH ENCODING 'UTF8' TEMPLATE template0;

CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;

/****** --- DIMENSION --- ******/


/***
	Tạo các đối tượng enum để lưu trữ các sự lựa chọn cố định
***/

--
-- enum của trình độ học vấn
--

CREATE TYPE EDU_LEVEL AS ENUM (
	'mầm non, mẫu giáo',
	'tiểu học',
	'trung học cơ sở',
	'trung học phổ thông',
	'khóa đào tạo ngắn hạn',
	'trung cấp',
	'cao đẳng',
	'đại học trở lên'
);

--
-- enum của trình độ văn hóa
--

CREATE TYPE CUL_LEVEL AS ENUM (
	'chưa tốt nghiệp tiểu học',
	'tiểu học',
	'trung học cơ sở',
	'trung học phổ thông'
);

--
-- enum của trình độ đào tạo
--

CREATE TYPE TRAIN_LEVEL AS ENUM (
	'chưa qua đào tạo nghề',
	'sơ cấp chứng chỉ dạy nghề',
	'trung cấp',
	'cao đẳng',
	'đại học',
	'sau đại học'
);

--
-- enum của trợ cấp xã hội
--

CREATE TYPE TCXH AS ENUM (
	'người cao tuổi',
	'người khuyết tật',
	'trẻ em mồ côi'
);

--
-- enum của trạng thái nghề nghiệp
--

CREATE TYPE JOB_STATUS AS ENUM (
	'đang làm việc',
	'không có việc làm',
	'không có khả năng lao động',
	'đang đi học',
	'không muốn đi làm',
	'nghỉ hưu/nội trợ'
);

--
-- enum của loại công việc
--

CREATE TYPE JOB_CATE AS ENUM (
	'công chức viên chức',
	'làm cho doanh nghiệp',
	'làm thuê cho hộ gia đình',
	'khác'
);

--
-- enum của loại hợp đồng lao động
--

CREATE TYPE CONTRACT_TYPE AS ENUM (
	'có hợp đồng lao động',
	'không có hợp đồng lao động',
	'không làm công ăn lương'
);

--
-- enum của loại lương hưu
--

CREATE TYPE PENSION_TYPE AS ENUM (
	'đang hưởng lương hưu',
	'đang hưởng trợ cấp bảo hiểm xã hội hằng tháng',
	'đang hưởng trợ cấp người có công hằng tháng'
);

--
-- enum của phân loại hộ gia đình
--

CREATE TYPE CLASSIFICATION AS ENUM (
	'hộ nghèo',
	'hộ cận nghèo',
	'hộ không nghèo'
);

--
-- enum của loại nghề nghiệp gia đình
--

CREATE TYPE NLND AS ENUM (
	'nông nghiệp',
	'lâm nghiệp',
	'ngư nghiệp',
	'diêm nghiệp'
);

--
-- enum của loại chi trả ữu đãi (NCC)
--

CREATE TYPE spendtype AS ENUM
(
	'một lần',
	'hàng tháng',
	'hàng năm',
	'bổ sung'
);

--
-- enum của Loại đơn vị
--

CREATE TYPE unit_type AS ENUM (
	'doanh nghiệp',
	'đơn vị huấn luyện',
	'đơn vị kiểm định',
	'đơn vị huấn luyện và kiểm định',
	'hợp tác xã'
);

/*** 
	Tạo các Dimension chứa thông tin Hộ gia đình Rà soát ở từng kỳ
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS "stgDimFamily" (
	family_id uuid,	-- PK
	family_code VARCHAR(10),
	family_type VARCHAR(25),
	years SMALLINT,
	province_code CHAR(5),
	province_name VARCHAR(50),
	district_code CHAR(5),
	district_name VARCHAR(50),
	ward_code CHAR(5),
	ward_name VARCHAR(50),
	family_number VARCHAR(20),
	nation_in_place BOOL,
	PRIMARY KEY(family_id)
);
-- INSERT INTO "stgDimFamily"(family_id, province_code, province_name, district_code, district_name, family_number, nation_in_place)
-- VALUES ('70321aa9-91b9-4652-a52d-aa40892cab71', '67', 'Đắk Nông', '663', 'Đắk Mil', '7009/TT_DM', 't');


/*** 
	Tạo các Dimension chứa thông tin Thành viên Hộ gia đình Rà soát
	Incremental Load
***/

-- NOTE: Là phân cấp của bảng DimFamily

CREATE TABLE IF NOT EXISTS "stgDimFamilyMember" (
	member_id uuid,	-- PK
	family_id uuid,	-- PK
	full_name VARCHAR(35) NOT NULL,
	owner_relationship VARCHAR(15),
	year_of_birth SMALLINT,
	month_of_birth SMALLINT,
	day_of_birth SMALLINT,
	identity_card_number VARCHAR(12),
	nation VARCHAR(15),
	sex BOOL,
	height INT,
	weight INT,
	education_status BOOL,
	education_level public.EDU_LEVEL,
	culture_level public.CUL_LEVEL,
	training_level public.TRAIN_LEVEL,
	has_medical_insurance BOOL,
	social_assistance public.TCXH,
	has_job public.JOB_STATUS,
	job_type public.JOB_CATE,
	has_contract public.CONTRACT_TYPE,
	has_pension public.PENSION_TYPE,
	PRIMARY KEY(member_id)
);


/*** 
	Tạo các Dimension chứa thông tin B1 của từng Hộ tham gia các kỳ khảo sát
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS "stgDimSurvey" 
(
	family_id uuid,
	year INT,
	month INT,
	a_id uuid,
	fast_classify_person VARCHAR(35),
	condition_codes CHAR(5)[],
	condition_names VARCHAR(255)[],
	b1_id uuid,
	is_aquaculture BOOL,
	electricity_source CHAR(5),
	water_source CHAR(5),
	reason_names VARCHAR(255)[],
	get_policy_names VARCHAR(255)[],
	need_policy_names VARCHAR(255)[],
	a_grade BOOL,
	b1_grade SMALLINT,
	b2_grade SMALLINT,
	final_result public.CLASSIFICATION,
	classify_person VARCHAR(35),
	PRIMARY KEY(family_id)
);



/*** 
	Tạo các Dimension chứa thông tin Items
	Full Load
***/


CREATE TABLE IF NOT EXISTS "stgDimItems" (
	item_codes CHAR(5),
	item_names VARCHAR(255),
	measures FLOAT8,
	PRIMARY KEY(item_codes)
);



/***
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS "stgDimCompany"
(
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
    PRIMARY KEY(company_id)
);
SELECT * FROM "stgDimCompany"



/***
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/



CREATE TABLE IF NOT EXISTS "stgEquipment"
(
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
	PRIMARY KEY(device_id, quality_info_id)
);



/***
	Tạo các Dimension lữu trữ thông tin thông tin chi tiết từng vụ tai nạn
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS "stgDimAccident"
(
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
	PRIMARY KEY(accident_id)
);



/*** 
	Tạo các Dimension chứa thông tin Thời gian
	Full Load hoặc No Load
***/


CREATE TABLE IF NOT EXISTS "stgDate" 
(
    date_key INT, -- PK: month + year + day
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
    same_day_year_ago_date DATE,
    PRIMARY KEY(date_key)
);



/*** 
	Tạo các Dimension chứa thông tin Vị trí địa lý
	Full Load hoặc No Load
***/


CREATE TABLE IF NOT EXISTS "stgDimLocation"
(
    LocationKey BIGINT, -- PK: province + district + ward
	region_code CHAR(5),
	region_name VARCHAR(50),
	province_code CHAR(5),
	province_name VARCHAR(50),
	district_code CHAR(5),
	district_name VARCHAR(50),
	ward_code CHAR(5),
	ward_name VARCHAR(50),
    PRIMARY KEY(LocationKey)
);
ALTER TABLE "stgDimLocation" ALTER COLUMN LocationKey TYPE BIGINT;



/*** 
	Tạo các Stage chứa thông tin stgDimNCC
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS public."stgDimNCC"
(
	profile_code CHAR(10),
	ncc_code CHAR(10) NOT NULL,
	-- Common
	full_name VARCHAR(35) NOT NULL,
	birth_of_date DATE NOT NULL,
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
	PRIMARY KEY(profile_code)
);



/*** 
	Tạo các Stage chứa thông tin DimSubsidy
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS public."stgDimSubsidy"
(
	province_code CHAR(5),
	district_code CHAR(5),
	profile_code CHAR(10) NOT NULL, -- PK
	subsidy_code CHAR(10) NOT NULL,	-- PK
	year INT NOT NULL,	-- PK
	ncc_code CHAR(10) NOT NULL,
	full_name VARCHAR(35) NOT NULL,
	birth_of_date DATE,
	sex BOOL,
	ethnic VARCHAR(15),
	decided_monthly_date DATE,
	decided_once_date DATE,
	spend_type public.spendtype,
	subsidy_name VARCHAR(255),
	subsidy_money FLOAT8,
	submoney FLOAT8,
	recieve_date DATE NOT NULL,
	PRIMARY KEY(profile_code, subsidy_code, year)
);



/****** --- FACT --- ******/



/*** 
	Tạo các Fact chứa thông tin Fact Poverty Status
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, SurveyKey, LocationKey, family_code, owner_name, province_name, district_name
***/


CREATE TABLE IF NOT EXISTS "stgPovertyStatusFact" 
(
	family_id uuid,
	year SMALLINT,
	province_name VARCHAR(35),
	district_name VARCHAR(35),
	family_code VARCHAR(10),
	owner_name VARCHAR(35),
	hard_reasons VARCHAR(255)[],
	get_policies VARCHAR(255)[],
	need_policies VARCHAR(255)[],
	member_num SMALLINT,
	a_grade BOOL,
	b1_grade SMALLINT,
	b2_grade SMALLINT,
	b1_diff SMALLINT,
	b2_diff SMALLINT,
	final_result public.CLASSIFICATION,
    PRIMARY KEY(family_id)
);
CREATE INDEX year_idx ON "stgPovertyStatusFact"(year);
CREATE INDEX ownername_idx ON "stgPovertyStatusFact" USING SPGIST(owner_name);



/*** 
	Tạo các Dimension chứa thông tin Thành viên tham gia rà soát
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, MemberKey, owner_name, member_name, province_name, district_name, age
***/


CREATE TABLE IF NOT EXISTS "stgMemberSurveyFact" 
(
	member_id  uuid,
	family_id uuid,
	year INT,
	month INT,
	province_name VARCHAR(35),
	district_name VARCHAR(35),
	member_name VARCHAR(35),
	owner_relationship VARCHAR(15),
	year_of_birth SMALLINT,
	month_of_birth SMALLINT,
	day_of_birth SMALLINT,
	age SMALLINT,
	identity_card_number VARCHAR(12),
	nation VARCHAR(15),
	final_result public.CLASSIFICATION,
    PRIMARY KEY(member_id)
);
CREATE INDEX membername_idx ON "stgMemberSurveyFact" USING SPGIST(member_name);
CREATE INDEX ownerrelationship_idx ON "stgMemberSurveyFact" USING SPGIST(owner_relationship);
CREATE INDEX nation_idx ON "stgMemberSurveyFact"(nation);
CREATE INDEX age_idx ON "stgMemberSurveyFact"(age);
CREATE INDEX yearofbirth_idx ON "stgMemberSurveyFact"(year_of_birth);

/*** 
	Tạo các Dimension chứa thông tin Từng vụ tai nạn trong Tai nạn Định kỳ
	Full Load hoặc No Load
	Index: company_name, compay_foreign_name, reason, career
***/


CREATE TABLE IF NOT EXISTS "stgPeriodicalAccidentFact" 
(
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
    PRIMARY KEY(accident_id)
);
CREATE INDEX reason_idx ON "stgPeriodicalAccidentFact" USING SPGIST(reason);
CREATE INDEX factor_idx ON "stgPeriodicalAccidentFact" USING SPGIST(factor);
CREATE INDEX career_idx ON "stgPeriodicalAccidentFact" USING SPGIST(career);



/*** 
	Tạo các Dimension chứa thông tin Fact Subsidy Reporting
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, SurveyKey, LocationKey, family_code, owner_name, province_name, district_name
***/


CREATE TABLE IF NOT EXISTS public."stgSubsidyReportFact"
(
	province_code CHAR(5),
	district_code CHAR(5),
	profile_code CHAR(10) NOT NULL,
	ncc_code CHAR(10) NOT NULL,
	full_name VARCHAR(35) NOT NULL,
	ethnic VARCHAR(15),
	subsidy_code CHAR(10) NOT NULL,
	year INT NOT NULL,
	spend_type public.spendtype,
	subsidy_name VARCHAR(255),
	subsidy_money FLOAT8 NOT NULL,
	submoney FLOAT8,
	start_subsidize DATE NOT NULL,
	recieve_date DATE NOT NULL,
	actual_spending FLOAT8 NOT NULL,
	spend_diff FLOAT8,
	recieve_days INT,
	PRIMARY KEY(profile_code, subsidy_code, year)
);
SELECT * FROM "stgSubsidyReportFact"

/*** 
	Tạo các Dimension tham chiếu từ DimAudit trong Data warehouse
***/

-- CREATE EXTENSION postgres_fdw;
-- CREATE EXTENSION dblink;

CREATE SERVER ldtbxh_dwh_server FOREIGN DATA WRAPPER postgres_fdw 
	OPTIONS (host 'host.docker.internal', port '5434', dbname 'LdtbxhDWH');
	
CREATE FOREIGN TABLE "DimAuditForeigned" (
	id uuid DEFAULT gen_random_uuid(),
	process_name VARCHAR(35),
	start_at TIMESTAMP,
	finished_at TIMESTAMP,
	information VARCHAR(255),
	status VARCHAR(10)
) SERVER ldtbxh_dwh_server OPTIONS (schema_name 'public', table_name 'DimAudit');

CREATE USER MAPPING FOR postgres SERVER ldtbxh_dwh_server OPTIONS (user 'postgres', password 'nhanbui');


SELECT * FROM "stgDimSubsidy"

