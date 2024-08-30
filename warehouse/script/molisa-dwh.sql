-- CREATE DATABASE "LdtbxhDWH" WITH ENCODING 'UTF8' TEMPLATE template0;

-- DELETE FROM hongheo."PovertyStatusFact";
-- DELETE FROM hongheo."MemberSurveyFact";
-- DELETE FROM hongheo."DimFamily";
-- DELETE FROM hongheo."DimFamilyMember";
-- DELETE FROM hongheo."DimSurvey";
-- DELETE FROM atvsld."PeriodicalAccidentFact";
-- DELETE FROM atvsld."DimCompany";
-- DELETE FROM atvsld."DimAccident";
-- DELETE FROM ncc."SubsidyReportFact"
-- DELETE FROM "DimAudit" WHERE information='ETL successfully'

SELECT * FROM hongheo."DimFamilyMember"
SELECT * FROM ncc."DimNCC"

-- CREATE SCHEMA public;
-- GRANT ALL ON SCHEMA public TO postgres;


/****** Tạo các Tablespace chỉ định nơi lưu trữ các database object ******/

-- DROP TABLESPACE ts_datalocations
-- CREATE TABLESPACE ts_datahongheo LOCATION '/home/postgres/tablespace/ts_dwh_hn';
-- CREATE TABLESPACE ts_dataatvsld LOCATION '/home/postgres/tablespace/ts_dwh_atvsld';
-- CREATE TABLESPACE ts_datancc LOCATION '/home/postgres/tablespace/ts_dwh_ncc';


/****** --- DIMENSION --- ******/



/****** Tạo các Schema lưu trữ dữ liệu tích hợp Hộ nghèo ******/


CREATE SCHEMA hongheo;
GRANT ALL ON SCHEMA hongheo TO postgres;


/***
	Tạo các đối tượng enum để lưu trữ các sự lựa chọn cố định
***/

--
-- enum của trình độ học vấn
--

CREATE TYPE hongheo.EDU_LEVEL AS ENUM (
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

CREATE TYPE hongheo.CUL_LEVEL AS ENUM (
	'chưa tốt nghiệp tiểu học',
	'tiểu học',
	'trung học cơ sở',
	'trung học phổ thông'
);

--
-- enum của trình độ đào tạo
--

CREATE TYPE hongheo.TRAIN_LEVEL AS ENUM (
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

CREATE TYPE hongheo.TCXH AS ENUM (
	'người cao tuổi',
	'người khuyết tật',
	'trẻ em mồ côi'
);

--
-- enum của trạng thái nghề nghiệp
--

CREATE TYPE hongheo.JOB_STATUS AS ENUM (
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

CREATE TYPE hongheo.JOB_CATE AS ENUM (
	'công chức viên chức',
	'làm cho doanh nghiệp',
	'làm thuê cho hộ gia đình',
	'khác'
);

--
-- enum của loại hợp đồng lao động
--

CREATE TYPE hongheo.CONTRACT_TYPE AS ENUM (
	'có hợp đồng lao động',
	'không có hợp đồng lao động',
	'không làm công ăn lương'
);

--
-- enum của loại lương hưu
--

CREATE TYPE hongheo.PENSION_TYPE AS ENUM (
	'đang hưởng lương hưu',
	'đang hưởng trợ cấp bảo hiểm xã hội hằng tháng',
	'đang hưởng trợ cấp người có công hằng tháng'
);

--
-- enum của phân loại hộ gia đình
--

CREATE TYPE hongheo.CLASSIFICATION AS ENUM (
	'hộ nghèo',
	'hộ cận nghèo',
	'hộ không nghèo'
);

--
-- enum của loại nghề nghiệp gia đình
--

CREATE TYPE hongheo.NLND AS ENUM (
	'nông nghiệp',
	'lâm nghiệp',
	'ngư nghiệp',
	'diêm nghiệp'
);



/*** 
	Tạo các Dimension chứa thông tin Hộ gia đình Rà soát ở từng kỳ
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS hongheo."DimFamily" (
	FamilyKey uuid DEFAULT gen_random_uuid(),	-- PK
	family_id uuid NOT NULL,	-- BK
	family_code VARCHAR(10) UNIQUE NOT NULL,
	family_type VARCHAR(25) NOT NULL,
	years SMALLINT NOT NULL,
	province_code CHAR(5) NOT NULL,
	province_name VARCHAR(50),
	district_code CHAR(5) NOT NULL,
	district_name VARCHAR(50),
	ward_code CHAR(5) NOT NULL,
	ward_name VARCHAR(50),
	family_number VARCHAR(20),
	nation_in_place BOOL,
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(FamilyKey)
);



/*** 
	Tạo các Dimension chứa thông tin Thành viên Hộ gia đình Rà soát
	Incremental Load
***/

CREATE TABLE IF NOT EXISTS hongheo."DimFamilyMember" (
	MemberKey uuid DEFAULT gen_random_uuid(),	--PK
	member_id uuid NOT NULL,	-- BK
	family_id uuid NOT NULL,	-- FK
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
	education_level hongheo.EDU_LEVEL,
	culture_level hongheo.CUL_LEVEL,
	training_level hongheo.TRAIN_LEVEL,
	has_medical_insurance BOOL,
	social_assistance hongheo.TCXH,
	has_job hongheo.JOB_STATUS,
	job_type hongheo.JOB_CATE,
	has_contract hongheo.CONTRACT_TYPE,
	has_pension hongheo.PENSION_TYPE,
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(MemberKey)
);



/*** 
	Tạo các Dimension chứa thông tin B1 của từng Hộ tham gia các kỳ khảo sát
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS hongheo."DimSurvey" 
(
	SurveyKey uuid DEFAULT gen_random_uuid(),
	family_id uuid NOT NULL,
	year INT,
	month INT,
	a_id uuid NOT NULL,
	fast_classify_person VARCHAR(35),
	condition_codes CHAR(5)[] NOT NULL,
	condition_names VARCHAR(255)[] NOT NULL,
	b1_id uuid NOT NULL,
	is_aquaculture BOOL,
	electricity_source CHAR(5),
	water_source CHAR(5),
	reason_names VARCHAR(255)[] NOT NULL,
	get_policy_names VARCHAR(255)[] NOT NULL,
	need_policy_names VARCHAR(255)[] NOT NULL,
	a_grade BOOL NOT NULL,
	b1_grade SMALLINT NOT NULL,
	b2_grade SMALLINT NOT NULL,
	final_result hongheo.CLASSIFICATION,
	classify_person VARCHAR(35),
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(SurveyKey)
);



/****** Tạo các Schema lưu trữ dữ liệu tích hợp ATVS Lao động ******/


CREATE SCHEMA atvsld;
GRANT ALL ON SCHEMA atvsld TO postgres;


--
-- enum của Loại đơn vị
--

CREATE TYPE atvsld.unit_type AS ENUM (
	'doanh nghiệp',
	'đơn vị huấn luyện',
	'đơn vị kiểm định',
	'đơn vị huấn luyện và kiểm định',
	'hợp tác xã'
);


/***
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS atvsld."DimCompany"
(
    CompanyKey uuid DEFAULT gen_random_uuid(),
	company_id uuid NOT NULL,
    name VARCHAR(35) NOT NULL,
    tax_number VARCHAR(15) NOT NULL,
	unit atvsld.unit_type NOT NULL,
    business_name VARCHAR(35) NOT NULL,
    major_name VARCHAR(35) NOT NULL,
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
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
    PRIMARY KEY(CompanyKey)
);




/***
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/



CREATE TABLE IF NOT EXISTS atvsld."DimEquipment"
(
	DeviceKey uuid DEFAULT gen_random_uuid(),
	device_id uuid NOT NULL,
	device_name VARCHAR(35) NOT NULL,
	quality_info_id uuid,
	company_id uuid NOT NULL,
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
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(DeviceKey)
);



/***
	Tạo các Dimension lữu trữ thông tin thông tin chi tiết từng vụ tai nạn
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS atvsld."DimAccident"
(
    AccidentKey uuid DEFAULT gen_random_uuid(),
	accident_id uuid NOT NULL,
	reason VARCHAR(255) NOT NULL,
	factor VARCHAR(255) NOT NULL,
	career VARCHAR(255) NOT NULL,
	year CHAR(10) NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    period CHAR(10) NOT NULL,
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
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(AccidentKey)
);




/*** 
	Tạo các Dimension chứa thông tin Thời gian
	Full Load hoặc No Load
***/


CREATE TABLE IF NOT EXISTS public."DimDate"
(
    DateKey INT, -- PK: month + year
	month INT,
    month_num_overall INT,
    month_name VARCHAR(255),
    month_abbrev VARCHAR(255),
    quarter INT,
    year INT,
	fiscal_month INT,
    fiscal_quarter INT,
    fiscal_year INt,
    PRIMARY KEY(DateKey)
);



/*** 
	Tạo các Dimension chứa thông tin Vị trí địa lý
	Full Load hoặc No Load
***/


-- CREATE TABLE IF NOT EXISTS public."DimLocation"
-- (
--     LocationKey INT, -- PK: province + district
-- 	province_code CHAR(5),
-- 	province_name VARCHAR(50) NOT NULL,
-- 	district_code CHAR(5),
-- 	district_name VARCHAR(50) NOT NULL,
-- 	region_code CHAR(5),
-- 	region_name VARCHAR(50) NOT NULL,
--     PRIMARY KEY(LocationKey)
-- );


CREATE TABLE IF NOT EXISTS public."DimAudit"
(
    id uuid DEFAULT gen_random_uuid(),
	process_name VARCHAR(35),
	start_at TIMESTAMP,
	finished_at TIMESTAMP,
	information VARCHAR(255),
	status VARCHAR(10) CHECK (status IN('SUCCESS', 'ERROR', 'PENDING')),
    PRIMARY KEY(id)
);
INSERT INTO "DimAudit"(process_name, start_at, finished_at, status) VALUES
('data integration', NULL, '2019-12-31 00:00:00', 'SUCCESS');




CREATE SCHEMA ncc;
GRANT ALL ON SCHEMA ncc TO postgres;


--
-- enum của loại chi trả ữu đãi (NCC)
--

CREATE TYPE ncc.spendtype AS ENUM
(
	'một lần',
	'hàng tháng',
	'hàng năm',
	'bổ sung'
);


/*** 
	Tạo các Dimension chứa thông tin DimNCC
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS ncc."DimNCC"
(
	ProfileKey uuid DEFAULT gen_random_uuid(),
	profile_code CHAR(10) NOT NULL,
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
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(ProfileKey)
);



/*** 
	Tạo các Dimension chứa thông tin DimSubsidy
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS ncc."DimSubsidy"
(
	SubsidyKey uuid DEFAULT gen_random_uuid(),
	province_code CHAR(5),
	district_code CHAR(5),
	profile_code CHAR(10) NOT NULL,
	ncc_code CHAR(10) NOT NULL,
	full_name VARCHAR(35) NOT NULL,
	birth_of_date DATE,
	sex BOOL,
	ethnic VARCHAR(15),
	decided_monthly_date DATE,
	decided_once_date DATE,
	subsidy_code CHAR(10) NOT NULL,
	year INT NOT NULL,
	spend_type ncc.spendtype,
	subsidy_name VARCHAR(255),
	subsidy_money FLOAT8,
	submoney FLOAT8,
	recieve_date DATE NOT NULL,
	RowIsCurrent BOOL NOT NULL,
	RowStartDate TIMESTAMP NOT NULL,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(SubsidyKey)
);



/****** --- FACT --- ******/



/*** 
	Tạo các Dimension chứa thông tin Fact Subsidy Reporting
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, SurveyKey, LocationKey, family_code, owner_name, province_name, district_name
***/


CREATE TABLE IF NOT EXISTS ncc."SubsidyReportFact"
(
	ProfileKey uuid,
	SubsidyKey uuid,
	DateKey INT,
	province_code CHAR(5),
	district_code CHAR(5),
	profile_code CHAR(10) NOT NULL,
	ncc_code CHAR(10) NOT NULL,
	full_name VARCHAR(35) NOT NULL,
	ethnic VARCHAR(15),
	subsidy_code CHAR(10) NOT NULL,
	year INT NOT NULL,
	spend_type ncc.spendtype,
	subsidy_name VARCHAR(255),
	subsidy_money FLOAT8 NOT NULL,
	submoney FLOAT8,
	spend_diff FLOAT8,
	recieve_days INT,
	PRIMARY KEY(ProfileKey, SubsidyKey),
	CONSTRAINT FK_SubsidyReportFact_DimNCC FOREIGN KEY(ProfileKey) REFERENCES ncc."DimNCC"(ProfileKey),
	CONSTRAINT FK_SubsidyReportFact_DimSubsidy FOREIGN KEY(SubsidyKey) REFERENCES ncc."DimSubsidy"(SubsidyKey),
	CONSTRAINT FK_SubsidyReportFact_DimDate FOREIGN KEY(DateKey) REFERENCES public."DimDate"(DateKey)
);



/*** 
	Tạo các Dimension chứa thông tin Fact Poverty Status
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, SurveyKey, LocationKey, family_code, owner_name, province_name, district_name
***/


CREATE TABLE IF NOT EXISTS hongheo."PovertyStatusFact" 
(
    FamilyKey uuid NOT NULL, -- FK
	SurveyKey uuid NOT NULL, -- FK
	IsDeleted BOOL DEFAULT 'false',
	year SMALLINT,
	province_name VARCHAR(35),
	district_name VARCHAR(35),
	family_id  uuid NOT NULL,
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
	final_result hongheo.CLASSIFICATION,
    PRIMARY KEY(FamilyKey, SurveyKey),
	CONSTRAINT FK_PovertyStatusFact_DimFamily FOREIGN KEY(FamilyKey) REFERENCES hongheo."DimFamily"(FamilyKey),
	CONSTRAINT FK_PovertyStatusFact_DimSurvey FOREIGN KEY(SurveyKey) REFERENCES hongheo."DimSurvey"(SurveyKey)
);

CREATE INDEX ownername_idx ON hongheo."PovertyStatusFact" USING SPGIST(owner_name);
CREATE INDEX provincename_PovertyStatusFact_idx ON hongheo."PovertyStatusFact" USING SPGIST(province_name);
CREATE INDEX districtname_PovertyStatusFact_idx ON hongheo."PovertyStatusFact" USING SPGIST(district_name);


/*** 
	Tạo các Dimension chứa thông tin Thành viên tham gia rà soát
	Full Load hoặc No Load
	Điều kiện: hongheo_classify_result.status = 'Đã rà soát'
	Index: FamilyKey, MemberKey, owner_name, member_name, province_name, district_name, age
***/


CREATE TABLE IF NOT EXISTS hongheo."MemberSurveyFact" 
(
    FamilyKey uuid NOT NULL, -- FK
	MemberKey uuid NOT NULL, -- FK
	DateKey INT NOT NULL, -- FK
	IsDeleted BOOL DEFAULT 'false',
	year INT,
	month INT,
	province_name VARCHAR(35),
	district_name VARCHAR(35),
	member_id  uuid NOT NULL,
	family_id uuid NOT NULL,
	owner_name VARCHAR(35),
	member_name VARCHAR(35) NOT NULL,
	owner_relationship VARCHAR(15),
	year_of_birth SMALLINT,
	month_of_birth SMALLINT,
	day_of_birth SMALLINT,
	age SMALLINT,
	identity_card_number VARCHAR(12),
	nation VARCHAR(15),
	final_result hongheo.CLASSIFICATION,
    PRIMARY KEY(FamilyKey, MemberKey, DateKey),
	CONSTRAINT FK_MemberSurveyFact_DimFamily FOREIGN KEY(FamilyKey) REFERENCES hongheo."DimFamily"(FamilyKey),
	CONSTRAINT FK_MemberSurveyFact_DimFamilyMember FOREIGN KEY(MemberKey) REFERENCES hongheo."DimFamilyMember"(MemberKey),
	CONSTRAINT FK_MemberSurveyFact_DimDate FOREIGN KEY(DateKey) REFERENCES public."DimDate"(DateKey)
);

CREATE INDEX membername_idx ON hongheo."MemberSurveyFact" USING SPGIST(member_name);
CREATE INDEX ownerrelationship_idx ON hongheo."MemberSurveyFact" USING SPGIST(owner_relationship);
CREATE INDEX nation_idx ON hongheo."MemberSurveyFact"(nation);
CREATE INDEX age_idx ON hongheo."MemberSurveyFact"(age);
CREATE INDEX yearofbirth_idx ON hongheo."MemberSurveyFact"(year_of_birth);


/*** 
	Tạo các Dimension chứa thông tin Từng vụ tai nạn trong Tai nạn Định kỳ
	Full Load hoặc No Load
	Index: company_name, compay_foreign_name, reason, career
***/


CREATE TABLE IF NOT EXISTS atvsld."PeriodicalAccidentFact" 
(
	AccidentKey uuid NOT NULL, -- FK
    CompanyKey uuid NOT NULL, -- FK
	DateKey INT NOT NULL, -- FK
	IsDeleted BOOL DEFAULT 'false',
	accident_id uuid NOT NULL,
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
    PRIMARY KEY(AccidentKey, CompanyKey, DateKey),
	CONSTRAINT FK_PeriodicalAccidentFact_DimAccident FOREIGN KEY(AccidentKey) REFERENCES atvsld."DimAccident"(AccidentKey),
	CONSTRAINT FK_PeriodicalAccidentFact_DimCompany FOREIGN KEY(CompanyKey) REFERENCES atvsld."DimCompany"(CompanyKey),
	CONSTRAINT FK_PeriodicalAccidentFact_DimDate FOREIGN KEY(DateKey) REFERENCES public."DimDate"(DateKey)
);

CREATE INDEX companyname_idx ON atvsld."PeriodicalAccidentFact" USING SPGIST(company_name);
CREATE INDEX factor_idx ON atvsld."PeriodicalAccidentFact" USING SPGIST(factor);
CREATE INDEX reason_idx ON atvsld."PeriodicalAccidentFact" USING SPGIST(reason);
CREATE INDEX career_idx ON atvsld."PeriodicalAccidentFact" USING SPGIST(career);





