-- CREATE DATABASE "NguoiDanDM" WITH ENCODING 'UTF8' TEMPLATE template0;


--
-- enum của loại chi trả ữu đãi (NCC)
--

CREATE TYPE public.spendtype AS ENUM
(
	'một lần',
	'hàng tháng',
	'hàng năm',
	'bổ sung'
);


--
-- enum của phân loại hộ gia đình
--

CREATE TYPE public.CLASSIFICATION AS ENUM (
	'hộ nghèo',
	'hộ cận nghèo',
	'hộ không nghèo',
	'N/A'
);


--
-- enum của trình độ học vấn
--

CREATE TYPE public.EDU_LEVEL AS ENUM (
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

CREATE TYPE public.CUL_LEVEL AS ENUM (
	'chưa tốt nghiệp tiểu học',
	'tiểu học',
	'trung học cơ sở',
	'trung học phổ thông'
);

--
-- enum của trình độ đào tạo
--

CREATE TYPE public.TRAIN_LEVEL AS ENUM (
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

CREATE TYPE public.TCXH AS ENUM (
	'người cao tuổi',
	'người khuyết tật',
	'trẻ em mồ côi'
);

--
-- enum của trạng thái nghề nghiệp
--

CREATE TYPE public.JOB_STATUS AS ENUM (
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

CREATE TYPE public.JOB_CATE AS ENUM (
	'công chức viên chức',
	'làm cho doanh nghiệp',
	'làm thuê cho hộ gia đình',
	'khác'
);

--
-- enum của loại hợp đồng lao động
--

CREATE TYPE public.CONTRACT_TYPE AS ENUM (
	'có hợp đồng lao động',
	'không có hợp đồng lao động',
	'không làm công ăn lương'
);

--
-- enum của loại lương hưu
--

CREATE TYPE public.PENSION_TYPE AS ENUM (
	'đang hưởng lương hưu',
	'đang hưởng trợ cấp bảo hiểm xã hội hằng tháng',
	'đang hưởng trợ cấp người có công hằng tháng'
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


CREATE TABLE IF NOT EXISTS public."DimLocation"
(
    LocationKey INT, -- PK: province + district
	province_code CHAR(5),
	province_name VARCHAR(50) NOT NULL,
	district_code CHAR(5),
	district_name VARCHAR(50) NOT NULL,
	region_code CHAR(5),
	region_name VARCHAR(50) NOT NULL,
    PRIMARY KEY(LocationKey)
);



/*** 
	Tạo các Dimension chứa thông tin DimNCC
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS public."DimNCC"
(
	ProfileKey uuid DEFAULT gen_random_uuid(),
	profile_code CHAR(10),
	ncc_code CHAR(10),
	-- Common
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
	RowIsCurrent BOOL,
	RowStartDate TIMESTAMP,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(ProfileKey)
);
INSERT INTO "DimNCC"(profilekey,profile_code, start_subsidize, support_bhyt, home_town) VALUES('00000000-0000-0000-0000-000000000000','N/A','2020-01-01','FALSE','N/A');


/*** 
	Tạo các Dimension chứa thông tin DimSubsidy
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS public."DimSubsidy"
(
	SubsidyKey uuid DEFAULT gen_random_uuid(),
	province_code CHAR(5),
	district_code CHAR(5),
	profile_code CHAR(10),
	ncc_code CHAR(10),
	full_name VARCHAR(35),
	birth_of_date DATE,
	sex BOOL,
	ethnic VARCHAR(15),
	decided_monthly_date DATE,
	decided_once_date DATE,
	subsidy_code CHAR(10),
	year INT,
	spend_type public.spendtype,
	subsidy_name VARCHAR(255),
	subsidy_money FLOAT8,
	submoney FLOAT8,
	recieve_date DATE,
	RowIsCurrent BOOL,
	RowStartDate TIMESTAMP,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(SubsidyKey)
);



CREATE TABLE IF NOT EXISTS public."DimSurvey" 
(
	SurveyKey uuid DEFAULT gen_random_uuid(),
	family_id uuid,
	years SMALLINT,
	a_id uuid,
	fast_classify_person VARCHAR(35),
	condition_codes CHAR(5)[],
	condition_names VARCHAR(255)[],
	b1_id uuid,
	is_aquaculture BOOL,
	electricity_source CHAR(5),
	water_source CHAR(5),
	reason_codes CHAR(10)[],
	reason_names VARCHAR(255)[],
	get_policy_names VARCHAR(255)[],
	need_policy_names VARCHAR(255)[],
	a_grade BOOL,
	b1_grade SMALLINT,
	b2_grade SMALLINT,
	final_result public.CLASSIFICATION,
	classify_person VARCHAR(35),
	RowIsCurrent BOOL,
	RowStartDate TIMESTAMP,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(SurveyKey)
);
INSERT INTO "DimSurvey"(surveykey,family_id, a_grade, b1_grade, b2_grade, final_result) VALUES('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','FALSE',-1,-1,'N/A');


/*** 
	Tạo các Dimension chứa thông tin Thành viên Hộ gia đình Rà soát
	Incremental Load
***/

-- NOTE: Là phân cấp của bảng DimFamily

CREATE TABLE IF NOT EXISTS public."DimFamilyMember" (
	MemberKey uuid DEFAULT gen_random_uuid(),
	member_id uuid,
	family_id uuid,
	full_name VARCHAR(35),
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
	RowIsCurrent BOOL,
	RowStartDate TIMESTAMP,
	RowEndDate TIMESTAMP,
	PRIMARY KEY(MemberKey)
);
INSERT INTO "DimFamilyMember"(memberkey,member_id) VALUES('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000');



/*** 
	Tạo các Dimension chứa thông tin Fact NCC Poverty Reporting
	Full Load hoặc No Load
	Điều kiện: 
		+ DimFamily.years = DimSubsidy.year AND DimFamilyMember.identity_card = DimNCC.identity_number
		+ year vả month extract từ recieve_date
		+ district và province lấy từ family_info
	Index: FamilyKey, SurveyKey, LocationKey, family_code, owner_name, province_name, district_name
***/


CREATE TABLE IF NOT EXISTS public."NccPovertyFact"
(
	MemberKey uuid,
	SurveyKey uuid,
	ProfileKey uuid,
	DateKey INT,
	year INT,
	month INT,
	-- family member
	member_id uuid,
	full_name VARCHAR(35),
	identity_card VARCHAR(12),
	-- family survey info
	family_id uuid,
	a_grade BOOL,
	b1_grade SMALLINT,
	b2_grade SMALLINT,
	final_result public.CLASSIFICATION,
	-- ncc profile
	profile_code CHAR(10),
	ncc_code CHAR(10),
	-- measures
	total_subsidy INT,
	total_money FLOAT8,
	PRIMARY KEY(ProfileKey, SurveyKey, MemberKey, DateKey),
	CONSTRAINT FK_NccPovertyFact_DimNCC FOREIGN KEY(ProfileKey) REFERENCES "DimNCC"(ProfileKey),
	CONSTRAINT FK_NccPovertyFact_DimSurvey FOREIGN KEY(SurveyKey) REFERENCES "DimSurvey"(SurveyKey),
	CONSTRAINT FK_NccPovertyFact_DimFamilyMember FOREIGN KEY(MemberKey) REFERENCES "DimFamilyMember"(MemberKey),
	CONSTRAINT FK_NccPovertyFact_DimDate FOREIGN KEY(DateKey) REFERENCES "DimDate"(DateKey)
);
