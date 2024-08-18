-- CREATE DATABASE "DoanhNghiepDM" WITH ENCODING 'UTF8' TEMPLATE template0;


/***
	Tạo các đối tượng enum để lưu trữ các sự lựa chọn cố định
***/



--
-- enum của Loại đơn vị
--

CREATE TYPE public.unit_type AS ENUM (
	'doanh nghiệp',
	'đơn vị huấn luyện',
	'đơn vị kiểm định',
	'đơn vị huấn luyện và kiểm định',
	'hợp tác xã'
);



CREATE TYPE public.kdcl AS ENUM (
	'bước 1 - đang khai báo',
	'bước 1 - chờ tiếp nhận',
	'bước 1 - từ chối',
	'bước 2 - đang bổ sung',
	'bước 2 - chờ tiếp nhận',
	'bước 2 - từ chối',
	'bước 2 - hoàn thành'
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
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/


CREATE TABLE IF NOT EXISTS public."DimCompany"
(
    CompanyKey uuid DEFAULT gen_random_uuid(),
	company_id uuid NOT NULL,
    name VARCHAR(35) NOT NULL,
    tax_number VARCHAR(15) NOT NULL,
	unit unit_type NOT NULL,
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
	RowEndDate TIMESTAMP NOT NULL,
    PRIMARY KEY (CompanyKey)
);




/***
	Tạo các Dimension lữu trữ thông tin Thiết bị và GCN chất lượng (nếu có)
	Incremental Load
***/



CREATE TABLE IF NOT EXISTS public."DimEquipment"
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
	RowEndDate TIMESTAMP NOT NULL,
	PRIMARY KEY(DeviceKey)
);


/*** 
	Tạo các Dimension trong Data Mart chứa thông tin Kiểm định chất lượng
	Full Load hoặc No Load
	Điều kiện: status LIKE '%từ chối' OR '%hoàn thành'
	Index: device_name, quality_info_id
***/


CREATE TABLE IF NOT EXISTS public."ImportDeviceFact" 
(
	DeviceKey uuid NOT NULL, -- FK
    CompanyKey uuid NOT NULL, -- FK
	DateKey INT NOT NULL, -- FK
	IsDeleted BOOL DEFAULT 'false',
	device_id uuid NOT NULL,
	device_name VARCHAR(35) NOT NULL,
	quality_info_id uuid,
	year INT,
	month INT,
	company_id uuid NOT NULL,
	origin VARCHAR(35),
	quantity INT,
	gcn_number CHAR(15),
	issuance_date DATE,
	quality_organize VARCHAR(35),
	quality_base VARCHAR(35),
	status public.kdcl,
    PRIMARY KEY(DeviceKey, CompanyKey, DateKey),
	CONSTRAINT FK_ImportDeviceFact_DimEquipment FOREIGN KEY(DeviceKey) REFERENCES public."DimEquipment"(DeviceKey),
	CONSTRAINT FK_ImportDeviceFact_DimCompany FOREIGN KEY(CompanyKey) REFERENCES public."DimCompany"(CompanyKey),
	CONSTRAINT FK_ImportDeviceFact_DimDate FOREIGN KEY(DateKey) REFERENCES public."DimDate"(DateKey)
);

CREATE INDEX quality_idx ON public."ImportDeviceFact"(quantity);
CREATE INDEX devicename_idx ON public."ImportDeviceFact" USING SPGIST(device_name);



