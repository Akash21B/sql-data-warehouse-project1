/*
===========================================================
  Project: Modern Data Warehouse - Silver Layer Schema
  Description:
    This script defines the schema for the Silver Layer 
    of a Data Warehouse using SQL Server. It includes 
    cleaned and structured tables sourced from various 
    CRM and ERP systems.

  Tables Created:
    - silver.crm_cust_info       : Cleaned customer master data
    - silver.crm_prd_info        : Standardized product information
    - silver.crm_sales_details   : Transformed sales transaction details
    - silver.erp_CUST_AZ12       : Customer birthdate and gender info
    - silver.erp_LOC_A101        : Customer location data
    - silver.erp_PX_CAT_G1V2     : Product category and maintenance metadata

  Key Features:
    - Uses IF OBJECT_ID to drop existing tables safely
    - Standardizes data types (e.g., DATE, NVARCHAR, INT)
    - Includes ETL metadata column `dwh_create_date` with default timestamp
    - Follows Data Vault style naming (e.g., sls_, cst_, prd_ prefixes)

===========================================================
*/


IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status	NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date	DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_LOC_A101','U') IS NOT NULL
	DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

