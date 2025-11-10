/*
===============================================================================
 🏗️  SCRIPT: 01_load_bronze_procedure.sql
-------------------------------------------------------------------------------
 📁  PURPOSE:
     This stored procedure (`bronze.load_bronze`) is responsible for creating 
     all base/landing tables under the **Bronze Layer** of the Data Warehouse.
     The Bronze Layer holds raw ingested data from multiple source systems 
     such as CRM and ERP, before any transformation or cleansing.

 ⚙️  FUNCTIONALITY:
     - Drops existing Bronze tables if they already exist.
     - Recreates all Bronze layer tables with the correct structure.
     - Prepares environment for bulk data loading in subsequent ETL steps.

 📦  TABLES CREATED:
     - bronze.crm_cust_info
     - bronze.crm_prd_info
     - bronze.crm_sales_details
     - bronze.erp_loc_a101
     - bronze.erp_loc_az12
     - bronze.erp_px_cat_g1v2

 🚀  EXECUTION ORDER:
     1. Run `00_init_database.sql`  → creates database & schemas
     2. Run this script             → creates Bronze tables
     3. Run subsequent Silver/Gold layer scripts

 ⚠️  WARNING:
     Executing this procedure will DROP and RECREATE all Bronze tables.
     Do NOT execute on production databases without a backup.

 📅  CREATED BY : Shiv  
 🧠  MAINTAINED BY : Shiv (Data Engineering Practice)
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
 DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT ,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50) , 
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE

);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
  prd_id       INT,
  prd_key      NVARCHAR(50),
  prd_nm       NVARCHAR(50),
  prd_cost     INT,
  prd_line     NVARCHAR(50),
  prd_start_dt DATETIME,
  prd_end_dt   DATETIME

  );

  
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
  CREATE TABLE bronze.crm_sales_details(
  sls_ord_num     NVARCHAR(50),
  sls_prd_key     NVARCHAR(50),
  sls_cust_id     INT,
  sls_order_dt    DATE,
  sls_ship_dt     DATE,
  sls_due_dt      DATE,
  sls_sales       INT,
  sls_quantity    INT,
  sls_price       INT
  );

  
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;

  CREATE TABLE bronze.erp_loc_a101(
  cid  NVARCHAR(50),
  cntry  NVARCHAR(50)
  
 );

   
IF OBJECT_ID ('bronze.erp_loc_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_az12;
  CREATE TABLE bronze.erp_loc_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
  );

  IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
  DROP TABLE bronze.erp_px_cat_g1v2;
	CREATE TABLE bronze.erp_px_cat_g1v2(
	id          NVARCHAR(50),
	cat         NVARCHAR(50),
	subcat	   NVARCHAR(50),
	maintenance  NVARCHAR(50)
  
  );

  END
GO



