/******************************************************************************************
  Script Name  : load_silver.sql
  Procedure    : silver.load_silver
  Description  : Creates and refreshes all Silver Layer tables in the Data Warehouse.
                 The Silver Layer is designed for cleaned, standardized, and 
                 integrated data coming from the Bronze Layer.

  -----------------------------------------------------------------------------------------
  ⚠️ WARNING & IMPORTANT NOTES:
  1. This script DROPS and RE-CREATES all Silver Layer tables.
     ➤ All existing data in these tables will be deleted permanently.

  2. Ensure that the 'silver' schema exists in your database before running this script.
     If not, uncomment the following lines:
         -- CREATE SCHEMA silver;
         -- GO

  3. Recommended:
     - Run this only after the Bronze Layer has been successfully loaded.
     - Execute in the correct database context (USE DataWarehouse;).
     - Review all table structures before modifying for production use.

  Author       : Shiv
  Created On   : 2025-11-10
******************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 

    -- Drop and recreate crm_cust_info
    IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE silver.crm_cust_info;

    CREATE TABLE silver.crm_cust_info (
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50), 
        cst_lastname NVARCHAR(50),
        cst_material_status NVARCHAR(50),
        cst_gndr NVARCHAR(50),
        cst_create_date DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    -- Drop and recreate crm_prd_info
    IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE silver.crm_prd_info;

    CREATE TABLE silver.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt DATETIME,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    -- Drop and recreate crm_sales_details
    IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE silver.crm_sales_details;

    CREATE TABLE silver.crm_sales_details (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    -- Drop and recreate erp_loc_a101
    IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE silver.erp_loc_a101;

    CREATE TABLE silver.erp_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    -- Drop and recreate erp_loc_az12
    IF OBJECT_ID('silver.erp_loc_az12', 'U') IS NOT NULL
        DROP TABLE silver.erp_loc_az12;

    CREATE TABLE silver.erp_loc_az12 (
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    -- Drop and recreate erp_px_cat_g1v2
    IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE silver.erp_px_cat_g1v2;

    CREATE TABLE silver.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

END;
GO
