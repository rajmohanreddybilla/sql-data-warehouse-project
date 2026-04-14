/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO



create or alter procedure bronze.load_bronze as
begin

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 

begin try


SET @batch_start_time = GETDATE();
    print '==========================================';
    print 'Loading bronze layer';
    print '==========================================';

    print '------------------------------------------';
    print 'Loading CRM Tables';
    print '------------------------------------------';

SET @start_time = GETDATE();
print '>> Truncating Table: bronze.crm_cust_info';
truncate table bronze.crm_cust_info;

print'>> Inserting Data Into: bronze.crm_cust_info';
bulk insert bronze.crm_cust_info
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (firstrow  = 2,
  fieldterminator = ',',
  tablock
  );
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



SET @start_time = GETDATE();
print '>> Truncating Table: bronze.crm_prd_info';
truncate table bronze.crm_prd_info;

print'>> Inserting Data Into: bronze.crm_prd_info';
bulk insert bronze.crm_prd_info
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



SET @start_time = GETDATE();
print '>> Truncating Table: bronze.crm_sales_details';
truncate table bronze.crm_sales_details;

print'>> Inserting Data Into: bronze.crm_sales_details';
bulk insert bronze.crm_sales_details
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with ( firstrow = 2, fieldterminator = ',',
tablock);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


    print '------------------------------------------';
    print 'Loading CRM Tables';
    print '------------------------------------------';


SET @start_time = GETDATE();
print '>> Truncating Table: bronze.erp_cust_az12';
truncate table bronze.erp_cust_az12;

print'>> Inserting Data Into: bronze.erp_cust_az12';
bulk insert bronze.erp_cust_az12
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with ( firstrow = 2, fieldterminator = ',', tablock);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


SET @start_time = GETDATE();
print '>> Truncating Table: bronze.erp_loc_a101';
truncate table bronze.erp_loc_a101;

print'>> Inserting Data Into: bronze.erp_loc_a101';
bulk insert bronze.erp_loc_a101
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with ( firstrow = 2, fieldterminator = ',', tablock);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


SET @start_time = GETDATE();
print '>> Truncating Table: bronze.erp_px_cat_g1v2';
truncate table bronze.erp_px_cat_g1v2;

print'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
bulk insert bronze.erp_px_cat_g1v2
from 'E:\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with ( firstrow = 2, fieldterminator = ',', tablock);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

end try
begin catch
        PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
end catch
end;

exec bronze.load_bronze;
