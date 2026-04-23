/*
** loading the data from source system in to bronze layer **
**created stored procedure for daily activity **
**add variables for each table how much time taking to load the data in to tables.**
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
--Declaring the Variables for duration for loading the table
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';

--TABLE:crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info;

		PRINT '>>  Inserting The Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\dwh_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';

--TABLE: crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;
	
		PRINT '>>  Inserting The Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\dwh_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';
--TABLE:crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details;

		PRINT '>>  Inserting The Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\dwh_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

--TABLE:bronze.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12;
	
		PRINT '>>  Inserting The Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\dwh_project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';

--TABLE:bronze.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101;

		PRINT '>>  Inserting The Data Into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\dwh_project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';

--TABLE:bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>>  Truncating Table: bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2;

		PRINT '>>  Inserting The Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\dwh_project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer is Compleated';
		PRINT ' -Total Load Duration : ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==============================================';
	END TRY
	BEGIN CATCH
		PRINT '--------------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------------';
	END CATCH
ENDIF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(15),
	cst_create_date DATE
);
--
IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
--
IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
-----
IF OBJECT_ID ('bronze.erp_cust_az12' , 'U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
	cid NVARCHAR(20),
	bdate DATE,
	gen NVARCHAR(30)
);
--
IF OBJECT_ID ('bronze.erp_loc_a101' , 'U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(30)
);
--
IF OBJECT_ID ('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL 
DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

