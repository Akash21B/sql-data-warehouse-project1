/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================

Purpose:
--------
This stored procedure automates the daily data ingestion process for the 
Bronze Layer in the data warehouse. It performs the following:

- Truncates existing CRM and ERP staging tables in the bronze schema.
- Bulk inserts fresh data from CSV files into respective bronze tables.
- Prints detailed ETL process logs, including table-level load durations.
- Tracks and prints the total batch execution time.
- Implements TRY...CATCH for error handling with meaningful messages.

Usage:
------
Execute daily as part of the ETL pipeline to populate bronze tables with 
raw data from source systems.

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@start_time_batch DATETIME,@end_time_batch DATETIME
	BEGIN TRY	
		SET @start_time_batch = GETDATE();
		PRINT '================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================';

		PRINT '--------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';

		PRINT '--------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>> Inserting Data Into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\akash\SQL 30 hours\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @end_time_batch = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';
		PRINT 'TIME FOR WHOLE BRONZE BATCH TO LOAD: '+CAST(DATEDIFF(second,@start_time_batch,@end_time_batch) AS NVARCHAR)+' SECONDS';
	END TRY
	BEGIN CATCH
		PRINT '================================';
		PRINT 'ERROR OCCURED DUEING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE '+ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE ' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE ' +CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
