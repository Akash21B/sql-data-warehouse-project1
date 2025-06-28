/*
====================================================================
  Stored Procedure: silver.load_silver
  Description:
    This procedure loads and transforms data from the Bronze Layer 
    to the Silver Layer in a SQL Server-based Data Warehouse.

  Highlights:
    - Truncates and reloads Silver Layer tables with clean, validated data
    - Applies business logic, transformation rules, and data standardization
    - Ensures data quality (e.g., date format, gender mapping, ID trimming)
    - Measures load duration for each table and total batch processing
    - Logs detailed messages for transparency and debugging
    - Catches and prints any runtime errors during execution

  Tables Affected:
    - silver.crm_cust_info
    - silver.crm_prd_info
    - silver.crm_sales_details
    - silver.erp_CUST_AZ12
    - silver.erp_LOC_A101
    - silver.erp_PX_CAT_G1V2

  Best Practices Implemented:
    - Row-level deduplication using ROW_NUMBER
    - Data cleaning with TRIM, CASE, and ISNULL
    - Safe date conversion from integer to DATE
    - Protection against divide-by-zero errors
    - Standardized country and gender values
    - Business rule enforcement for sales, prices, and quantities

====================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME,@start_time_batch DATETIME,@end_time_batch DATETIME
		BEGIN TRY	
			SET @start_time_batch = GETDATE();
			PRINT '================================';
			PRINT 'Loading Silver Layer';
			PRINT '================================';

			PRINT '--------------------------------';
			PRINT 'Loading CRM Tables'
			PRINT '--------------------------------';

			SET @start_time = GETDATE();
		-- Truncating the Data in the silver.crm_cust_info
		TRUNCATE TABLE silver.crm_cust_info;
		-- Inserting the Data in the silver.crm_cust_info
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT cst_id,
			   cst_key,
			   TRIM(cst_firstname) AS cst_firstname,
			   TRIM(cst_lastname) AS cst_lastname,
			   CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'n/a'
			   END cst_material_status,
			   CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'n/a'
			   END cst_gndr,
			   cst_create_date
			  FROM(
		SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as latest
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)t
		WHERE latest = 1;
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		-- Truncating the Data in the silver.crm_prd_info
		TRUNCATE TABLE silver.crm_prd_info;
		-- Inserting the Data in the silver.crm_prd_info
		INSERT INTO silver.crm_prd_info(
						prd_id,
						cat_id,
						prd_key,
						prd_nm,
						prd_cost,
						prd_line,
						prd_start_dt,
						prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'


			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		-- Truncating the Data in the silver.crm_sales_details
		TRUNCATE TABLE silver.crm_sales_details;
		-- Inserting the Data in the silver.crm_sales_details
		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price 
		)
		SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
		
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
				END AS sls_order_dt,
		
				CASE
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
				END AS sls_ship_dt,
		
				CASE
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
				END AS sls_due_dt,
		
				CASE
					WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price)
						THEN sls_quantity*ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
		
				CASE
					WHEN sls_price <=0 OR sls_sales IS NULL 
						THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
				END AS sls_price
		
		  FROM bronze.crm_sales_details
		  SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		  PRINT '--------------------------------';

			PRINT '--------------------------------';
			PRINT 'Loading ERP Tables'
			PRINT '--------------------------------';

			SET @start_time = GETDATE();
		  -- Truncating the Data in the silver.erp_CUST_AZ12
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		-- Inserting the Data in the silver.erp_CUST_AZ12
		INSERT INTO silver.erp_CUST_AZ12(
			CID,
			BDATE,
			GEN
		)
		SELECT
	
			CASE
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID,
	
			CASE
				WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
	
			CASE
				WHEN GEN IS NULL THEN 'N/A'
				WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(GEN)) = '' THEN 'N/A'
				ELSE GEN
			END AS GEN
		from bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		  -- Truncating the Data in the silver.erp_LOC_A101
		TRUNCATE TABLE silver.erp_LOC_A101;
		-- Inserting the Data in the silver.erp_LOC_A101
		INSERT INTO silver.erp_LOC_A101
		(
			CID,
			CNTRY
		)
		SELECT 
				REPLACE(CID,'-','') AS CID,
		
				CASE
					WHEN CNTRY IS NULL OR TRIM(CNTRY)='' THEN 'N/A'
					WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
					WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
					ELSE CNTRY
				END AS CNTRY
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		-- Truncating the Data in the silver.erp_PX_CAT_G1V2
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		-- Inserting the Data in the silver.erp_PX_CAT_G1V2
		INSERT INTO silver.erp_PX_CAT_G1V2(
				ID,
				CAT,
				SUBCAT,
				MAINTENANCE
		)
		select 
				ID,
				CAT,
				SUBCAT,
				MAINTENANCE
		from bronze.erp_PX_CAT_G1V2;
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
