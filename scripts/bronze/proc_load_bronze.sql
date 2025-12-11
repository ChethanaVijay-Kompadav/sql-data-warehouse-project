/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER  PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @END_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '================================================';
		PRINT 'Loading BRONZE Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT ' Loading CRM Tables';
		PRINT '>> ------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE [bronze].[crm_cust_info];

		PRINT ' >> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT TOP 5 * FROM [bronze].[crm_cust_info];
		--SELECT COUNT(*) FROM [bronze].[crm_cust_info];

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.crm_prod_info';
		TRUNCATE TABLE [bronze].[crm_prod_info];

		PRINT ' >> Inserting Data into: bronze.crm_prod_info';
		BULK INSERT [bronze].[crm_prod_info]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT TOP 5 * FROM [bronze].[crm_prod_info];
		--SELECT COUNT(*) FROM [bronze].[crm_prod_info];

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE [bronze].[crm_sales_details];

		PRINT ' >> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT TOP 5 * FROM [bronze].[crm_sales_details];
		--SELECT COUNT(*) FROM [bronze].[crm_sales_details];

		PRINT '------------------------------------------------';
		PRINT ' Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE [bronze].[erp_cust_az12];

		PRINT ' >> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT  * FROM [bronze].[erp_cust_az12];
		--SELECT COUNT(*) FROM [bronze].[erp_cust_az12];

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101];

		PRINT ' >> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT TOP 5 * FROM [bronze].[erp_loc_a101];
		--SELECT COUNT(*) FROM [bronze].[erp_loc_a101];

		SET @start_time = GETDATE()
		PRINT ' >> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

		PRINT ' >> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'K:\SQL2022\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------------------';
		--GO
	
		--SELECT TOP 5 * FROM [bronze].[erp_px_cat_g1v2];
		--SELECT COUNT(*) FROM [bronze].[erp_px_cat_g1v2];
		SET @batch_end_time = GETDATE()

		PRINT '================================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================';

	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
