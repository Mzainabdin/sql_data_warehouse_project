/*
====================================================================
Store Procedure : Load Bronze layer (Source -> Bronze)
====================================================================
Script Purpose:
    Load data into bronze schema from external CSV files.
    It perform the following actions.
       - Truncate the bronze table before loading data.
       - Uses the 'Bulk Insert' command to load data from csv to bronze tables.
Parameters:
   * None
   * The store procedure doesn't accept any parameters or return any values.
Usage Example:
   EXECUTE bronze.load_bronze
*/
CREATE OR ALTER PROCEDURE bronze.load_procedure AS
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		Print '=====================';
		Print 'Loading Bronze Layer';
		Print '=====================';

		Print '**********************';
		Print 'Loading CRM Tables';
		Print '**********************';
		--Start
		SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.crm_cust_info<<<';
		TRUNCATE TABLE bronze.crm_cust_info 
		Print '>>>Inserting Data into: bronze.crm_cust_info<<<';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '*******************************'
		--END
		--START
		SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.crm_prd_info<<<';
		TRUNCATE TABLE bronze.crm_prd_info
		Print '>>>Inserting Data into: bronze.crm_prd_info<<<';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		--END

		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '*******************************'
		--START
		SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.crm_sales_details<<<';
		TRUNCATE TABLE bronze.crm_sales_details
		Print '>>>Inserting Data into: bronze.crm_sales_details<<<';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '*******************************'
		--END
		Print '**********************';
		Print 'Loading ERP Tables';
		Print '**********************';
		SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.erp_cust_az12<<<';
		TRUNCATE TABLE bronze.erp_cust_az12
		Print '>>>Inserting Data into: bronze.erp_cust_az12<<<';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp/CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
			SET @end_time = GETDATE();
			PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
			PRINT '*******************************'
			--END
			--START
			SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.erp_loc_a101<<<';
		TRUNCATE TABLE bronze.erp_loc_a101
		Print '>>>Inserting Data into: bronze.erp_loc_a101<<<';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp/LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
			SET @end_time = GETDATE();
			PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
			PRINT '*******************************'
			--END
			--START
			SET @start_time = GETDATE();
		Print '>>>Truncating Table: bronze.erp_px_cat_g1v2<<<';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		Print '>>>Inserting Data into: bronze.erp_px_cat_g1v2<<<';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp/PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
        PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '*******************************'
		--END
		SET @batch_end_time = GETDATE();
		PRINT '*******************************'
		PRINT 'Load Bronze Layer Completed'
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '*******************************'
	END TRY
	BEGIN CATCH
		PRINT '================================='
		PRINT 'Error Occured During Bronze Layer'
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR :' + CAST (ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'ERROR :' + CAST (ERROR_STATE() AS NVARCHAR(50));
		PRINT '================================='
	END	CATCH
END;
