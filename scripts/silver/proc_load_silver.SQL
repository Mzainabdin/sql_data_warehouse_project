/*
==================================================================
Store Procedure: Load Silver Layer (Bronze -> silver ) 
==================================================================
Purpose:
       This script perform ETL process to load data from bronze 
       to silver schema tables.
*****************************************************************
Main Action:
  Truncate: Remove already existing data
  Insert:   Insert data from bronze to silver
****************************************************************
  Sub Action:
    Cleaning:   Before loading from bronze, clean bad data.
    New Columns:Create new columns as per the requirement of
    Data Modeling.
***************************************************************
Parameters: 
    None
    This store procedure doesn't accept any parameters or
    return any values.
**************************************************************
Usage Example:
    EXEC silver.load_silver
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time  DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=============================='
		PRINT'Loading Silver Layer'
		PRINT'=============================='

		PRINT'=============================='
		PRINT'Loading Loading CRM Tables'
		PRINT'=============================='
		--crm_cust_info
		SET @start_time = GETDATE()
		Print 'crm_cust_info'
		PRINT 'Truncate: silver.crm_cust_info:'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT 'Load: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) cst_firstname,
		TRIM(cst_lastname) cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date
		FROM
		(
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) as flag_list
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_list = 1
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
		--crm_prd_info
		SET @start_time = GETDATE()
		Print 'crm_product_info'
		PRINT 'Truncate: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT 'Load: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(
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
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN  'M' THEN 'Mountain'
					WHEN  'R' THEN 'Road'
					WHEN  'S' THEN 'Other Sales'
					WHEN  'M' THEN 'Touring'
					ELSE 'N/A' 
				END prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CAST (DATEADD(DAY,-1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
		--crm_sales_details
		SET @start_time = GETDATE()
		PRINT 'crm_sales_details'
		PRINT 'Truncate: crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT 'Load: crm_sales_details'
		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
		sls_ord_num, 
		sls_prd_key, --No Transformation needed
		sls_cust_id, --No Transformation needed
		CASE 
			WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE)
		END sls_order_dt, --Replace 0 and less than date numbering with NULL. Cast sls_order_date AS VARCHAR THAN DATE
		CASE 
			WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE)
		END sls_ship_dt, --Replace 0 and less than date numbering with NULL. Cast sls_order_date AS VARCHAR THAN DATE
		CASE 
			WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE)
		END sls_due_dt, --Replace 0 and less than date numbering with NULL. Cast sls_order_date AS VARCHAR THAN DATE
		CASE
			WHEN sls_sales < 0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales,--Sales corrected by replacing invalid sales with accurate by price and qty multiplication
		sls_quantity, --No Transformation
		CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL 
		THEN sls_sales/ sls_quantity --COrrected by dividing sale with qty 
		ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
	
		--ERP DATA LOAD
		PRINT'=============================='
		PRINT'Loading ERP Tables'
		PRINT'=============================='
		--erp_cust_az12
		SET @start_time = GETDATE()
		PRINT 'erp_cust_az12'
		PRINT 'TRUNCATE: erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT 'Load: erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL 
			ELSE bdate 
		END bdate,
		CASE gen
			WHEN  'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			WHEN 'Female' THEN 'Female'
			WHEN 'Male' THEN 'Male'
			ELSE 'N/A'
		END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
		--crm_erp_loc_a101
		SET @start_time = GETDATE()
		PRINT 'erp_loc_a101'
		PRINT 'TRUNCATE: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT 'LOAD: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
			cid,
			cntry
		)
		SELECT 
		REPLACE(cid, '-', '') AS cid,
		CASE
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			ELSE TRIM(cntry)
		END cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
		--erp_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT 'erp_px_cat_g1v2'
		PRINT 'TRUNCATE: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT 'LOAD: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT '>>> Load Duration:'+ CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(50)) + ' milli_seconds'
		PRINT '>>>-----------------'
	
		SET @batch_end_time = GETDATE()
		PRINT '======================================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT 'Total Duration: '+ CAST(DATEDIFF(second,@batch_start_time, @batch_end_time)AS NVARCHAR(50))+' seconds'
		PRINT '======================================='
	
	END TRY
	BEGIN CATCH
		PRINT '======================================='
		PRINT 'ERROR OCCUR DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR); 
		PRINT '======================================='

	END CATCH
END
