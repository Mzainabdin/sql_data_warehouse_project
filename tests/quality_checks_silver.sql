/*
==================================================
Quality Checks
==================================================
Script Purpose: 
  This script perform quality check to validate the clean data 
  be transported to silver layer.
  These checks ensures in the silver layer:
      Each primary key is unique no duplicates.
      All columns with string values are trimmed.
      Each Null is replace with N/A
      Distinct data should be in fields like gender marital status etc.
Usage Notes:
      > Run these checks after data loading in silver layer.
      > Resolve any discrpancies during the checks
*/
--============================================
--Silver.crm_cust_info Quality Check
--============================================
--Quality check of cst_id Column
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
--Quality Check of firstname and Lastname 
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
--Data Standardization and Consistency Check
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
--Now check Data in table form
SELECT * FROM silver.crm_cust_info
--============================================
--Silver.crm_prd_info Quality Check
--============================================
--Check the loaded data
SELECT * FROM silver.crm_prd_info
--Check Prd_id
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 OR prd_id IS NULL
--Check unwanted spaces
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
--Check Nulls and negative number
SELECT prd_cost FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL
--Data Standard and Quality
SELECT DISTINCT prd_line FROM silver.crm_prd_info
--Check invalid Dates
SELECT * FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt
--============================================
--Silver.crm_sales_details Quality Check
--============================================
--Check silver.crm_sales_details
SELECT * FROM silver.crm_sales_details
--Check extra spaces
SELECT sls_ord_num FROM silver.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num)
--Check if some values of prd_key and cust_id missing in other table
SELECT * FROM silver.crm_sales_details WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
SELECT * FROM silver.crm_sales_details WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--Check quantity, sls price and sales
SELECT * 
FROM silver.crm_sales_details
WHERE 
	sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity*sls_price OR
	sls_price <= 0 OR sls_sales IS NULL OR
	sls_quantity <= 0 OR sls_quantity IS NULL
--Invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt
FROM bronze.crm_sales_details
--============================================
--Silver.erp_cust_az12 Quality Check
--============================================
--Check silver.erp_cust_az12 for bad data 
--Check bronze.erp_cust_az12 for irrelavant data
SELECT
cid
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)
SELECT * FROM silver.crm_cust_info
--Check invalid Bdate
SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE() 
--Check gender col
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12
--============================================
--Silver.erp_loc_a101 Quality Check
--============================================
--Check Silver
--Check erp_loc_a101
--Irrelavant data in cid col
SELECT cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)
--Check Distinct cntry
SELECT 
DISTINCT cntry old_cntry
FROM silver.erp_loc_a101
--============================================
--Silver.erp_px_cat_g1v2 Quality Check
--============================================
--CHECK invalid cat
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2
--CHECK invalid sub_cat
SELECT subcat
FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)
--Check Maintenance 
SELECT maintenance
FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)
