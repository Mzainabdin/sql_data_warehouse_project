/*
--===================================================
Quality Checks:
--===================================================
Script Purpose:
	This Check perform quality checks to ensure:
		- The unique customer and product keys and
		  connectivity of fact and dimension tables.
		- Validation of realtionship in data model 
		  for analytical purposes
Usage Notes: 
		- Run these checks after data loading 
			silver layer.
		- Resolve discerpancies found during checks
*/
--==============================================
-- Check Quality gold.dim_customers
--==============================================
-- Check uniqueness of customer keys.
-- Expectation: No result
SELECT 
customer_key,
COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1
--==============================================
-- Check Quality gold.dim_products
--==============================================
-- Check uniqueness of product keys.
-- Expectation: No result
SELECT
product_key,
COUNT(*) duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1
--==============================================
-- Check Quality gold.fact_sales
--==============================================
--Check the data model connectivity between facts and dimesions
SELECT 
f.customer_key,
c.customer_key,
f.product_key,
p.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
