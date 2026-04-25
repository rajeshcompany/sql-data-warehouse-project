----===============================
-- purpose to create gold layer
----=================================
----========================================----
--          First Dimension: Customer
----========================================----

CREATE VIEW gold.dim_customer AS
SELECT
    -- Surrogate key for DW (auto-generated sequence)
    ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key,
    
    -- Business identifiers
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    
    -- Customer attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    
    -- Gender logic: prefer CRM value, fallback to ERP if unknown
    CASE 
        WHEN ci.cst_gndr != 'unknown' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
    END AS gender,
    
    -- Demographics
    ca.bdate AS birthdate,
    
    -- Metadata
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


-----=====================================-----
--          Second Dimension: Products
-----=====================================-----

CREATE VIEW gold.dim_products AS
SELECT
    -- Surrogate key for DW
    ROW_NUMBER () OVER (ORDER BY pn.prd_start_dt, pn.prd_id) AS product_key,
    
    -- Business identifiers
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    
    -- Product attributes
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    
    -- Financials
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    
    -- Metadata
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL   -- Exclude historical/inactive products


---==================================================---
--          Fact Table: Sales
---==================================================---

CREATE VIEW gold.fact_sales AS
SELECT
    -- Transaction identifiers
    sd.sls_ord_num AS order_number,
    
    -- Foreign keys to dimensions
    pr.product_key,
    cu.customer_key,
    
    -- Dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    
    -- Measures
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    ON sd.sls_cust_id = cu.customer_id;
