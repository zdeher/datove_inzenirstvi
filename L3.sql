-- L3_branch
CREATE OR REPLACE TABLE `utility-terrain-455612-s3.L3_marketing.L3_branch` AS 
SELECT 
 branch_id -- PK
 ,branch_name
FROM `utility-terrain-455612-s3.L2.L2_branch` 

;-- L3_contract
CREATE OR REPLACE TABLE `utility-terrain-455612-s3.L3_marketing.L3_contract` AS 
SELECT
 contract_id -- PK
 ,branch_id -- FK
 ,contract_valid_from
 ,contract_valid_to
 ,contract_status
 ,registration_end_reason
 ,flag_prolongation
 ,FORMAT_TIMESTAMP('%Y', contract_valid_from) AS start_year_of_contract --rok začátku smlouvy
 -- délka kontraktu
 ,CASE
  WHEN DATE_DIFF(contract_valid_to, contract_valid_from, DAY) < 183 THEN "less than half year"
  WHEN DATE_DIFF(contract_valid_to, contract_valid_from, DAY) BETWEEN 183 AND 366 THEN "1 year"
  WHEN DATE_DIFF(contract_valid_to, contract_valid_from, DAY) BETWEEN 367 AND 731 THEN "2 years"
  ELSE "more than two years"
 END AS contract_duration
FROM `utility-terrain-455612-s3.L2.L2_contract` 
WHERE contract_valid_from is not null -- nesmí být prázdné

;-- L3_product
CREATE OR REPLACE TABLE `utility-terrain-455612-s3.L3_marketing.L3_product` AS 
SELECT
 product_purchase_id -- PK
 ,product_id -- FK
 ,contract_id -- FK
 ,product_name
 ,product_type
 ,product_valid_from
 ,product_valid_to
 ,flag_unlimited_product 
 ,unit
FROM `utility-terrain-455612-s3.L2.L2_product_purchase` 

;-- L3_invoice
CREATE OR REPLACE TABLE `utility-terrain-455612-s3.L3_marketing.L3_invoice` AS --faktovka
SELECT
 i.invoice_id -- PK
 ,i.contract_id -- FK
 ,i.amount_w_vat
 ,i.return_w_vat
 ,i.paid_date
 ,amount_w_vat - return_w_vat AS total_paid
 ,pp.product_id
FROM `utility-terrain-455612-s3.L2.L2_invoice` AS i 
LEFT JOIN `utility-terrain-455612-s3.L2.L2_product_purchase` AS pp
 ON i.contract_id = pp.contract_id
WHERE pp.product_id IS NOT NULL 
