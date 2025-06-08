-- L2_invoice
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L2.L2_invoice` AS 
SELECT
 invoice_id --PK
 ,invoice_previous_id
 ,contract_id --FK
 ,invoice_type 
-- pokud je záporná hodnota v amount, tak nastavím hodnotu 0
 ,CASE 
  WHEN amount_w_vat < 0 THEN 0
  ELSE amount_w_vat
 END AS amount_w_vat
-- dopočet bez daně
 ,IF(amount_w_vat < 0, 0, amount_w_vat / 1.2) AS amount_wo_vat
 ,return_w_vat
 ,flag_invoice_issued
 ,date_issue
 ,due_date
 ,paid_date
 ,start_date
 ,end_date
 ,insert_date
 ,update_date
-- přiřadit pořadí konkrétní invoice 
 ,ROW_NUMBER() OVER(PARTITION BY contract_id ORDER BY date_issue) AS invoice_order
FROM `utility-terrain-455612-s3.L1.L1_invoice` 
WHERE invoice_type ="invoice"
 AND flag_invoice_issued = TRUE

;-- L2_product_purchase
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L2.L2_product_purchase` AS 
SELECT
 product_purchase_id -- PK
 ,contract_id -- FK
 ,product_id -- FK
 ,product_name
 ,product_type
 ,product_category
 ,product_status
 ,price_wo_vat_usd
-- dopočítat cenu s daní
 ,IF(price_wo_vat_usd < 0, 0, price_wo_vat_usd * 1.2) AS price_w_vat_usd
 ,product_valid_from
 ,product_valid_to
-- vytvořit flag_unlimited_product
 ,IF(product_valid_from = '2035-12-31', TRUE, FALSE) AS flag_unlimited_product 
 ,unit
 ,create_date
 ,update_date
FROM `utility-terrain-455612-s3.L1.L1_product_purchase` 
WHERE product_category in ("product", "rent")
 AND product_status IS NOT NULL AND product_status not in ("canceled", "canceled registration", "disconnected")

;-- L2_contract
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L2.L2_contract` AS 
SELECT
 contract_id -- PK
 ,branch_id -- FK
 ,contract_valid_from
 ,contract_valid_to
 ,contract_status
 ,registred_date
 ,registration_end_reason
 ,prolongation_date
 ,flag_prolongation
 ,activation_process_date
 ,signed_date
 ,flag_send_email
FROM `utility-terrain-455612-s3.L1.L1_contract` 
WHERE registred_date IS NOT NULL -- nesmí být prázdné


;-- L2_branch
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L2.L2_branch` AS 
SELECT
 branch_id -- PK
 ,branch_name
FROM `utility-terrain-455612-s3.L1.L1_branch` 
WHERE branch_name != "unknown" -- nesmí být "unknown"


;-- L2_product
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L2.L2_product` AS 
SELECT
 product_id -- PK
 ,product_name
 ,product_type
 ,product_category
FROM `utility-terrain-455612-s3.L1.L1_product` 
WHERE product_category in ("product", "rent") -- omezení kategorií

