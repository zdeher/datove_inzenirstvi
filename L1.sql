-- L1_google_sheets 
-- L1_branch 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_branch` AS 
SELECT
  CAST(id_branch AS INT) AS branch_id -- PK
  ,LOWER(branch_name) AS branch_name
  ,DATE(TIMESTAMP(PARSE_DATE("%d/%m/%Y", date_update)), "Europe/Prague") AS product_status_update_date 
FROM `utility-terrain-455612-s3.L0_google_sheet.branch`
WHERE id_branch != "NULL"
  AND branch_name != "NULL" -- ID a jméno nesmí být prázdné

;-- L1_product 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_product` AS 
SELECT 
 CAST(id_product AS INT) AS product_id -- PK
 ,LOWER(name) AS product_name
 ,LOWER(type) AS product_type
 ,LOWER(category) AS product_category
 ,DATE(TIMESTAMP(PARSE_DATE("%d/%m/%Y",date_update)), "Europe/Prague") AS product_update_date
 ,CASE 
  WHEN LOWER(is_vat_applicable) = "true" THEN TRUE
  WHEN LOWER(is_vat_applicable) = "false" THEN FALSE
  ELSE NULL
END AS is_vat_applicable
FROM `utility-terrain-455612-s3.L0_google_sheet.product`
WHERE id_product IS NOT NULL -- ID nesmí být prázdné
QUALIFY ROW_NUMBER() OVER(PARTITION BY id_product) = 1 -- nesmí být duplicity

;-- L1_status 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_status` AS
SELECT
 CAST(id_status AS INT) AS product_status_id
 ,LOWER(status_name) AS product_status_name
 ,DATE(TIMESTAMP(PARSE_DATE("%d/%m/%Y",date_update)), "Europe/Prague") AS product_status_update_date
FROM `utility-terrain-455612-s3.L0_google_sheet.status`
WHERE id_status != ""
 AND status_name != "" -- ID a jméno nesmí být prázdné
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_status_id) = 1 -- nesmí být duplicity

;-- L1_accounting_system 
-- L1_invoice 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_invoice` AS
SELECT
 CAST(id_invoice AS INT) AS invoice_id -- PK
 ,CAST(id_invoice_old AS INT) AS invoice_previous_id
 ,CAST(invoice_id_contract AS INT) AS contract_id --FK
 ,CAST(number AS INT) AS invoice_number
 ,CAST(status AS INT) AS invoice_status_id -- FK??
-- Invoice status < 100  have been issued. >= 100 - not issued 
 ,IF(status < 100, TRUE, FALSE) AS flag_invoice_issued
 ,CAST(id_branch AS INT) AS branch_id -- FK
 ,CAST(value AS FLOAT64) AS amount_w_vat
 ,CAST(payed AS FLOAT64) AS amount_paid
 ,CAST(value_storno AS FLOAT64) AS return_w_vat 
 ,CAST(flag_paid_currier AS bool) AS flag_paid_currier
 ,DATE(TIMESTAMP(date), "Europe/Prague") AS date_issue
 ,DATE(TIMESTAMP(scadent), "Europe/Prague") AS due_date
 ,DATE(TIMESTAMP(date_paid), "Europe/Prague") AS paid_date
 ,DATE(TIMESTAMP(start_date), "Europe/Prague") AS start_date
 ,DATE(TIMESTAMP(end_date), "Europe/Prague") AS end_date
 ,DATE(TIMESTAMP(date_insert), "Europe/Prague") AS insert_date
 ,DATE(TIMESTAMP(date_update), "Europe/Prague") AS update_date
 ,CAST(invoice_type AS INT) AS invoice_type_id 
 -- Invoice_type: 1 - invoice, 2 - return, 3 - credit_note, 4 - other
 ,CASE
   WHEN invoice_type = 1 THEN "invoice"
   WHEN invoice_type = 2 THEN "return"
   WHEN invoice_type = 3 THEN "credit_note"
   WHEN invoice_type = 4 THEN "other"
END AS invoice_type,
FROM `utility-terrain-455612-s3.L0_accounting_system.invoice`

;-- L1_invoice_load 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_invoice_load` AS 
SELECT 
 CAST(id_load AS INT) AS invoice_load_id -- PK
 ,CAST(id_contract AS INT) AS contract_id --FK
 ,CAST(id_package AS INT) AS package_id -- FK
 ,CAST(id_invoice AS INT) AS invoice_id -- FK
 ,CAST(id_package_template AS INT) AS product_id -- FK
 ,CAST(notlei AS FLOAT64) AS price_wo_vat_usd
 ,CAST(tva AS INT) AS vat_rate 
 ,CAST(value AS FLOAT64) AS price_w_vat_usd
 ,CAST(payed AS FLOAT64) AS paid_w_vat_usd
 ,LOWER(currency) AS currency
  -- sloupec um převést do angličtiny
 ,CASE
   WHEN um = "kus" THEN "item"
   WHEN um = "den" THEN "day"
   WHEN um = "min" THEN "minute"
   WHEN um = "0" THEN null   
   WHEN um IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') then  'month'
  else um end AS unit
 ,CAST(quantity AS FLOAT64) as quantity
 ,DATE(TIMESTAMP(start_date), "Europe/Prague") AS start_date
 ,DATE(TIMESTAMP(end_date), "Europe/Prague") AS end_date
 ,DATE(TIMESTAMP(date_insert), "Europe/Prague") AS insert_date
 ,DATE(TIMESTAMP(date_update), "Europe/Prague") AS update_date
 ,DATE(TIMESTAMP(load_date), "Europe/Prague") AS load_date
FROM `utility-terrain-455612-s3.L0_accounting_system.invoices_load`

;-- L1_crm 
-- L1_contract 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_contract` AS 
SELECT 
 CAST(id_contract AS INT) AS contract_id -- PK
 ,CAST(id_branch AS INT) AS branch_id -- FK
 ,DATE(TIMESTAMP(date_contract_valid_from), "Europe/Prague") AS contract_valid_from
 ,DATE(TIMESTAMP(date_contract_valid_to), "Europe/Prague") AS contract_valid_to
 ,DATE(TIMESTAMP(date_registered), "Europe/Prague") AS registred_date
 ,DATE(TIMESTAMP(date_signed), "Europe/Prague") AS signed_date
 ,DATE(TIMESTAMP(activation_process_date), "Europe/Prague") AS activation_process_date
 ,DATE(TIMESTAMP(prolongation_date), "Europe/Prague") AS prolongation_date
 ,LOWER(registration_end_reason) AS registration_end_reason
 ,CAST(flag_prolongation AS bool) AS flag_prolongation
 ,CAST(flag_send_inv_email AS bool) AS flag_send_email
 ,LOWER(contract_status) AS contract_status 
 ,DATE(TIMESTAMP(load_date), "Europe/Prague") AS load_date
FROM `utility-terrain-455612-s3.L0_crm.contract`

;-- L1_product_purchase 
CREATE OR REPLACE VIEW `utility-terrain-455612-s3.L1.L1_product_purchase` AS
SELECT 
 CAST(id_package AS INT) AS product_purchase_id -- PK
 ,CAST(id_branch AS INT) AS branch_id -- FK
 ,CAST(id_contract AS INT) AS contract_id -- FK
 ,CAST(id_package_template AS INT) AS product_id -- FK
 ,DATE(TIMESTAMP(date_insert), "Europe/Prague") AS create_date
 ,DATE(TIMESTAMP(start_date), "Europe/Prague") AS product_valid_from
 ,DATE(TIMESTAMP(end_date), "Europe/Prague") AS product_valid_to
 ,CAST(fee AS FLOAT64) AS price_wo_vat_usd
 ,DATE(TIMESTAMP(p.date_update), "Europe/Prague") AS update_date
 ,CAST(package_status AS INT) AS product_status_id -- FK
 -- sloupec measure_unit převést do angličtiny
  ,CASE
   WHEN measure_unit = "kus" THEN "item"
   WHEN measure_unit = "den" THEN "day"
   WHEN measure_unit = "min" THEN "minut"
   WHEN measure_unit = "0" THEN null   
   WHEN measure_unit IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') then  'month'
  else measure_unit end AS unit
 ,DATE(TIMESTAMP(load_date), "Europe/Prague") AS load_date
 -- přidané sloupce z jiných tabulek:
 ,pr.product_name AS product_name
 ,pr.product_type AS product_type
 ,pr.product_category AS product_category 
 ,s.product_status_name AS product_status
FROM `utility-terrain-455612-s3.L0_crm.product_purchase` AS p
-- JOIN na tabulku status
LEFT JOIN `utility-terrain-455612-s3.L1.L1_status` AS s
 ON p.package_status = s.product_status_id
-- JOIN na tabulku product
LEFT JOIN `utility-terrain-455612-s3.L1.L1_product` AS pr
 ON p.id_package_template = pr.product_id
; 
