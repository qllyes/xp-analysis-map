SELECT DISTINCT
  drug_code AS '国家药品编码',
  national_abc_category AS '国家医保目录',
  prov_abc_catalog AS '省医保目录',
  prov_insurance_price AS '省医保支付价'
FROM scm_xp_med_insu_cata_dfp
