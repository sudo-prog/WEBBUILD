-- schema_patch_v2.sql
-- Apply this patch to add unique constraint on (business_name, city)
-- IMPORTANT: If the leads table already has duplicate rows that violate this constraint,
-- you must first deduplicate using the following SQL:

-- DELETE FROM leads a USING leads b
-- WHERE a.ctid < b.ctid
--   AND a.business_name = b.business_name
--   AND a.city = b.city;

-- After deduplication, re-run this patch.

ALTER TABLE leads ADD CONSTRAINT unique_business_name_city UNIQUE (business_name, city);