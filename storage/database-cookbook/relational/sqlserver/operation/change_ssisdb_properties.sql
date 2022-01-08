use ssisdb
GO

select * 
from ssisdb.catalog.catalog_properties


UPDATE ssisdb.catalog.catalog_properties
SET property_value = 5
WHERE property_name = 'MAX_PROJECT_VERSIONS'
UPDATE ssisdb.catalog.catalog_properties
SET property_value = 7
WHERE property_name = 'RETENTION_WINDOW'