# Databricks notebook source
# Installs the databricks-sdk package to the latest version
%pip install databricks-sdk --upgrade
dbutils.library.restartPython()

# COMMAND ----------

# %sql
# DROP CATALOG IF EXISTS eo000_ctg_ext_loc_vasco CASCADE;
# -- DROP EXTERNAL LOCATION IF EXISTS eo000_ext_loc_ctg_vasco;

# COMMAND ----------

# Names of the old external location and catalog
old_external_location_name = 'eo000_ext_loc_ctg2'
old_catalog_name = 'eo000_ctg_ext_loc2'

# Storage credential for the new storage account
storage_credential_name = 'field_demos_credential'

# Details of the new external location to be created
external_location_name = 'eo000_ext_loc_ctg_vasco'
external_location_url = 'abfss://eo000extvasco@oneenvadls.dfs.core.windows.net/'

# Name for the new catalog that will be created
new_catalog_name = 'eo000_ctg_ext_loc_vasco'

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

# Initialize a Databricks Workspace Client
w = WorkspaceClient()

# Define a function to transfer permissions from an old securable (catalog, schema, table, etc.) to a new one
def parse_transfer_permissions(securable_type: catalog.SecurableType, old_securable_full_name: str, new_securable_full_name: str) -> int:
  try:
    # Fetching current grants (permissions) for the old securable
    grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
    if grants.privilege_assignments == None:
      return 1
    
    # List to store changes in permissions
    changes = []
    for principal_permission_pair in grants.privilege_assignments:
      principal = principal_permission_pair.principal

      # Extracting privileges and filtering out some specific ones
      privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges if (('VOL' not in privilege) and ('BROWSE' not in privilege))]
      changes.append(catalog.PermissionsChange(add=privileges, principal=principal))

    # Updating permissions for the new securable  
    w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)
    return 1
  
  except Exception as e:
    # Handling any exceptions and printing them
    print(str(e))
    return 0


# COMMAND ----------

# Create new external location
external_location_created = w.external_locations.create(name=external_location_name,
                                                credential_name=storage_credential_name,
                                                comment="Created with SDK",
                                                url=external_location_url)

# Transfer permissions from the old external location to the new one
parse_transfer_permissions(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, 
                           old_securable_full_name=old_external_location_name,
                           new_securable_full_name=external_location_created.name)                                            

# COMMAND ----------

# Create the new catalog
catalog_created = w.catalogs.create(name=new_catalog_name,
                            comment='Created by SDK',
                            storage_root=external_location_created.url)

# Transfer permissions from the old catalog to the new one
parse_transfer_permissions(securable_type=catalog.SecurableType.CATALOG, 
                           old_securable_full_name=old_catalog_name,
                           new_securable_full_name=catalog_created.full_name)

# COMMAND ----------

# Fetching all schemas from the old catalog
db_list = w.schemas.list(old_catalog_name)

for db in db_list:
  # Iterating through each schema and migrating them
  print(f'Creating Database {db.name} and transferring permissions ... ', end='')
  try:
    w.schemas.get(full_name=f'{catalog_created.name}.{db.name}')

    parse_transfer_permissions(securable_type=catalog.SecurableType.SCHEMA, 
                            old_securable_full_name=db.full_name,
                            new_securable_full_name=f'{catalog_created.name}.{db.name}')
    
  except Exception as e:
    # Creating the schema in the new catalog if not already present
    db_created = w.schemas.create(name=db.name,
                                 comment=db.comment,
                                 catalog_name=catalog_created.full_name,
                                 storage_root=db.storage_root)

    # Transferring permissions for the schema
    parse_transfer_permissions(securable_type=catalog.SecurableType.SCHEMA, 
                            old_securable_full_name=db.full_name,
                            new_securable_full_name=db_created.full_name)
    
  # Add comments to the schema in the new catalog  
  spark.sql(f'COMMENT ON SCHEMA {db_created.full_name} IS "{db.comment}"')  
  print('DONE!')
  
  # For each schema, fetch its tables from the old catalog
  tbl_list = w.tables.list(catalog_name=old_catalog_name, schema_name=db.name)
  for tbl in tbl_list:
    # Only migrate managed tables
    if tbl.table_type == catalog.TableType.MANAGED:
      # Clone the managed table from the old to the new catalog
      print(f'\t Cloning managed table {tbl.name} and transferring permissions ... ', end='')
      spark.sql(f'CREATE TABLE {db_created.full_name}.{tbl.name} DEEP CLONE {tbl.full_name}')

      # Add comments to the cloned table
      tbl_created = w.tables.get(full_name=f'{db_created.full_name}.{tbl.name}')
      spark.sql(f'COMMENT ON TABLE {tbl_created.full_name} IS "{tbl.comment}"')

      # Add comments to each column of the cloned table
      for col in tbl.columns:
        spark.sql(f"""
                  ALTER TABLE {tbl_created.full_name}
                  ALTER COLUMN {col.name}
                  COMMENT "{col.comment}"
                  """)

      # Transfer permissions for the table
      parse_transfer_permissions(securable_type=catalog.SecurableType.TABLE, 
                            old_securable_full_name=tbl.full_name,
                            new_securable_full_name=tbl_created.full_name)
      print('DONE!')


# COMMAND ----------

# Migrate catalog-level tags
catalog_tag_list = spark.sql(f"""
SELECT 
  * 
FROM 
  system.information_schema.catalog_tags 
WHERE 
  catalog_name = '{old_catalog_name}'
          """).collect()

for row in catalog_tag_list:
  spark.sql(f"""
            ALTER CATALOG {catalog_created.full_name}
            SET TAGS ('{row.tag_name}' = '{row.tag_value}')
            """)

# Migrate schema-level tags
schema_tag_list = spark.sql(f"""
SELECT 
  * 
FROM 
  system.information_schema.schema_tags 
WHERE 
  catalog_name = '{old_catalog_name}'
          """).collect()

for row in schema_tag_list:
  spark.sql(f"""
            ALTER SCHEMA {catalog_created.full_name}.{row.schema_name}
            SET TAGS ('{row.tag_name}' = '{row.tag_value}')
            """)

# Migrate table-level tags
table_tag_list = spark.sql(f"""
SELECT 
  * 
FROM 
  system.information_schema.table_tags 
WHERE catalog_name = '{old_catalog_name}'
          """).collect()
          
for row in table_tag_list:
  spark.sql(f"""
            ALTER TABLE {catalog_created.full_name}.{row.schema_name}.{row.table_name}
            SET TAGS ('{row.tag_name}' = '{row.tag_value}')
            """)

# Migrate column-level tags
column_tag_list = spark.sql(f"""
SELECT
  *
FROM
  system.information_schema.column_tags
WHERE
  catalog_name = '{old_catalog_name}'
          """).collect()

for row in column_tag_list:
  spark.sql(f"""
            ALTER TABLE {catalog_created.full_name}.{row.schema_name}.{row.table_name}
            ALTER COLUMN {row.column_name}
            SET TAGS ('{row.tag_name}' = '{row.tag_value}')
            """)

# COMMAND ----------

# MAGIC %md ##Testing

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
w = WorkspaceClient()
def parse_transfer_permissions(securable_type: catalog.SecurableType, old_securable_full_name: str, new_securable_full_name: str) -> int:
  try:
    grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
    if grants.privilege_assignments == None:
      return 1
    changes = []
    for principal_permission_pair in grants.privilege_assignments:
      principal = principal_permission_pair.principal
      privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges]
      changes.append(catalog.PermissionsChange(add=privileges, principal=principal))
    w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)
    return 1
  except Exception as e:
    print(str(e))
    return 0

# COMMAND ----------


#gives the list of all schemas in the catalog
db_list = w.schemas.list(old_catalog_name)

#table creation and migration
assets = w.tables.list(catalog_name=old_catalog_name, schema_name='db_other_assets')
for tbl in assets:
      print(f'\n Cloning table {tbl.name} and transferring permissions ... ', end='\n')

assets = w.volumes.list(catalog_name=old_catalog_name, schema_name='db_other_assets')
for tbl in assets:
      print(f'\n Cloning volume {tbl.name} and transferring permissions ... ', end='\n')

assets = w.functions.list(catalog_name=old_catalog_name, schema_name='db_other_assets')
for tbl in assets:
      print(f'\n Cloning function {tbl.name} and transferring permissions ... ', end='\n')

assets = w.workspace_bindings.get(name=old_catalog_name)
print(f'\n Cloning workspace_bindings {assets} and transferring permissions ... ', end='\n')

assets = w.register_models.get(name=old_catalog_name)
print(f'\n Cloning workspace_bindings {assets} and transferring permissions ... ', end='\n')

assets = w.model_versions.list(full_name='eo000_ctg_ext_loc2.db_other_assets.my_model')
for tbl in assets:
      print(f'\n Cloning model_versions {tbl} and transferring permissions ... ', end='\n')

