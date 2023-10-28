# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------

old_external_location_name = 'eo000_ext_loc_ctg2'
old_catalog_name = 'eo000_ctg_ext_loc2'

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
def parse_transfer_permissions(securable_type: catalog.SecurableType, old_securable_full_name: str, new_securable_full_name: str) -> int:
  try:
    grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
    if grants.privilege_assignments == None:
      return 1
    changes = []
    for principal_permission_pair in grants.privilege_assignments:
      principal = principal_permission_pair.principal
      privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges if (('VOL' not in privilege) and ('BROWSE' not in privilege))]
      changes.append(catalog.PermissionsChange(add=privileges, principal=principal))
    w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)
    return 1
  except Exception as e:
    print(str(e))
    return 0


# COMMAND ----------

# MAGIC %md
# MAGIC #External Location

# COMMAND ----------




#Create new external location
storage_credential_name = 'field_demos_credential'
external_location_name = 'eo000_ext_loc_ctg4'
external_location_url = 'abfss://eo000ext4@oneenvadls.dfs.core.windows.net/'
external_location_created = w.external_locations.create(name=external_location_name,
                                                credential_name=storage_credential_name,
                                                comment="Created with SDK",
                                                url=external_location_url)

# COMMAND ----------

parse_transfer_permissions(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, 
                           old_securable_full_name=old_external_location_name,
                           new_securable_full_name=external_location_created.name)

# COMMAND ----------

#Create the new catalog
new_catalog_name = 'eo000_ctg_ext_loc4'
catalog_created = w.catalogs.create(name=new_catalog_name,
                            comment='Created by SDK',
                            storage_root=external_location_created.url)

# COMMAND ----------

catalog_created.name

# COMMAND ----------

parse_transfer_permissions(securable_type=catalog.SecurableType.CATALOG, 
                           old_securable_full_name=old_catalog_name,
                           new_securable_full_name=catalog_created.full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCHEMA LEVEL

# COMMAND ----------

db.full_name

# COMMAND ----------

# old_securable_full_name=db.full_name
# new_securable_full_name=db_created.full_name
# securable_type=catalog.SecurableType.SCHEMA

# grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
# changes = []
# for principal_permission_pair in grants.privilege_assignments:
#   principal = principal_permission_pair.principal
#   privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges if (('VOL' not in privilege) and ('BROWSE' not in privilege))]
#   changes.append(catalog.PermissionsChange(add=privileges, principal=principal))
# w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)

# COMMAND ----------


w.schemas.get(full_name=f'{old_catalog_name}.wew')

# COMMAND ----------

db = w.schemas.list(old_catalog_name)[0]
db

# COMMAND ----------


db = w.schemas.list(old_catalog_name)[1]
# db.name
# db.storage_root

db_created = w.schemas.create(name=db.name,
                 catalog_name=catalog_created.full_name,
                 storage_root=db.storage_root)

parse_transfer_permissions(securable_type=catalog.SecurableType.SCHEMA, 
                           old_securable_full_name=db.full_name,
                           new_securable_full_name=db_created.full_name)


# COMMAND ----------

# MAGIC %md
# MAGIC # TABLE LEVEL

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLES

# COMMAND ----------

column_tag_list = spark.sql("""
SELECT
  *
FROM
  system.information_schema.column_tags
WHERE
  catalog_name = 'eo000_ctg_ext_loc2'
          """).collect()

# COMMAND ----------

db_dict = {}
tbl_dict = {}
for row in column_tag_list:
  db_list[](row.schema_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED eo000_ctg_ext_loc2.db1.tbl1

# COMMAND ----------

tbl = list(w.tables.list(catalog_name=old_catalog_name, schema_name=db.name))[0]
tbl

# COMMAND ----------


tbl = list(w.tables.list(catalog_name=old_catalog_name, schema_name=db.name))[0]

if tbl.table_type == catalog.TableType.MANAGED:
  spark.sql(f'CREATE TABLE {db_created.full_name}.{tbl.name} DEEP CLONE {tbl.full_name}')

  tbl_created = w.tables.get(full_name=f'{db_created.full_name}.{tbl.name}')

  parse_transfer_permissions(securable_type=catalog.SecurableType.TABLE, 
                           old_securable_full_name=tbl.full_name,
                           new_securable_full_name=tbl_created.full_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ## VIEWS

# COMMAND ----------

import re

# COMMAND ----------

tbl = list(w.tables.list(catalog_name=old_catalog_name, schema_name=db.name))[0]


if tbl.table_type == catalog.TableType.VIEW:
  view_defenition = tbl.view_definition
  view_defenition = re.sub(rf'{old_catalog_name}\.', f'{new_catalog_name}.', view_defenition)
  view_defenition = re.sub(rf'{old_catalog_name}`\.', f'{new_catalog_name}`.', view_defenition)

  spark.sql(view_defenition)

  tbl_created = w.tables.get(full_name=f'{db_created.full_name}.{tbl.name}')

  parse_transfer_permissions(securable_type=catalog.SecurableType.TABLE, 
                           old_securable_full_name=tbl.full_name,
                           new_securable_full_name=tbl_created.full_name)


# COMMAND ----------


