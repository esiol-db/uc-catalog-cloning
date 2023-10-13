# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

old_external_location_name = 'eo000_ext_loc_ctg2'
old_catalog_name = 'eo000_ctg_ext_loc2'

# COMMAND ----------

def parse_transfer_permissions(securable_type: catalog.SecurableType, old_securable_full_name: str, new_securable_full_name: str) -> int:
  try:
    grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
    changes = []
    for principal_permission_pair in grants.privilege_assignments:
      principal = principal_permission_pair.principal
      privilages = [eval('catalog.Privilege.'+privilage) for privilage in principal_permission_pair.privileges if 'VOL' not in privilage]
      changes.append(catalog.PermissionsChange(add=privilages, principal=principal))
    w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)
    return 1
  except Exception as e:
    print(str(e))
    return 0


# COMMAND ----------

# MAGIC %md
# MAGIC #External Location

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

#Create new external location
storage_credential_name = 'field_demos_credential'
external_location_name = 'eo000_ext_loc_ctg4'
external_location_url = 'abfss://eo000ext4@oneenvadls.dfs.core.windows.net/'
external_location_created = w.external_locations.create(name=external_location_name,
                                                credential_name=storage_credential_name,
                                                comment="your comment",
                                                url=external_location_url)

# COMMAND ----------

external_location

# COMMAND ----------

parse_transfer_permissions(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, 
                           old_securable_full_name=old_external_location_name,
                           new_securable_full_name=external_location.name)

# COMMAND ----------

grants = w.grants.get(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, full_name=f'{old_external_location_name}')
changes = []
for principal_permission_pair in grants.privilege_assignments:
  principal = principal_permission_pair.principal
  #privilages = [eval('catalog.Privilege.'+privilage) for privilage in principal_permission_pair.privileges if 'VOL' not in privilage]
  changes.append(catalog.PermissionsChange(add=principal_permission_pair.privileges, principal=principal))
# w.grants.update(full_name=external_location.name, securable_type=catalog.SecurableType.EXTERNAL_LOCATION, changes=changes)


# COMMAND ----------

changes

# COMMAND ----------

#Create the new catalog
new_catalog_name = 'eo000_ctg_ext_loc4'
new_catalog = w.catalogs.create(name=new_catalog_name,
                            comment='your comment',
                            storage_root=external_location.url)

# COMMAND ----------

new_catalog

# COMMAND ----------

grants = w.grants.get(securable_type=catalog.SecurableType.CATALOG, full_name='eo000_ctg_ext_loc2')
grants

# COMMAND ----------

for principal_permission in grants.privilege_assignments:
  principal = principal_permission.principal
  privilages = [eval('catalog.Privilege.'+privilage) for privilage in principal_permission.privileges if privilage not in ['CREATE_VOLUME','READ_VOLUME']]

# COMMAND ----------

privilages

# COMMAND ----------

# MAGIC %md
# MAGIC # SCHEMA LEVEL

# COMMAND ----------


db = w.schemas.list(old_catalog_name)[0]
# db.name
# db.storage_root

db_created = w.schemas.create(name=db.name,
                 catalog_name=new_catalog.name,
                 storage_root=db.storage_root)

grants = w.grants.get(securable_type=catalog.SecurableType.SCHEMA, full_name=f'{old_catalog_name}.{db.name}')
changes = []
for principal_permission_pair in grants.privilege_assignments:
  principal = principal_permission_pair.principal
  privilages = [eval('catalog.Privilege.'+privilage) for privilage in principal_permission_pair.privileges if 'VOL' not in privilage]
  changes.append(catalog.PermissionsChange(add=privilages, principal=principal))
w.grants.update(full_name=db_created.full_name, securable_type=catalog.SecurableType.SCHEMA, changes=changes)



# COMMAND ----------

grants

# COMMAND ----------

changes

# COMMAND ----------

changes.privilege_assignments[0].principal

# COMMAND ----------

catalog.PermissionsChange.from_dict(changes[0])

# COMMAND ----------

w.grants.update(securable_type=catalog.SecurableType.CATALOG, full_name='eo000_ctg_ext_loc4', changes=[changes.privilege_assignments[0]])

# COMMAND ----------

print(list(w.tables.list(catalog_name='eo000_ctg_ext_loc2', schema_name='db1'))[0])

# COMMAND ----------


