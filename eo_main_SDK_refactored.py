# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from databricks.sdk.core import DatabricksError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from typing import *

class MigrateCatalog():
  def __init__(self, 
               old_external_location_name: str,
               old_catalog_name: str,
               new_storage_credential_name: str,
               new_external_location_name: str,
               new_external_location_url: str,
               new_catalog_name: str,
               db_dict: Optional[Dict[str, List]]) -> None:
    self.w = WorkspaceClient()
    self.old_external_location_name = old_external_location_name
    self.old_catalog_name = old_catalog_name
    self.new_storage_credential_name = new_storage_credential_name
    self.new_external_location_name = new_external_location_name
    self.new_external_location_url = new_external_location_url
    self.new_catalog_name = new_catalog_name
    self.db_dict = db_dict
    self.securable_dict = {
      catalog.SecurableType.EXTERNAL_LOCATION: [self.w.external_locations, 'External location'],
      catalog.SecurableType.CATALOG: [self.w.catalogs, 'Catalog'],
      catalog.SecurableType.SCHEMA: [self.w.schemas, 'Database'],
      catalog.SecurableType.TABLE: [self.w.tables, 'Table']
                           }
  

  def _migrate_tag(self, securable_type_str: str,
                  old_catalog_name: str, 
                  new_securable_full_name: str) -> bool:
    try:
      securable_tag_list = spark.sql(f"""
      SELECT * FROM 
        system.information_schema.{securable_type_str.lower()}_tags 
      WHERE catalog_name = '{old_catalog_name}'
              """).collect()
      

      for row in securable_tag_list:
        if securable_type_str.lower() == 'column':
          spark.sql(f"""
                  ALTER TABLE {new_securable_full_name}
                  ALTER COLUMN {row.column_name}
                  SET TAGS ('{row.tag_name}' = '{row.tag_value}')
                  """)
        else:
          spark.sql(f"""
                  ALTER {securable_type_str} {new_securable_full_name}
                  SET TAGS ('{row.tag_name}' = '{row.tag_value}')
                  """)
          
    except Exception as e:
      print(f'\t\t{str(e)}')


  def _parse_transfer_permissions(self, 
                                  securable_type: catalog.SecurableType, 
                                  old_securable_full_name: str, 
                                  new_securable_full_name: str) -> bool:
    try:
      grants = self.w.grants.get(securable_type=securable_type, 
                                full_name=f'{old_securable_full_name}')
      if grants.privilege_assignments == None:
        return True
      changes = []
      for principal_permission_pair in grants.privilege_assignments:
        principal = principal_permission_pair.principal
        privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges if (('VOL' not in privilege) and ('BROWSE' not in privilege))]
        changes.append(catalog.PermissionsChange(add=privileges, principal=principal))
      self.w.grants.update(full_name=new_securable_full_name, 
                          securable_type=securable_type, 
                          changes=changes)
      return True
    except Exception as e:
      # print(str(e))
      return False
  

  
  def _get_or_create_transfer(self, 
                      securable_type: catalog.SecurableType, 
                      old_securable_full_name: str,
                      new_securable_full_name: str,
                      **kwarg) -> None:
    analysis_exception_hit = 0
    try:
      new_securable = self.securable_dict[securable_type][0].get(new_securable_full_name)
      print(f'{self.securable_dict[securable_type][1]} {new_securable_full_name} already exists. Only transferring permissions and comment ...', end=' ')

    except Exception as e:
      print(f'Creating {self.securable_dict[securable_type][1]} {new_securable_full_name} and transferring permissions and comment ...', end=' ')
      try:
        new_securable = self.securable_dict[securable_type][0].create(
          name=new_securable_full_name,
          comment=self.securable_dict[securable_type][0].get(old_securable_full_name).comment,
          **kwarg)
      except AnalysisException as ae:
        analysis_exception_hit = 1
        print(f'\n\t\t{str(ae)}')
    
    finally:
      if not analysis_exception_hit:
        _ = self._parse_transfer_permissions(securable_type=catalog.SecurableType, 
                              old_securable_full_name=old_securable_full_name,
                              new_securable_full_name=new_securable_full_name)         
        print('DONE!')  
    
    return new_securable
  

  def _get_or_create_managed_table_transfer(self, 
                      securable_type: catalog.SecurableType, 
                      old_securable_full_name: str,
                      new_securable_full_name: str,
                      **kwarg) -> None:
    tbl = kwarg['tbl']
    analysis_exception_hit = 0
    try:
      new_table = self.securable_dict[securable_type][0].get(full_name=new_securable_full_name)
      print(f'\tManaged {self.securable_dict[securable_type][1]} {tbl.name} already exists. Only transferring permissions and comments ...', end=' ')
    except Exception as e:
      try:
        print(f'\tCloning Managed {self.securable_dict[securable_type][1]} {tbl.name} and transferring permissions ... ', end=' ')
        spark.sql(f'CREATE TABLE {new_securable_full_name} DEEP CLONE {tbl.full_name}')
        
        new_table = self.securable_dict[securable_type][0].get(full_name=new_securable_full_name)
        spark.sql(f'COMMENT ON TABLE {new_securable_full_name} IS "{tbl.comment}"')
      except AnalysisException as ae:
        new_table = None
        analysis_exception_hit = 1
        print(f'\n\t\t\{str(ae)}')
    finally:
      if not analysis_exception_hit:
        for col in tbl.columns:
          spark.sql(f"""
                    ALTER TABLE {new_securable_full_name}
                    ALTER COLUMN {col.name}
                    COMMENT "{col.comment}"
                    """)

        _ = self._parse_transfer_permissions(securable_type=catalog.SecurableType.TABLE, 
                          old_securable_full_name=old_securable_full_name,
                          new_securable_full_name=new_securable_full_name)
        print('DONE!')
        
    return new_table
  
  
  def __call__(self):
    self.new_external_location = self._get_or_create_transfer(
                        catalog.SecurableType.EXTERNAL_LOCATION,
                        self.old_external_location_name,
                        self.new_external_location_name,
                        credential_name=self.new_storage_credential_name,
                        url=self.new_external_location_url)    

    print('\t', end='')
    self.new_catalog = self._get_or_create_transfer(
                        catalog.SecurableType.CATALOG,
                        self.old_catalog_name,
                        self.new_catalog_name,
                        storage_root=self.new_external_location.url) 
         
    _ = self._migrate_tag('catalog',
                  self.old_catalog_name, 
                  self.new_catalog_name)
    
    db_list = self.w.schemas.list(self.old_catalog_name)
    for db in db_list: 
      print('\t', end='\t')         
      self.new_db = self._get_or_create_transfer(
                        catalog.SecurableType.SCHEMA,
                        f'{self.old_catalog_name}.{db.name}',
                        f'{self.new_catalog_name}.{db.name}',
                        catalog_name=self.new_catalog.name,
                        storage_root=db.storage_root)
      
      _ = self._migrate_tag('schema',
                  self.old_catalog_name, 
                  f'{self.new_catalog_name}.{db.name}')
      
      tbl_list = self.w.tables.list(catalog_name=self.old_catalog_name, schema_name=db.name)
      for tbl in tbl_list:
        if tbl.table_type == catalog.TableType.MANAGED:
          print('\t', end='\t')
          self.new_table = self._get_or_create_managed_table_transfer( 
                      catalog.SecurableType.TABLE, 
                      f'{self.old_catalog_name}.{db.name}.{tbl.name}',
                      f'{self.new_catalog_name}.{db.name}.{tbl.name}',
                      tbl=tbl)
          
          _ = self._migrate_tag('table',
                  self.old_catalog_name, 
                  f'{self.new_catalog_name}.{db.name}.{tbl.name}')
          
          _ = self._migrate_tag('column',
                  self.old_catalog_name, 
                  f'{self.new_catalog_name}.{db.name}.{tbl.name}')
          

# COMMAND ----------

inputs = dict(old_external_location_name = 'eo000_ext_loc_ctg2',
old_catalog_name = 'eo000_ctg_ext_loc2',
new_storage_credential_name = 'field_demos_credential',
new_external_location_name = 'eo000_ext_loc_ctg5',
new_external_location_url = 'abfss://eo000ext5@oneenvadls.dfs.core.windows.net/',
new_catalog_name = 'eo000_ctg_ext_loc5')

# COMMAND ----------

migrate = MigrateCatalog(**inputs, db_dict=None)
migrate()

# COMMAND ----------

def parse_transfer_permissions(securable_type: catalog.SecurableType, old_securable_full_name: str, new_securable_full_name: str) -> bool:
  try:
    grants = w.grants.get(securable_type=securable_type, full_name=f'{old_securable_full_name}')
    if grants.privilege_assignments == None:
      return True
    changes = []
    for principal_permission_pair in grants.privilege_assignments:
      principal = principal_permission_pair.principal
      privileges = [eval('catalog.Privilege.'+privilege) for privilege in principal_permission_pair.privileges if (('VOL' not in privilege) and ('BROWSE' not in privilege))]
      changes.append(catalog.PermissionsChange(add=privileges, principal=principal))
    w.grants.update(full_name=new_securable_full_name, securable_type=securable_type, changes=changes)
    return True
  except Exception as e:
    print(str(e))
    return False


# COMMAND ----------

#Create new external location
try:
  w.external_locations.get(new_external_location_name)
  print(f'External location {new_external_location_name} already exists. Only transferring permissions and comment ...', end=' ')
except Exception as e:
  print(f'Creating External location {new_external_location_name} and transferring permissions and comment ...', end=' ')
  external_location_created = w.external_locations.create(name=new_external_location_name,
                                                credential_name=new_storage_credential_name,
                                                comment=w.external_locations.get(old_external_location_name).comment,
                                                url=new_external_location_url)
finally:
  parse_transfer_permissions(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, 
                           old_securable_full_name=old_external_location_name,
                           new_securable_full_name=new_external_location_name)         
  print('DONE!')                                  

# COMMAND ----------

#Create the new catalog
try:  
  w.catalogs.get(new_catalog_name)
  print(f'Catalog {new_catalog_name} already exists. Only transferring permissions and comment ...', end=' ')
except Exception as e:
  print(f'Creating catalog {new_catalog_name} and transferring permissions and comment ...', end=' ')
  catalog_created = w.catalogs.create(name=new_catalog_name,
                            comment=w.catalogs.get(old_catalog_name).comment,
                            storage_root=external_location_created.url)
finally:
  parse_transfer_permissions(securable_type=catalog.SecurableType.CATALOG, 
                           old_securable_full_name=old_catalog_name,
                           new_securable_full_name=new_catalog_name)
  print('DONE!')                                  

# COMMAND ----------


#gives the list of all schemas in the catalog
db_list = w.schemas.list(old_catalog_name)

for db in db_list:
  #schema creation and migration
  try:
    w.schemas.get(full_name=f'{new_catalog_name}.{db.name}')
    print(f'Database {db.name} already exists. Only transferring permissions and comment ...', end=' ')
  except DatabricksError as e:
    print(f'Creating database {db.name} and transferring permissions and comment ...', end=' ')
    db_created = w.schemas.create(name=db.name,
                                 comment=db.comment,
                                 catalog_name=new_catalog_name,
                                 storage_root=db.storage_root)
  finally:
    parse_transfer_permissions(securable_type=catalog.SecurableType.SCHEMA, 
                            old_securable_full_name=db.full_name,
                            new_securable_full_name=f'{new_catalog_name}.{db.name}')
    print('DONE!')
  
  #table creation and migraiton
  tbl_list = w.tables.list(catalog_name=old_catalog_name, schema_name=db.name)
  for tbl in tbl_list:
    analysis_exception_flag = 0
    if tbl.table_type == catalog.TableType.MANAGED:
      try:
        w.tables.get(full_name=f'{new_catalog_name}.{db.name}.{tbl.name}')
        print(f'\tManaged table {tbl.name} already exists. Only transferring permissions and comments ...', end=' ')
      except DatabricksError as de:
        try:
          print(f'\tCloning managed table {tbl.name} and transferring permissions ... ', end=' ')
          spark.sql(f'CREATE TABLE {new_catalog_name}.{db.name}.{tbl.name} DEEP CLONE {tbl.full_name}')
          
          tbl_created = w.tables.get(full_name=f'{new_catalog_name}.{db.name}.{tbl.name}')
          spark.sql(f'COMMENT ON TABLE {new_catalog_name}.{db.name}.{tbl.name} IS "{tbl.comment}"')
        except AnalysisException as ae:
          analysis_exception_flag = 1
          print(f'\n\t\t{str(ae)}')
      finally:
        if not analysis_exception_flag:
          for col in tbl.columns:
            spark.sql(f"""
                      ALTER TABLE {new_catalog_name}.{db.name}.{tbl.name}
                      ALTER COLUMN {col.name}
                      COMMENT "{col.comment}"
                      """)

          parse_transfer_permissions(securable_type=catalog.SecurableType.TABLE, 
                            old_securable_full_name=tbl.full_name,
                            new_securable_full_name=f'{new_catalog_name}.{db.name}.{tbl.name}')
          print('DONE!')


# COMMAND ----------



# COMMAND ----------

migrate_tag(securable_type_str='catalog', 
            old_catalog_name='eo000_ctg_ext_loc2', 
            new_securable_full_name='eo000_ctg_ext_loc5') 

# COMMAND ----------

#Catalog level Tags
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


#Schema level Tags
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

#Table level Tags
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



#Column level Tags
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



# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
catalog = "eo000_ctg_ext_loc2"
schema = "db_other_assets"
model_name = "my_model"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/95ece4dd64aa492a8b1c1a34966a8303/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table eo000_ctg_ext_loc2.db_other_assets.valid_groups(group_name string);
# MAGIC
# MAGIC insert into eo000_ctg_ext_loc2.db_other_assets.valid_groups
# MAGIC values
# MAGIC ('admins');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table eo000_ctg_ext_loc2.db_other_assets.tbl2(id int, name string);
# MAGIC
# MAGIC insert into eo000_ctg_ext_loc2.db_other_assets.tbl2 values(1, 'name1'),(2,'name2');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function eo000_ctg_ext_loc2.db_other_assets.mask_pii(col_name string)
# MAGIC return if(exists(
# MAGIC   select 1
# MAGIC   from eo000_ctg_ext_loc2.db_other_assets.valid_groups v
# MAGIC   where true = is_account_group_member(v.group_name)
# MAGIC ), col_name, '*****');

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table eo000_ctg_ext_loc2.db_other_assets.tbl2 alter column name set mask eo000_ctg_ext_loc2.db_other_assets.mask_pii;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION eo000_ctg_ext_loc2.db_other_assets.id_filter(id STRING)
# MAGIC RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, id < 2);

# COMMAND ----------

# MAGIC %sql ALTER TABLE eo000_ctg_ext_loc2.db_other_assets.tbl2 SET ROW FILTER eo000_ctg_ext_loc2.db_other_assets.id_filter ON (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from eo000_ctg_ext_loc2.db_other_assets.tbl2;
