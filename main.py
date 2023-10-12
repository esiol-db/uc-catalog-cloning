# Databricks notebook source
from py4j.protocol import Py4JError
def migrate_catalog_and_managed_data_within(new_catalog: str, old_catalog: str) -> None:
  """
    Migrate an existing catalog to a new catalog.

    This function migrates the contents of the 'old_catalog' to the new catalog already created and named 'new_catalog'. The
    migration process includes moving all databases and their managed tables from
    the old catalog to the new catalog in addition to all the fine-grained permissions granted on those.

    Args:
        old_catalog (str): The name of the existing catalog to be migrated.
        new_catalog (str): The name of the new catalog that will be created.

    Returns:
        None: This function does not return any value explicitly.
  """
  #1) transfer Catalog permissions
  catalog_privileges = spark.sql(f"SELECT * FROM {old_catalog}.information_schema.catalog_privileges").collect()
  for row in catalog_privileges:
    spark.sql(f"GRANT {row.privilege_type} ON CATALOG {new_catalog} TO `{row.grantee}`")


  #2) Create databases, tables, migrate the managed data and all the permission accesses
  df = spark.sql(f"SHOW DATABASES IN {old_catalog}")
  databaseNames = list(map(lambda x: x.databaseName, df.collect()))
  
  for databaseName in databaseNames:
      # Create the databases and transfer their permissions
      print(f'Creating Database {databaseName} and transferring permissions ... ', end='')
      spark.sql(f'CREATE DATABASE IF NOT EXISTS {new_catalog}.{databaseName}')
      schema_privileges = spark.sql(f"SELECT * FROM {old_catalog}.information_schema.schema_privileges WHERE schema_name = '{databaseName}'").collect()
      for row in schema_privileges:
        if row.inherited_from == 'NONE':
          spark.sql(f"GRANT {row.privilege_type} ON SCHEMA {new_catalog}.{databaseName} TO `{row.grantee}`")
      print('DONE!')

      # Clone the managed tables and transfer their permissions
      allTables = list(map(lambda x: x.tableName, filter(lambda y: not y.isTemporary, spark.sql(f"SHOW TABLES IN {old_catalog}.{databaseName}").collect())))
      for table in allTables:
          try:
              managed = len(list(filter(lambda x: x.data_type=="MANAGED" and x.col_name=="Type", spark.sql(f"DESCRIBE FORMATTED {old_catalog}.{databaseName}.{table}").collect())))
          except Py4JError as e:
              managed = 0
          if managed > 0:
              print(f'\t Cloning managed table {table} and transferring permissions ... ', end='')
              spark.sql(f'CREATE TABLE {new_catalog}.{databaseName}.{table} DEEP CLONE {old_catalog}.{databaseName}.{table}')
              table_privileges = spark.sql(f"SELECT * FROM {old_catalog}.information_schema.table_privileges WHERE table_schema = '{databaseName}' AND table_name = '{table}'").collect()
              for row in table_privileges:
                if row.inherited_from == 'NONE':
                  spark.sql(f"GRANT {row.privilege_type} ON TABLE {new_catalog}.{databaseName}.{table} TO `{row.grantee}`")
              print('DONE!')
  return None
            


# COMMAND ----------

#test
old_catalog = 'eo000_ctg_ext_loc2'
new_catalog = 'eo000_ctg_ext_loc3' 

migrate_catalog_and_managed_data_within(new_catalog, old_catalog)
