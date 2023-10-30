from typing import Dict, Optional, List
import re
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler(filename="logs.log", mode="w")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


try:
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
except Exception as e:
    logger.info(e)
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except ImportError as ie:
        logger.info(ie)
        raise ImportError(
            "Could not import databricks-connect, please install with `pip install databricks-connect`."
        ) from ie

try:
    from databricks.sdk.core import DatabricksError
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import catalog
except ImportError as e:
    logger.info(e)
    raise ImportError(
        "Could not import databricks-sdk, please install with `pip install databricks-sdk --upgrade`.\n"
        "If you are running from Databricks you also need to restart Python by running `dbutils.library.restartPython()`"
    ) from e
try:
    from pyspark.sql.utils import AnalysisException
except ImportError as e:
    logger.info(e)
    raise ImportError(
        "Could not import pyspark, please install with `pip install pyspark`."
    ) from e
try:
    from termcolor import cprint
except ImportError as e:
    logger.info(e)
    raise ImportError(
        "Could not import termcolor, please install with `pip install termcolor`."
    ) from e


class MigrateCatalog:
    def __init__(
        self,
        old_catalog_external_location_name: str,
        old_catalog_name: str,
        new_catalog_external_location_pre_req: List,
        new_catalog_name: str,
        schemas_locations_dict: Optional[Dict[str, List]],
    ) -> None:
        self.w = WorkspaceClient()
        self.old_ext_loc_name = old_catalog_external_location_name
        self.old_ctlg_name = old_catalog_name
        self.new_external_location_pre_req = new_catalog_external_location_pre_req
        (
            self.new_ext_loc_name,
            self.new_strg_cred_name,
            self.new_ext_loc_url,
        ) = self.new_external_location_pre_req
        self.new_ctlg_name = new_catalog_name
        self.db_dict = self._build_location_for_schemas(
            schemas_locations_dict or dict()
        )
        self.securable_dict = {
            catalog.SecurableType.EXTERNAL_LOCATION: [
                self.w.external_locations,
                "External location",
            ],
            catalog.SecurableType.CATALOG: [self.w.catalogs, "Catalog"],
            catalog.SecurableType.SCHEMA: [self.w.schemas, "Schema"],
            catalog.SecurableType.TABLE: [self.w.tables, "Table"],
        }

    def _print_to_console(
        self,
        message: str,
        end: str = "\n",
        indent_size: int = 1,
        indent_level: int = 0,
        color: str = None,
        on_color: str = None,
    ) -> None:
        indent = " " * indent_size * indent_level
        cprint(indent + message.strip(), color=color, on_color=on_color, end=end)

    def _build_location_for_schemas(self, db_dict: Dict[str, List]) -> Dict[str, List]:
        databricks_exception_hit = 0
        db_dict_out = {}
        if db_dict:
            self._print_to_console(
                "Creating external locations if they do not exist for the schemas based on the input dictionary.",
                color="cyan",
            )
        for db_name, (ext_loc_name, cred_name, url) in db_dict.items():
            try:
                db_external_location = self.w.external_locations.get(ext_loc_name)
                self._print_to_console(
                    f"External location {ext_loc_name} already exists and will be used for {db_name}.",
                    indent_level=3,
                )
            except DatabricksError as e:
                logger.info(e)
                self._print_to_console(
                    f"Creating External location {ext_loc_name} ...",
                    indent_level=3,
                    end=" ",
                )
                try:
                    db_external_location = self.w.external_locations.create(
                        name=ext_loc_name, credential_name=cred_name, url=url
                    )
                    self._print_to_console("DONE!", color="green")

                except AnalysisException as e:
                    logger.info(e)
                    self._print_to_console(str(e), color="red", on_color="on_yellow")
                except DatabricksError as de:
                    databricks_exception_hit = 1
                    logger.exception(de)
                    raise de
            finally:
                if not databricks_exception_hit:
                    db_dict_out[db_name] = db_external_location.url

        return db_dict_out

    def _migrate_tags(
        self,
        securable_type_str: str,
        old_catalog_name: str,
        new_securable_full_name: str,
    ) -> bool:
        _, schema, table = (*new_securable_full_name.split("."), None, None)[:3]
        schema_clause = f"\tAND schema_name = '{schema}'" if schema else ""
        table_clause = f"\tAND table_name = '{table}'" if table else ""
        query = (
            f"""
        SELECT * FROM 
        system.information_schema.{securable_type_str.lower()}_tags 
        WHERE catalog_name = '{old_catalog_name}'
              """
            + schema_clause
            + table_clause
        )
        try:
            securable_tag_list = spark.sql(query).collect()

            for row in securable_tag_list:
                if securable_type_str.lower() == "column":
                    spark.sql(
                        f"""
                  ALTER TABLE {new_securable_full_name}
                  ALTER COLUMN {row.column_name}
                  SET TAGS ('{row.tag_name}' = '{row.tag_value}')
                  """
                    )
                else:
                    spark.sql(
                        f"""
                  ALTER {securable_type_str} {new_securable_full_name}
                  SET TAGS ('{row.tag_name}' = '{row.tag_value}')
                  """
                    )

        except DatabricksError as e:
            logger.info(e)
            self._print_to_console(str(e), color="red", on_color="on_yellow")

    def _parse_transfer_permissions(
        self,
        securable_type: catalog.SecurableType,
        old_securable_full_name: str,
        new_securable_full_name: str,
    ) -> bool:
        try:
            grants = self.w.grants.get(
                securable_type=securable_type, full_name=f"{old_securable_full_name}"
            )
            if grants.privilege_assignments == None:
                return True
            changes = []
            for principal_permission_pair in grants.privilege_assignments:
                principal = principal_permission_pair.principal
                privileges = [
                    eval(f"catalog.Privilege.{privilege}")
                    for privilege in principal_permission_pair.privileges
                    if (("VOL" not in privilege) and ("BROWSE" not in privilege))
                ]
                changes.append(
                    catalog.PermissionsChange(add=privileges, principal=principal)
                )
            self.w.grants.update(
                full_name=new_securable_full_name,
                securable_type=securable_type,
                changes=changes,
            )
            return True
        except DatabricksError as e:
            logger.info(e)
            self._print_to_console(str(e), color="red", on_color="on_yellow")
            return False

    def _get_or_create_transfer(
        self,
        securable_type: catalog.SecurableType,
        old_securable_full_name: str,
        new_securable_full_name: str,
        print_indent_level: str = 0,
        **kwarg,
    ) -> None:
        new_securable = None
        analysis_exception_hit = 0
        databricks_exception_hit = 0
        new_securable_name = re.findall("[^.]+$", new_securable_full_name)[0]
        try:
            new_securable = self.securable_dict[securable_type][0].get(
                new_securable_full_name
            )
            self._print_to_console(
                f"{self.securable_dict[securable_type][1]} {new_securable_name} already exists. Only transferring permissions, comments and tags ...",
                indent_level=print_indent_level,
                end=" ",
            )
        except DatabricksError as e:
            logger.info(e)
            self._print_to_console(
                f"Creating {self.securable_dict[securable_type][1]} {new_securable_name} and transferring permissions, comments and tags ...",
                indent_level=print_indent_level,
                end=" ",
            )
            try:
                if securable_type == catalog.SecurableType.TABLE:
                    new_securable = self.securable_dict[securable_type][0].get(
                        old_securable_full_name
                    )
                    spark.sql(
                        f"CREATE TABLE {new_securable_full_name} DEEP CLONE {new_securable.full_name}"
                    )
                    new_securable = self.securable_dict[securable_type][0].get(
                        full_name=new_securable_full_name
                    )
                    spark.sql(
                        f'COMMENT ON TABLE {new_securable_full_name} IS "{new_securable.comment or ""}"'
                    )
                else:
                    new_securable = self.securable_dict[securable_type][0].create(
                        name=new_securable_name,
                        comment=self.securable_dict[securable_type][0]
                        .get(old_securable_full_name)
                        .comment
                        or "",
                        **kwarg,
                    )
            except AnalysisException as ae:
                logger.exception(ae)
                analysis_exception_hit = 1
                self._print_to_console(str(ae), color="red", on_color="on_yellow")
            except DatabricksError as de:
                logger.exception(de)
                databricks_exception_hit = 1
                raise de
        finally:
            if not (analysis_exception_hit or databricks_exception_hit):
                _ = self._parse_transfer_permissions(
                    securable_type=securable_type,
                    old_securable_full_name=old_securable_full_name,
                    new_securable_full_name=new_securable_full_name,
                )
                if securable_type != catalog.SecurableType.EXTERNAL_LOCATION:
                    _ = self._migrate_tags(
                        self.securable_dict[securable_type][1],
                        self.old_ctlg_name,
                        new_securable_full_name,
                    )
                if securable_type == catalog.SecurableType.TABLE:
                    # for col in new_securable.columns:
                    #     spark.sql(
                    #         f"""
                    # ALTER TABLE {new_securable_full_name}
                    # ALTER COLUMN {col.name}
                    # COMMENT "{col.comment}"
                    # """
                    #     )
                    _ = self._migrate_tags(
                        "column", self.old_ctlg_name, new_securable_full_name
                    )
                self._print_to_console("DONE!", color="green")

        return new_securable

    def __call__(self):
        self._print_to_console(
            "Creating data assets if they do not exist and migrate permissions, comments and tags.",
            color="cyan",
        )
        self.new_external_location = self._get_or_create_transfer(
            catalog.SecurableType.EXTERNAL_LOCATION,
            self.old_ext_loc_name,
            self.new_ext_loc_name,
            print_indent_level=3,
            credential_name=self.new_strg_cred_name,
            url=self.new_ext_loc_url,
        )

        self.new_catalog = self._get_or_create_transfer(
            catalog.SecurableType.CATALOG,
            self.old_ctlg_name,
            self.new_ctlg_name,
            print_indent_level=6,
            storage_root=self.new_external_location.url,
        )

        db_list = self.w.schemas.list(self.old_ctlg_name)
        for db in db_list:
            self.new_db = self._get_or_create_transfer(
                catalog.SecurableType.SCHEMA,
                f"{self.old_ctlg_name}.{db.name}",
                f"{self.new_ctlg_name}.{db.name}",
                print_indent_level=9,
                catalog_name=self.new_catalog.name,
                storage_root=self.db_dict.get(db.name, None),
            )

            tbl_list = self.w.tables.list(
                catalog_name=self.old_ctlg_name, schema_name=db.name
            )
            for tbl in tbl_list:
                if tbl.table_type == catalog.TableType.MANAGED:
                    self.new_table = self._get_or_create_transfer(
                        catalog.SecurableType.TABLE,
                        f"{self.old_ctlg_name}.{db.name}.{tbl.name}",
                        f"{self.new_ctlg_name}.{db.name}.{tbl.name}",
                        print_indent_level=12,
                    )
