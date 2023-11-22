# uc-catalog-migration

**uc-catalog-migration** is a powerful migration script designed to streamline the process of creating a new catalog with an updated storage location. It facilitates the seamless migration of all associated databases, managed tables, permission accesses, tags, and comments from the source catalog to the target catalog.

## How to Use

Follow these steps to make the most of this migration script:

1. **Installation**:
   - Ensure that you have Python installed on your system.

2. **Import the `MigrateCatalog` Class**:
   - Import the `MigrateCatalog` class from the `migratecatalog` module in your Python script or Jupyter notebook.

   ```python
   from migratecatalog import MigrateCatalog
   ```
   

   
3. **Instantiate the `MigrateCatalog` Class**:

Create an instance of the MigrateCatalog class with the necessary configuration parameters as illustrated in [example.ipynb](https://github.com/databricks/uc-catalog-migration/blob/main/example.ipynb)

   ```python
migration = MigrateCatalog(
         source_catalog_external_location_name,
         source_catalog_name,
         target_catalog_external_location_pre_req,
         target_catalog_name,
         schemas_locations_dict
)
   ```
Replace the input arguments with your specific configuration details.

4. **Run the migration**:

Execute the migration by calling the MigrateCatalog instance.

```python
migration()
```

Example Notebook:

For a hands-on example, refer to the provided Jupyter notebook in the repository [example.ipynb](https://github.com/databricks/uc-catalog-migration/blob/main/example.ipynb).

## Customization
Customize the migration process by modifying the MigrateCatalog class and its methods to suit your specific requirements.

## Issues and Support:
If you encounter any issues or have questions, please check the project's issue tracker on GitHub or reach out to the community for support.

## Contributing
We welcome contributions from the open-source community to make this migration script even more powerful and versatile. Please follow these guidelines:

- Fork the repository and create a feature branch for your contributions.
- Ensure that your code adheres to PEP 8 style guidelines.
- Write clear and concise documentation for your changes.
- Create tests for your code to maintain code quality.
- Submit a pull request for review.

## LICENSE
Please see [LICENSE](https://github.com/esiol-db/uc-catalog-migration/blob/main/LICENSE)
