# Integrations

## Databricks

- Databricks Lakeflow / DLT is the primary runtime target for generated pipeline code
- Databricks Asset Bundles are integrated through resource generation in `resources/lhp/*.yml`
- Generated extraction notebooks for `jdbc_watermark_v2` are intended to run as Databricks Job notebook tasks
- The repo assumes Databricks secret access patterns such as `dbutils.secrets.get(...)`

Relevant files:

- `src/lhp/generators/bundle/workflow_resource.py`
- `src/lhp/templates/bundle/workflow_resource.yml.j2`
- `src/lhp/templates/init/bundle/databricks.yml.j2`
- `docs/databricks_bundles.rst`

## External Data Systems

- JDBC sources are supported through `src/lhp/generators/load/jdbc.py` and `src/lhp/generators/load/jdbc_watermark_job.py`
- Watermarking targets JDBC-compatible operational databases such as Postgres, SQL Server, Oracle, and MySQL
- Kafka ingestion is supported through `src/lhp/generators/load/kafka.py`
- Cloud object storage / Unity Catalog Volumes are assumed for landed files used by Auto Loader

Relevant files:

- `src/lhp/core/validators/load_validator.py`
- `src/lhp/core/config_field_validator.py`
- `src/lhp/generators/load/cloudfiles.py`
- `src/lhp/extensions/watermark_manager/_manager.py`

## Secrets And Environment Resolution

- Secret references are expressed as `${secret:scope/key}` in YAML and converted into runtime code
- Environment substitutions are applied before validation through `EnhancedSubstitutionManager`

Relevant files:

- `src/lhp/utils/substitution.py`
- `src/lhp/utils/secret_code_generator.py`
- `src/lhp/core/services/flowgroup_processor.py`

## Downstream Consumer Context

- The current hardening effort is driven by the Wumbo deployment shape: multiple extraction notebooks feeding a single DLT pipeline
- The Lakehouse Plumber repo itself does not vendor Wumbo, but the design constraints come from that generated topology

