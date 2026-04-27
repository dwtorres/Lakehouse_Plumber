========================
Error Reference
========================

.. meta::
   :description: Troubleshooting guide with all Lakehouse Plumber error codes, causes, and resolution steps.

Overview
========

When Lakehouse Plumber encounters a problem, it displays a structured error with a unique
code in the format ``LHP-{CATEGORY}-{NUMBER}``:

- **LHP** — Lakehouse Plumber prefix
- **CATEGORY** — The error category (e.g. ``CFG`` for configuration, ``VAL`` for validation)
- **NUMBER** — A unique number within that category

Here is what an error looks like in your terminal:

.. code-block:: text
   :caption: Example error output

   ❌ Error [LHP-VAL-001]: Missing required field 'source'
   ======================================================================

   The Load action 'load_customers' requires a 'source' field. This field
   specifies where to read data from.

   📍 Context:
      • Component Type: Load action
      • Component Name: load_customers
      • Missing Field: source

   💡 How to fix:
      1. Add the 'source' field to your configuration
      2. Check the example below for the correct format

   📝 Example:
      actions:
        - name: load_customers
          type: load
          source:
            type: cloudfiles
            path: /data/customers/

   📚 More info: https://docs.lakehouseplumber.com/errors/lhp-val-001
   ======================================================================

Each error includes the cause, relevant context, numbered fix suggestions, and a
configuration example where applicable. Search this page for your error code to find
detailed resolution steps.

.. tip::

   Use the ``--verbose`` flag with any LHP command to see additional debug information
   that can help diagnose the issue.

Error Categories
================

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - Category
     - Prefix
     - Description
   * - Configuration
     - ``CFG``
     - Invalid or conflicting settings in YAML files, presets, templates, or bundle configuration
   * - Validation
     - ``VAL``
     - Missing required fields, invalid field values, or structural problems in action definitions
   * - I/O
     - ``IO``
     - Files not found, read/write failures, or file format issues
   * - Action
     - ``ACT``
     - Unknown action types, subtypes, or presets
   * - Dependency
     - ``DEP``
     - Circular dependencies between views or preset inheritance cycles
   * - Manifest
     - ``MAN``
     - B2 batch-manifest runtime errors: MERGE retry exhaustion, ownership conflicts, missing rows, or DAB taskValue payload overflow

Configuration Errors (LHP-CFG)
==============================

Configuration errors indicate problems with your YAML files, presets, templates,
or Databricks Asset Bundle setup. They are the most common error category.

LHP-CFG-001: Configuration Conflict
------------------------------------

**When it occurs:** You have specified the same configuration option in multiple ways,
typically when both legacy and new-format fields are present in the same action.

**Common causes:**

- Using both a top-level field and its equivalent under ``options``
- A preset defines a value that conflicts with an explicit value in the action

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-001)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
         format: json              # Legacy top-level field
         options:
           cloudFiles.format: json  # Same setting in new format

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
         options:
           cloudFiles.format: json  # Use only the new format

.. seealso::

   :doc:`actions/index` for the current configuration format for each action type.

LHP-CFG-006: Invalid Event Log Configuration
---------------------------------------------

**When it occurs:** The ``event_log`` section in ``lhp.yaml`` is not a valid YAML mapping,
or its field values have incorrect types.

**Common causes:**

- Providing a scalar value (string, number, boolean) instead of a mapping
- Using invalid types for fields (e.g., a list for ``catalog``)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-006)

   # lhp.yaml
   event_log: "enable_logging"     # String, but a mapping is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

.. seealso::

   :doc:`monitoring` for all available event log and monitoring configuration options.

LHP-CFG-007: Incomplete Event Log Configuration
------------------------------------------------

**When it occurs:** The ``event_log`` section in ``lhp.yaml`` is enabled but missing
required fields (``catalog`` and/or ``schema``).

**Common causes:**

- Defining ``event_log`` without specifying ``catalog`` or ``schema``
- Forgetting to add both required fields when enabling event logging

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-007)

   # lhp.yaml
   event_log:
     name_suffix: "_event_log"
     # Missing: catalog and schema!

.. code-block:: yaml
   :caption: After (fixed) — provide both required fields

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

.. code-block:: yaml
   :caption: Alternative fix — disable event logging

   # lhp.yaml
   event_log:
     enabled: false
     name_suffix: "_event_log"

.. note::

   When ``enabled`` is ``true`` (the default), both ``catalog`` and ``schema`` are required.
   Set ``enabled: false`` to define the section without activating event log injection.

LHP-CFG-008: Invalid Monitoring Configuration
----------------------------------------------

**When it occurs:** The ``monitoring`` section in ``lhp.yaml`` is not a valid YAML mapping,
its field values have incorrect types, or it fails cross-validation with the ``event_log``
section. This error code covers several monitoring validation scenarios:

**Scenario 1: monitoring is not a mapping**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   monitoring: "enable_monitoring"     # String, but a mapping is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

**Scenario 2: materialized_views is not a list**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views: "events_summary"   # String, but a list is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views:
       - name: "events_summary"
         sql: "SELECT * FROM all_pipelines_event_log"

**Scenario 3: monitoring requires event_log**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   # event_log is missing or disabled!
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. code-block:: yaml
   :caption: After (fixed) — add event_log

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. code-block:: yaml
   :caption: Alternative fix — disable monitoring

   # lhp.yaml
   monitoring:
     enabled: false

**Scenario 4: monitoring.checkpoint_path is missing**

``checkpoint_path`` is required whenever ``monitoring.enabled`` is ``true``. Each
streaming query in the generated union notebook writes to its own checkpoint directory
at ``{checkpoint_path}/{pipeline_name}/``.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
   monitoring: {}      # Missing checkpoint_path

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

**Additional validation (all LHP-CFG-008):**

- Each materialized view entry must be a YAML mapping with at least a ``name`` field
- MV names must be unique within the ``materialized_views`` list
- Each MV must specify either ``sql`` or ``sql_path``, not both

.. seealso::

   :doc:`monitoring` for complete monitoring configuration reference and examples.

LHP-CFG-009: YAML Parsing Error
--------------------------------

**When it occurs:** A YAML file contains invalid syntax that cannot be parsed.

**Common causes:**

- Incorrect indentation (mixing tabs and spaces)
- Missing colons after keys
- Unquoted strings containing special characters (``:`` ``#`` ``{`` ``}`` ``[`` ``]``)
- Unclosed quotes or brackets

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-009)

   actions:
     - name: load_events
       type: load
       source:
         path: /data/{date}/events  # Braces need quoting
         comment: Load events: raw  # Colon in value needs quoting

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         path: "/data/{date}/events"     # Quoted
         comment: "Load events: raw"     # Quoted

.. tip::

   Use a YAML linter (``yamllint`` or your IDE's YAML extension) to catch syntax
   issues before running ``lhp validate``.

LHP-CFG-010: Deprecated Field
------------------------------

**When it occurs:** Your configuration uses a field name that has been removed
or replaced in a newer version of Lakehouse Plumber.

**Common causes:**

- Using a field name from an older version of LHP
- Copy-pasting examples from outdated documentation

The error message tells you exactly which field to use instead. Replace the
deprecated field with the suggested replacement.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-010)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         schemaHints: "id BIGINT, name STRING"   # Deprecated field

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         options:
           cloudFiles.schemaHints: "id BIGINT, name STRING"   # New format

LHP-CFG-012: Missing Template Parameters
-----------------------------------------

**When it occurs:** A template requires parameters that were not provided in
the ``template_parameters`` section of the flowgroup.

**Common causes:**

- Forgetting to add ``template_parameters`` when using a template
- Misspelling a parameter name
- Using a template that was recently updated with new required parameters

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-012)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load
   # Missing template_parameters!
   actions:
     - name: load_data
       type: load

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load
   template_parameters:
     source_path: /data/raw/events    # Required by template
     file_format: json                # Required by template
   actions:
     - name: load_data
       type: load

.. seealso::

   :doc:`templates_reference` for how templates and parameters work.

LHP-CFG-018: Landing Path Overlaps Bronze Write Schema
------------------------------------------------------

**When it occurs:** A ``jdbc_watermark_v2`` action's ``landing_path`` resolves to the
same Unity Catalog catalog and schema as the flowgroup's bronze write target.

**Why it is an error:** AutoLoader reads Parquet from a UC managed volume and the Delta
table write target both require exclusive ownership of their UC schema location. When they
share a schema, Databricks raises ``LOCATION_OVERLAP`` at pipeline startup and refuses to
create the table.

**Common causes:**

- Copied a flowgroup YAML and forgot to update ``landing_path`` to a dedicated schema
- Used the same ``<catalog>/<schema>`` segment in the volume path as in ``write_target``

**Resolution:**

1. Change ``landing_path`` to a UC volume in a dedicated landing schema, e.g.
   ``/Volumes/<catalog>/landing_<schema>/<table>``
2. Or use an external location (``abfss://``) that is outside Unity Catalog managed storage

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-018)

   landing_path: /Volumes/bronze_catalog/bronze_schema/landing/product
   write_target:
     catalog: bronze_catalog
     schema: bronze_schema

.. code-block:: yaml
   :caption: After (fixed)

   landing_path: /Volumes/bronze_catalog/landing_bronze/product
   write_target:
     catalog: bronze_catalog
     schema: bronze_schema

.. seealso::

   ADR-003 §Q5 for the full analysis of the ``UC LOCATION_OVERLAP`` failure mode.

LHP-CFG-020: Bundle Resource Error
-----------------------------------

**When it occurs:** An error occurred while generating or syncing Databricks Asset Bundle
resource files during ``lhp generate``.

**Common causes:**

- Invalid YAML in existing bundle resource files under ``resources/lhp/``
- File permission issues preventing writes to the resources directory
- Corrupted resource files from a previous interrupted generation

**Resolution:**

1. Run ``lhp validate --env <env>`` to check your configuration
2. Check that files under ``resources/lhp/`` are valid YAML
3. If files are corrupted, delete them and re-run ``lhp generate --env <env> --force``

.. seealso::

   :doc:`databricks_bundles` for bundle setup and configuration.

LHP-CFG-021: Bundle YAML Processing Error
------------------------------------------

**When it occurs:** A bundle-related YAML file (such as ``databricks.yml`` or a resource
file) could not be processed.

**Common causes:**

- Invalid YAML syntax in ``databricks.yml``
- Malformed resource YAML files under ``resources/lhp/``
- Encoding issues (file is not UTF-8)

**Resolution:**

1. Validate ``databricks.yml`` with a YAML linter
2. Check for syntax errors at the line number shown in the error details
3. Ensure all YAML files use UTF-8 encoding

LHP-CFG-022: Missing databricks.yml
------------------------------------

**When it occurs:** Bundle operations require a ``databricks.yml`` file, but it was
not found in the project root.

**Common causes:**

- Running bundle commands in a project that was not initialized with bundle support
- Running commands from the wrong directory

.. code-block:: bash
   :caption: Resolution options

   # Option 1: Initialize a new project with bundle support
   lhp init my_project

   # Option 2: Skip bundle operations
   lhp generate --env dev --no-bundle

LHP-CFG-023: Missing Databricks Bundle Targets
-----------------------------------------------

**When it occurs:** Substitution files exist for environments (e.g., ``substitutions/dev.yaml``)
but the corresponding targets are not defined in ``databricks.yml``.

**Common causes:**

- Adding a new substitution file without updating ``databricks.yml``
- Renaming a target in ``databricks.yml`` without updating the substitution file name

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-023) — substitutions/staging.yaml exists but...

   # databricks.yml
   targets:
     dev:
       default: true
     prod:
       mode: production
     # Missing: staging target!

.. code-block:: yaml
   :caption: After (fixed)

   # databricks.yml
   targets:
     dev:
       default: true
     staging:                    # Added to match substitutions/staging.yaml
       mode: development
     prod:
       mode: production

.. seealso::

   :doc:`databricks_bundles` for target configuration details.

LHP-CFG-024: Bundle Template Error
-----------------------------------

**When it occurs:** An error occurred while fetching or processing bundle templates
during project initialization.

**Common causes:**

- Network connectivity issues when downloading templates
- Invalid template URL or path
- Template file is corrupted or in unexpected format

**Resolution:**

1. Check your internet connection if using remote templates
2. Verify the template path or URL is correct
3. Try running the command again

LHP-CFG-025: Bundle Configuration Error
----------------------------------------

**When it occurs:** The ``databricks.yml`` file or bundle configuration has structural
problems that prevent LHP from processing it.

**Common causes:**

- Missing required fields in ``databricks.yml``
- Invalid YAML structure in the bundle configuration
- Incompatible bundle configuration format

**Resolution:**

1. Review your ``databricks.yml`` against the Databricks Asset Bundles documentation
2. Run ``lhp validate`` for detailed diagnostics
3. Compare with a working project's ``databricks.yml``

LHP-CFG-027: Template Not Found
--------------------------------

**When it occurs:** The flowgroup references a template name that does not exist
in the ``templates/`` directory.

**Common causes:**

- Typo in the template name
- The template file is missing from the ``templates/`` directory
- Using a template name without the correct file extension

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-027)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: stanard_cloudfiles_load    # Typo!

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load   # Correct spelling

.. tip::

   Run ``lhp list_templates`` to see all available template names.

LHP-CFG-031: Separator Collision in for_each Pipeline or Flowgroup Name
------------------------------------------------------------------------

**When it occurs:** A flowgroup declares ``workflow.execution_mode: for_each`` and
either the ``pipeline`` or the ``flowgroup`` field contains the literal ``::`` string.

**Why it is an error:** The B2 batch-manifest system uses ``<pipeline>::<flowgroup>``
as a composite row key. A ``::`` already present in either component makes the key
unparseable and corrupt.

**Common causes:**

- Using ``::`` as a hierarchical separator in pipeline or flowgroup names before
  the B2 feature was available
- Copying a legacy flowgroup name convention to a new for_each flowgroup

**Resolution:**

Replace ``::`` with a different separator (e.g. ``-`` or ``_``) in the ``pipeline``
or ``flowgroup`` field.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-031)

   pipeline: bronze::core
   workflow:
     execution_mode: for_each

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: bronze_core
   workflow:
     execution_mode: for_each

**Note:** Legacy flowgroups that do *not* use ``execution_mode: for_each`` are not
affected by this check and may continue using ``::`` in their names.

LHP-CFG-032: Composite load_group Not Unique Within Project
------------------------------------------------------------

**When it occurs:** Two or more flowgroups that both declare
``workflow.execution_mode: for_each`` produce the same composite key
``<pipeline>::<flowgroup>``.

**Why it is an error:** The B2 manifest stores one row per concrete action using the
composite as a namespace identifier. Duplicate composites cause manifest rows from
separate flowgroups to collide, corrupting watermark and iteration state.

**Common causes:**

- Renaming a flowgroup mid-development without checking for an existing flowgroup
  with the same pipeline + flowgroup combination

**Resolution:**

Rename the ``pipeline`` or ``flowgroup`` field in one of the conflicting flowgroups
so that the resulting composite ``<pipeline>::<flowgroup>`` is unique across the
project.

LHP-CFG-033: for_each Post-Expansion Structural Violation
----------------------------------------------------------

**When it occurs:** This code covers four distinct structural checks that run after
template expansion for flowgroups with ``workflow.execution_mode: for_each``:

1. **Action count out of bounds** — fewer than 1 or more than 300 actions.
2. **Shared-key disagreement** — ``jdbc_watermark_v2`` actions in the same flowgroup
   differ in ``source_system_id``, ``landing_path`` root prefix, ``wm_catalog``, or
   ``wm_schema``.
3. **Concurrency out of bounds** — ``workflow.concurrency`` is present and not in
   ``[1, 100]``.
4. **Mixed-mode pipeline** — a pipeline contains both ``for_each`` and non-``for_each``
   flowgroups.

**Why it is an error:** The B2 orchestrator issues a single ``iterations`` taskValue
for the entire flowgroup and fans out over all actions in parallel. Structural
inconsistencies prevent the manifest from being populated or the fan-out from being
deterministic.

**Resolutions by sub-check:**

1. *Action count:* If zero, verify ``use_template`` + ``template_parameters`` produce
   at least one action. If >300, split by source schema prefix or table subset into
   multiple flowgroups — batch-scoped manifests isolate them.
2. *Shared keys:* Ensure all actions in the flowgroup target the same source system
   (``source_system_id``), landing zone root (``landing_path``), and watermark
   catalog/schema. Split into separate flowgroups if they belong to different systems.
3. *Concurrency:* Set ``workflow.concurrency`` to a value in ``[1, 100]``, or omit it
   to use the default ``min(action_count, 10)``.
4. *Mixed mode:* Move non-``for_each`` flowgroups to a separate pipeline, or convert
   all flowgroups in the pipeline to ``for_each``.

.. rubric:: YAML examples — action count (facet 1)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-033 — 301 actions after template expansion)

   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each
   template_parameters:
     tables: [t001, t002, ... t301]   # 301 entries — exceeds 300-action limit

.. code-block:: yaml
   :caption: After (fixed — split into two flowgroups)

   # flowgroup A: first 150 tables
   pipeline: crm_bronze
   flowgroup: product_ingestion_a
   workflow:
     execution_mode: for_each
   template_parameters:
     tables: [t001, ... t150]

   # flowgroup B: remaining 151 tables
   pipeline: crm_bronze
   flowgroup: product_ingestion_b
   workflow:
     execution_mode: for_each
   template_parameters:
     tables: [t151, ... t301]

.. rubric:: YAML examples — shared-key disagreement (facet 2)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-033 — two source systems in one flowgroup)

   pipeline: crm_bronze
   flowgroup: mixed_sources
   workflow:
     execution_mode: for_each
   actions:
     - name: load_crm_orders
       type: load
       source_system_id: crm_prod
       wm_catalog: metadata
       wm_schema: devtest_orchestration
     - name: load_erp_orders
       type: load
       source_system_id: erp_prod    # different source_system_id — violates shared-key rule
       wm_catalog: metadata
       wm_schema: devtest_orchestration

.. code-block:: yaml
   :caption: After (fixed — one flowgroup per source system)

   # crm flowgroup
   pipeline: crm_bronze
   flowgroup: crm_orders
   workflow:
     execution_mode: for_each
   actions:
     - name: load_crm_orders
       type: load
       source_system_id: crm_prod
       wm_catalog: metadata
       wm_schema: devtest_orchestration

   # erp flowgroup
   pipeline: crm_bronze
   flowgroup: erp_orders
   workflow:
     execution_mode: for_each
   actions:
     - name: load_erp_orders
       type: load
       source_system_id: erp_prod
       wm_catalog: metadata
       wm_schema: devtest_orchestration

.. rubric:: YAML examples — concurrency out of bounds (facet 3)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-033 — concurrency 200 exceeds maximum 100)

   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each
     concurrency: 200

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each
     concurrency: 10   # or omit entirely for default min(action_count, 10)

.. rubric:: YAML examples — mixed execution_mode in one pipeline (facet 4)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-033 — legacy flowgroup alongside for_each in same pipeline)

   # file: pipelines/crm_bronze/product_ingestion.yaml
   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each

   # file: pipelines/crm_bronze/reference_data.yaml
   pipeline: crm_bronze        # same pipeline name
   flowgroup: reference_data   # no execution_mode — legacy emission
   actions:
     - name: load_ref
       type: load

.. code-block:: yaml
   :caption: After (fixed — move legacy flowgroup to a separate pipeline)

   # file: pipelines/crm_bronze/product_ingestion.yaml
   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each

   # file: pipelines/crm_bronze_legacy/reference_data.yaml
   pipeline: crm_bronze_legacy   # different pipeline name — no mixed-mode
   flowgroup: reference_data
   actions:
     - name: load_ref
       type: load

LHP-CFG-034: Mixed ``execution_mode`` Flowgroups in Same Pipeline (Orchestrator)
----------------------------------------------------------------------------------

**When it occurs:** A pipeline contains a mix of flowgroups, some with
``workflow.execution_mode: for_each`` and some without (legacy per-action static
emission). The orchestrator merge guard catches this at codegen time as the
safety-net layer behind LHP-CFG-033 facet 4.

**Why it is an error:** B2 codegen merges all ``jdbc_watermark_v2`` actions across
flowgroups in a pipeline into one synthetic flowgroup and emits a single
``prepare_manifest → for_each_ingest → validate`` DAB workflow. Mixed
``execution_mode`` values cannot share a single workflow YAML — the manifest and
fan-out logic are incompatible with the legacy per-action emission topology.

**Common causes:**

- A new ``for_each`` flowgroup was added to an existing pipeline that already
  has one or more non-``for_each`` flowgroups.
- Template expansion produced a ``for_each`` flowgroup and a non-``for_each``
  flowgroup in the same pipeline scope.
- LHP-CFG-033 facet 4 was somehow bypassed at validation time and the
  orchestrator encountered the mixed state during codegen.

**Resolution:**

1. Move the ``for_each`` flowgroup(s) into a separate pipeline name (rename the
   ``pipeline:`` field), or
2. Remove ``workflow.execution_mode: for_each`` from the new flowgroup to keep
   the legacy per-action static emission for the entire pipeline.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-034 — mixed mode reaches orchestrator)

   # product_ingestion.yaml
   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each

   # reference_data.yaml
   pipeline: crm_bronze        # same pipeline — mixed mode not caught earlier
   flowgroup: reference_data
   actions:
     - name: load_ref
       type: load

.. code-block:: yaml
   :caption: After (fixed — separate pipelines)

   # product_ingestion.yaml
   pipeline: crm_bronze
   flowgroup: product_ingestion
   workflow:
     execution_mode: for_each

   # reference_data.yaml
   pipeline: crm_bronze_legacy   # separate pipeline; no mixed-mode
   flowgroup: reference_data
   actions:
     - name: load_ref
       type: load

.. note::

   LHP-CFG-033 (facet 4) is the primary validator-time guard for this condition.
   LHP-CFG-034 is a defence-in-depth backstop at orchestrator codegen time.
   If you see LHP-CFG-034 without a preceding LHP-CFG-033 in the same run,
   file an issue — the validator guard may have a gap.

.. seealso::

   :ref:`LHP-CFG-033 <LHP-CFG-033: for_each Post-Expansion Structural Violation>`
   for the validator-time check covering the same mixed-mode condition.

LHP-CFG-035: Non-Strict Watermark Operator for ``execution_mode: for_each``
----------------------------------------------------------------------------

**When it occurs:** A ``jdbc_watermark_v2`` action inside a ``for_each`` flowgroup
declares ``watermark.operator: '>='`` or ``watermark.operator: '<='``.

**Why it is an error:** Non-strict operators include the high-water-mark boundary
row on every re-run. In ``for_each`` batches the same boundary row is re-read and
re-landed on every execution, causing silent duplicate rows in the bronze layer.
The effect is invisible until an analyst notices inflated row counts.

**Common causes:**

- Copying a watermark action that previously used ``>=`` (valid outside ``for_each``)
  into a new ``for_each`` flowgroup without updating the operator.
- Accepting the Pydantic model default of ``>=`` without explicitly setting
  ``operator: '>'`` in the YAML — both look identical after model construction.
- Believing that ``>=`` is "safer" because it avoids row loss; in ``for_each`` the
  duplicate cost outweighs the fence-post benefit.

**Resolution:**

Set ``watermark.operator`` to ``'>'`` (ascending watermark, the common case for
timestamp and numeric columns) or ``'<'`` (descending watermark).

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-035)

   actions:
     - name: load_orders
       type: load
       source:
         type: jdbc_watermark_v2
       watermark:
         operator: ">="   # Non-strict — duplicates on re-run in for_each

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_orders
       type: load
       source:
         type: jdbc_watermark_v2
       watermark:
         operator: ">"    # Strict — excludes boundary row

.. seealso::

   The :ref:`R12 strict-\`>\` operator awareness <R12 strict-\`>\` operator awareness>`
   section of the B2 for-each rollout runbook for the operational context and
   sub-second precision requirements.

LHP-CFG-036: Pipeline Contains Multiple ``for_each`` Flowgroups
---------------------------------------------------------------

**When it occurs:** A pipeline contains two or more flowgroups that both declare
``workflow.execution_mode: for_each``. This is detected at ``lhp validate`` time
(via ``validate_project_invariants``) and at ``lhp generate`` time (inside
``_generate_workflow_resources``), whichever runs first.

**Why it is an error:** The DAB workflow generator emits one workflow YAML per
pipeline, keyed on the **first** ``for_each`` flowgroup it encounters. The second
(and any subsequent) flowgroup's ``__lhp_prepare_manifest_<fg>.py`` and
``__lhp_validate_<fg>.py`` aux files are generated on disk but never referenced
by the workflow YAML, so their actions never execute. The bug is silent — ``lhp
validate`` counts ``N expected / N completed`` from the one flowgroup that did run
and reports ``status: pass``.

**Common causes:**

- An engineer split a large set of JDBC tables into two ``for_each`` flowgroups
  within the same pipeline name (e.g., by source-system prefix) — a natural
  refactor that unexpectedly triggers this guard.
- A flowgroup YAML was copy-pasted and the ``pipeline:`` field was not changed to
  a new name, leaving two ``for_each`` flowgroups sharing one pipeline.
- Template expansion produces two flowgroups that both land on the same
  ``pipeline:`` value.

**Resolution:**

Choose one of two options:

1. **Consolidate:** If the two flowgroups target the same source system, same
   ``wm_catalog``, same ``wm_schema``, and the same ``landing_path`` root, merge
   their actions into a single ``for_each`` flowgroup. The 300-action cap
   (LHP-CFG-033) still applies.
2. **Split into separate pipelines:** Assign each ``for_each`` flowgroup its own
   ``pipeline:`` value. Separate pipelines each get their own DAB workflow YAML
   and execute independently.

If neither option fits your use case, file an issue describing the scenario — the
constraint exists because multi-for-each-per-pipeline workflow generation has not
yet been implemented.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-036 — two for_each flowgroups in one pipeline)

   # file: pipelines/crm_bronze/crm_orders.yaml
   pipeline: crm_bronze
   flowgroup: crm_orders
   workflow:
     execution_mode: for_each

   # file: pipelines/crm_bronze/crm_products.yaml
   pipeline: crm_bronze        # same pipeline — triggers LHP-CFG-036
   flowgroup: crm_products
   workflow:
     execution_mode: for_each

.. code-block:: yaml
   :caption: After (fixed — split into separate pipelines)

   # file: pipelines/crm_bronze_orders/crm_orders.yaml
   pipeline: crm_bronze_orders
   flowgroup: crm_orders
   workflow:
     execution_mode: for_each

   # file: pipelines/crm_bronze_products/crm_products.yaml
   pipeline: crm_bronze_products
   flowgroup: crm_products
   workflow:
     execution_mode: for_each

.. seealso::

   :ref:`LHP-CFG-034 <LHP-CFG-034: Mixed \`\`execution_mode\`\` Flowgroups in Same Pipeline (Orchestrator)>`
   for the related mixed-mode guard (``for_each`` + non-``for_each`` in one pipeline).

Manifest Errors (LHP-MAN)
==========================

Manifest errors occur at runtime inside Databricks notebooks generated by the B2
watermark scale-out feature. They are raised as ``RuntimeError("LHP-MAN-NNN: ...")`
prefix strings — the ``LHP-MAN-`` prefix is what operators should search for in
Databricks task logs. Codegen-time manifest errors (LHP-MAN-005) are raised before
deployment and appear in the ``lhp validate`` / ``lhp generate`` terminal output.

LHP-MAN-001: Manifest MERGE Retry Budget Exhausted
---------------------------------------------------

**When it occurs:** The ``prepare_manifest`` notebook attempts to MERGE a batch of
iteration rows into ``b2_manifests`` and the Delta concurrent-commit retry loop
exhausts its five-attempt budget without a successful commit.

**Why it is an error:** Without a committed MERGE, the manifest contains no rows
for this batch. Downstream ``for_each_ingest`` iterations receive no tasks and
the ``validate`` task reports a zero-expected-action noop pass — the batch is
silently skipped entirely.

**Common causes:**

- ``workflow.concurrency`` set too high relative to the Delta table's concurrent-commit
  capacity, causing many workers to write simultaneously and triggering frequent
  write-write conflicts on the ``b2_manifests`` table.
- A large number of concurrent pipelines all writing to the same ``b2_manifests``
  table (shared ``wm_catalog`` / ``wm_schema``) at the same time.
- Transient cluster instability causing Delta transaction log reads to fail mid-retry.

**Resolution:**

1. Reduce ``workflow.concurrency`` in the flowgroup YAML (valid range: 1-100;
   default is ``min(action_count, 10)``).
2. If multiple pipelines share the same ``wm_catalog`` / ``wm_schema``, stagger
   their schedule windows so their ``prepare_manifest`` runs do not overlap.
3. Inspect the Delta concurrent-commit metrics in the Databricks cluster Spark UI
   (look for ``numTransactionAborted`` in the Delta Lake metrics tab) to confirm
   the conflict rate.
4. As a last resort, set ``workflow.concurrency: 1`` to serialize all MERGE writes.

.. seealso::

   :ref:`LHP-MAN-002 <LHP-MAN-002: Manifest Claim Ownership Conflict>` for the
   related worker-side claim conflict that can occur when two DAB attempts race
   to claim the same manifest row.

LHP-MAN-002: Manifest Claim Ownership Conflict
-----------------------------------------------

**When it occurs:** A B2 worker iteration attempts to claim a manifest row via
UPDATE and reads back a ``worker_run_id`` that belongs to a different, currently-running
worker — indicating that a competing iteration has already taken ownership of that
row.

**Why it is an error:** The manifest's optimistic-concurrency ownership model
allows exactly one worker to own each row at a time. A ``worker_run_id`` mismatch
after the claim UPDATE means the current iteration's UPDATE matched zero rows
(because the competing owner's claim already set the row to ``execution_status:
'running'``). Proceeding would cause two workers to process the same action and
write conflicting watermark values.

**Common causes:**

- Two DAB for-each iterations are assigned to the same ``action_name`` in the same
  batch (which should not occur under normal DAB scheduling but can happen with
  misconfigured task parameters or a stale ``iterations`` taskValue from a previous
  run).
- A prior DAB attempt of the **same** iteration left the row in ``execution_status:
  'running'`` without transitioning it to ``'failed'`` (e.g., the worker process
  was killed between the claim UPDATE and the fail-mirror UPDATE). This scenario
  resolves on the next DAB retry once the row ages out or is manually reset.

**Resolution:**

1. This error is usually transient. DAB automatically retries the iteration
   (subject to the task ``max_retries`` setting). Check the Databricks Jobs UI
   for the ``for_each_ingest`` task — failed iterations appear in red and can be
   re-run individually with the ``Re-run failed iterations`` button.
2. If the error persists across all retries for a specific action, query
   ``b2_manifests`` for the row:

   .. code-block:: sql

      SELECT batch_id, action_name, worker_run_id, execution_status, updated_at
      FROM <wm_catalog>.<wm_schema>.b2_manifests
      WHERE batch_id = '<batch_id>'
        AND action_name = '<action_name>';

   If ``execution_status = 'running'`` and the owning ``worker_run_id`` task is
   no longer active in the Jobs UI, manually reset the row:

   .. code-block:: sql

      UPDATE <wm_catalog>.<wm_schema>.b2_manifests
      SET execution_status = 'failed', updated_at = current_timestamp()
      WHERE batch_id = '<batch_id>'
        AND action_name = '<action_name>';

   Then re-run the failed iteration from the Jobs UI.

.. seealso::

   :ref:`LHP-MAN-001 <LHP-MAN-001: Manifest MERGE Retry Budget Exhausted>` for the
   MERGE-side conflict that can precede ownership conflicts.

LHP-MAN-003: Manifest Row Missing for Action
--------------------------------------------

**When it occurs:** A B2 worker iteration reads back the ``b2_manifests`` row after
the claim UPDATE and finds zero rows matching ``(batch_id, action_name)``.

**Why it is an error:** The claim UPDATE matched zero rows (expected), but instead
of a competing owner, there is no row at all for this action in the current batch.
The iteration cannot proceed without a manifest row to track ownership and
execution status.

**Common causes:**

- The ``prepare_manifest`` task was skipped, failed before the MERGE committed,
  or ran with a different ``batch_id`` than the one the worker task received.
- A manual DELETE was run against ``b2_manifests`` for this ``batch_id`` while
  the job was in flight.
- The ``iterations`` taskValue was stale (from a previous job run) and refers to
  a ``batch_id`` for which the corresponding manifest rows have already been cleaned
  up by the 30-day retention DELETE.

**Resolution:**

1. Check whether ``prepare_manifest`` completed successfully in the Databricks Jobs
   UI (it must show ``Succeeded`` before ``for_each_ingest`` starts).
2. If ``prepare_manifest`` failed, inspect its logs for LHP-MAN-001 (MERGE retry
   exhaustion) or LHP-MAN-005 (payload overflow).
3. Re-run the entire workflow from the beginning (not just the failed iteration)
   so that ``prepare_manifest`` generates a fresh ``batch_id`` and a fresh set of
   manifest rows.
4. If this occurs after a manual DELETE, re-run the workflow to regenerate the
   rows.

LHP-MAN-004: Completion Mirror MERGE Retry Budget Exhausted
-----------------------------------------------------------

**When it occurs:** A B2 worker iteration successfully extracts data and commits
the watermark, then attempts to transition the manifest row to
``execution_status: 'completed'`` via a Delta UPDATE (the completion mirror). The
concurrent-commit retry loop exhausts its budget before the UPDATE commits.

**Why it is an error:** The watermark is updated (the actual data landed correctly),
but the manifest row remains in ``execution_status: 'running'``. The ``validate``
task uses ``coalesce(worker_status, manifest_status)`` to determine final state;
however, a row stuck in ``'running'`` contributes to the ``unfinished_n`` count,
causing LHP-VAL-050 to fire even though the data was successfully landed.

**Common causes:**

- The same high-concurrency conditions that cause LHP-MAN-001: many workers
  simultaneously trying to UPDATE the same ``b2_manifests`` table.
- A transient network or cluster failure between the watermark commit and the
  completion mirror UPDATE.

**Resolution:**

1. Reduce ``workflow.concurrency`` (same mitigation as LHP-MAN-001).
2. If the validate task reports LHP-VAL-050 for specific actions, check whether
   those actions' watermark rows are present and completed in the watermarks
   registry:

   .. code-block:: sql

      SELECT run_id, status, watermark_value, updated_at
      FROM metadata.<env>_orchestration.watermarks
      WHERE load_group = '<pipeline>::<flowgroup>'
        AND action_name = '<action_name>'
      ORDER BY updated_at DESC
      LIMIT 5;

   If the watermark row shows ``status: 'completed'``, the data landed correctly.
   The manifest row can be manually corrected:

   .. code-block:: sql

      UPDATE <wm_catalog>.<wm_schema>.b2_manifests
      SET execution_status = 'completed', updated_at = current_timestamp()
      WHERE batch_id = '<batch_id>'
        AND action_name = '<action_name>';

3. Re-run ``validate`` as a standalone task (without re-running extraction) after
   the manual correction.

.. seealso::

   :ref:`LHP-MAN-001 <LHP-MAN-001: Manifest MERGE Retry Budget Exhausted>` for the
   MERGE-side exhaustion pattern that shares the same root cause.
   :ref:`LHP-VAL-050 <LHP-VAL-050: B2 Validate Failed>` for the validate-task
   error that surfaces when completion mirror failures leave unfinished rows.

LHP-MAN-005: Manifest taskValue Payload Exceeds DAB 48 KB Ceiling
------------------------------------------------------------------

**When it occurs:** Two distinct guards fire this code at different lifecycle stages:

- **Codegen time** (``lhp validate`` / ``lhp generate``): the ``_validate_for_each_invariants``
  validator estimates the JSON payload that ``prepare_manifest`` will set via
  ``dbutils.jobs.taskValues.set(key="iterations", ...)`` and finds that the
  projected size × 1.10 fudge factor exceeds the DAB 48 KB ceiling.
- **Runtime** (inside the generated ``prepare_manifest`` notebook): the actual
  serialized payload ``json.dumps(_iterations)`` exceeds 48 × 1024 bytes, immediately
  before the ``dbutils.jobs.taskValues.set`` call.

**Why it is an error:** DAB hard-rejects any ``taskValues.set`` call whose value
exceeds 48 KB. If the set call is allowed to run with an oversized payload, DAB
raises an unstructured SDK exception. The runtime guard fires before the set call
so the error message is operator-readable. The codegen guard fires before
deployment so the operator can fix the configuration without ever running the job.

When the runtime guard fires, the ``b2_manifests`` rows for the failing batch have
already been MERGE'd (they are committed). The downstream ``for_each_ingest``
workers never receive an ``iterations`` taskValue and fail immediately at
LHP-VAL-048. The ``validate`` task (if reached) fails at LHP-VAL-050.

**Common causes:**

- A large number of actions combined with long field values. Each iteration entry
  carries 10 keys; entries average 350–550 bytes. The ceiling is reached at
  approximately 89–140 actions depending on field length.
- Long ``landing_path`` values (e.g., deeply nested Unity Catalog volume paths).
- Long ``jdbc_table`` or ``schema_name`` identifiers.
- Adding more keys to the iteration payload than the 10-key contract
  (``B2_ITERATION_KEYS``) defines — this requires a coordinated three-file edit
  and will push the per-entry size higher.

**Resolution:**

At **codegen time** (LHP-MAN-005 from ``lhp validate``):

1. Reduce action count by splitting the flowgroup into two smaller flowgroups, each
   in its own pipeline.
2. Shorten ``landing_path`` or ``jdbc_table`` identifiers.
3. Re-run ``lhp validate`` until the projected payload is under the ceiling.

At **runtime** (LHP-MAN-005 from ``prepare_manifest`` logs):

1. Apply the same codegen-time fixes and redeploy.
2. Clean up the orphaned ``b2_manifests`` rows for the failed batch:

   .. code-block:: sql

      DELETE FROM <wm_catalog>.<wm_schema>.b2_manifests
      WHERE batch_id = '<failed_batch_id>';

   Substitute the ``batch_id`` value from the ``manifest entries:`` log line
   that appears immediately before the LHP-MAN-005 error in ``prepare_manifest``
   stdout.
3. Re-run the workflow after redeployment. The 30-day retention DELETE in
   ``prepare_manifest`` will also clean up these rows automatically, but the manual
   DELETE avoids them appearing as stale rows in operational queries.

.. note::

   The ``prepare_manifest`` DAB task is configured with ``max_retries: 0``.
   LHP-MAN-005 is a deterministic failure — the same batch ID, the same action
   count, and the same field values produce the same oversized payload on every
   attempt. Retrying without a config change wastes DAB retry budget.

.. seealso::

   :ref:`LHP-VAL-048 <LHP-VAL-048: B2 Validate batch_id taskValue Absent>` for
   the worker-side error that fires when ``prepare_manifest`` does not emit the
   ``iterations`` taskValue.
   :ref:`LHP-CFG-033 <LHP-CFG-033: for_each Post-Expansion Structural Violation>`
   for the structural action-count cap (300) that is a companion limit.

Watermark Errors (LHP-WM)
==========================

Watermark errors are raised by the vendored ``lhp_watermark`` package inside
generated Databricks notebooks. They surface in worker task logs as the
exception class name (e.g., ``DuplicateRunError``) plus the ``LHP-WM-NNN``
compact code in the exception ``str()``.

LHP-WM-001: Duplicate run_id (DuplicateRunError)
-------------------------------------------------

**When it occurs:** A B2 worker iteration calls ``WatermarkManager.insert_new``
and the underlying MERGE matches an existing target row in the watermarks
table — the ``run_id`` derived for this iteration already exists.

**Why it is an error:** ``insert_new`` is the row-creation step for a fresh
extraction attempt. A matching existing row means a prior run already used
this ``run_id`` (replay, partial commit, or run-id collision via the
``__lhp_run_id_override`` widget or DAB task counter reset). Continuing would
either re-process the same source slice or merge into a foreign row's state
machine. The worker raises ``DuplicateRunError`` and aborts cleanly: it does
**not** call ``mark_failed`` (no row was created for this attempt to
transition) and does **not** mirror ``'failed'`` to the manifest (the issue
is run-id provenance, not extraction failure). The manifest row stays in its
claim-time ``'running'`` state. The worker also emits a structured
``_log_phase("duplicate_run_id_abort", error_code="LHP-WM-001", ...)`` event
in the task log so operators can locate the breadcrumb directly.

**Common causes:**

- Operator supplied an ``__lhp_run_id_override`` widget value that collides
  with a ``run_id`` already present in the watermarks table.
- A bundle redeploy or task-counter reset caused ``derive_run_id`` to
  reproduce a previously-used ``(jobRunId, taskRunId, attempt)`` triple,
  re-deriving an existing ``run_id``.
- A partial-commit replay where the prior run successfully wrote the
  watermarks row but failed before completing — the original ``run_id`` is
  still present on retry.

**Resolution:**

1. Locate the worker task log entry with ``"duplicate_run_id_abort"`` and
   note the ``run_id``, ``batch_id``, and ``action_name``.
2. Query the watermarks table for the existing row:

   .. code-block:: sql

      SELECT run_id, status, watermark_value, source_system_id, schema_name,
             table_name, created_at, updated_at
      FROM <wm_catalog>.<wm_schema>.lhp_watermarks
      WHERE run_id = '<run_id>';

3. If the existing row is from a known-completed prior run, the duplicate
   indicates a ``run_id`` provenance bug (override widget collision or
   redeploy reset). Clear the ``__lhp_run_id_override`` widget if set, or
   redeploy the bundle to force fresh DAB task identifiers.
4. The ``b2_manifests`` row for the affected action remains in
   ``execution_status: 'running'``. The ``validate`` task surfaces this as
   ``final_status = 'running'`` (LHP-VAL-050 fires) and the batch fails
   loudly. After resolving the run-id provenance, manually reset the
   manifest row (see LHP-MAN-002 resolution step 2) and re-run the failed
   iteration.

.. seealso::

   :ref:`LHP-MAN-002 <LHP-MAN-002: Manifest Claim Ownership Conflict>` for
   the related manifest-row reset procedure.
   :ref:`LHP-VAL-050 <LHP-VAL-050: B2 Validate Failed>` for the validate-side
   surfacing of the still-``running`` manifest row produced by this abort.

Validation Errors (LHP-VAL)
============================

Validation errors indicate that your configuration is syntactically valid YAML but
contains values that are structurally incorrect, missing, or incompatible.

LHP-VAL-001: Missing Required Field
------------------------------------

**When it occurs:** An action is missing a field that is required for its type.

**Common causes:**

- Forgetting to add ``source``, ``target``, or ``type`` to an action
- Incomplete action definition after copy-pasting from another flowgroup
- Template expansion that does not provide all required fields

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-001)

   actions:
     - name: load_customers
       type: load
       # Missing: source configuration!
       target: v_raw_customers

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_customers
       type: load
       source:
         type: cloudfiles
         path: /data/customers/
         options:
           cloudFiles.format: csv
       target: v_raw_customers

.. tip::

   The error message includes the specific field name that is missing and an
   example of the correct configuration.

LHP-VAL-002: Validation Failed
-------------------------------

**When it occurs:** An action or component has multiple validation issues that
were detected together during ``lhp validate`` or ``lhp generate``.

**Common causes:**

- Missing source view reference
- Invalid target reference
- Circular dependency in view definitions
- Multiple structural issues in a single action

The error details list each individual issue with a ``✗`` marker. Address each
item in the list to resolve this error.

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-002)

   actions:
     - name: process_data
       type: transform
       sub_type: sql
       # Missing: source
       # Missing: target
       sql: |
         SELECT * FROM somewhere

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: process_data
       type: transform
       sub_type: sql
       source: v_raw_data
       target: v_processed
       sql: |
         SELECT * FROM $source

.. note::

   In SQL transforms, ``$source`` is automatically replaced with the view name
   specified in the ``source`` field. See :doc:`actions/index` for details
   on SQL transform syntax.

LHP-VAL-006: Invalid Field Value
---------------------------------

**When it occurs:** A field has a value that is not in the set of allowed values.

**Common causes:**

- Typo in a value (e.g., ``streeming`` instead of ``streaming``)
- Using a value from a different context (e.g., a write target type in a load action)
- Case sensitivity issues

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-006)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
       target: v_events
       write_target:
         type: streeming_table    # Typo!

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
       target: v_events
       write_target:
         type: streaming_table    # Correct spelling

LHP-VAL-007: Invalid readMode
------------------------------

**When it occurs:** The ``readMode`` value is not valid for the action type.

**Common causes:**

- Using an unsupported readMode value
- Applying a readMode that is incompatible with the action's source type

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-007)

   actions:
     - name: load_events
       type: load
       readMode: continuous       # Not a valid readMode
       source:
         type: cloudfiles
         path: /data/events/

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       readMode: stream           # Valid: 'stream' or 'batch'
       source:
         type: cloudfiles
         path: /data/events/

.. note::

   Valid readMode values are ``stream`` (for ``spark.readStream``, the default) and
   ``batch`` (for ``spark.read``). SQL transforms reading from streaming sources must
   use ``stream(view_name)`` in the SQL expression.

LHP-VAL-008: Invalid Field Type
--------------------------------

**When it occurs:** A field has the wrong data type (e.g., a string where a list is
expected, or a number where a boolean is expected).

**Common causes:**

- Providing a string where a dictionary/object is expected
- YAML auto-conversion (e.g., ``yes``/``no`` converting to boolean)
- Passing a single value where a list is required

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-008)

   actions:
     - name: load_events
       type: load
       source: /data/events/      # String, but cloudfiles expects a dict

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:                     # Dictionary with required fields
         type: cloudfiles
         path: /data/events/

LHP-VAL-010: Duplicate Monitoring Pipeline Configuration
---------------------------------------------------------

**When it occurs:** Both the ``__eventlog_monitoring`` alias and the actual monitoring
pipeline name are defined as separate entries in ``pipeline_config.yaml``.

**Common causes:**

- Using the alias while also explicitly targeting the monitoring pipeline by its real name
- Copy-pasting a config document and forgetting to remove the duplicate

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-010)

   ---
   pipeline: __eventlog_monitoring
   serverless: false

   ---
   pipeline: acme_edw_event_log_monitoring
   serverless: true

.. code-block:: yaml
   :caption: After (fixed) — use only one

   ---
   pipeline: __eventlog_monitoring
   serverless: false

.. seealso::

   :doc:`monitoring` for details on the ``__eventlog_monitoring`` reserved keyword
   and monitoring pipeline configuration.

LHP-VAL-011: Monitoring Alias in Pipeline List / Schema Syntax Error
---------------------------------------------------------------------

This error code covers two validation scenarios.

**Scenario 1: Monitoring Alias in Pipeline List**

**When it occurs:** The ``__eventlog_monitoring`` alias is used inside a pipeline list
(e.g., ``pipeline: [bronze, __eventlog_monitoring]``) instead of as a standalone entry.

**Common causes:**

- Grouping the monitoring alias with other pipelines in a list
- Attempting to share configuration between regular pipelines and the monitoring pipeline

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-011)

   ---
   pipeline:
     - bronze_pipeline
     - __eventlog_monitoring
   serverless: false

.. code-block:: yaml
   :caption: After (fixed) — separate documents

   ---
   pipeline: bronze_pipeline
   serverless: false

   ---
   pipeline: __eventlog_monitoring
   serverless: false

.. seealso::

   :doc:`monitoring` for details on the ``__eventlog_monitoring`` reserved keyword
   and monitoring pipeline configuration.

**Scenario 2: Schema Syntax Error**

**When it occurs:** A schema file has invalid syntax or structure.

**Common causes:**

- Incorrect column type names
- Missing required fields in column definitions
- Malformed schema YAML structure

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-011)

   name: customer_schema
   columns:
     - name: id
       type: BIGINTT              # Typo in type name
     - name: email
                                   # Missing: type field

.. code-block:: yaml
   :caption: After (fixed)

   name: customer_schema
   columns:
     - name: id
       type: BIGINT
     - name: email
       type: STRING

.. tip::

   Valid schema types include: ``STRING``, ``BIGINT``, ``INT``, ``INTEGER``,
   ``LONG``, ``DOUBLE``, ``FLOAT``, ``BOOLEAN``, ``DATE``, ``TIMESTAMP``,
   ``BINARY``, ``BYTE``, ``SHORT``, and ``DECIMAL(precision,scale)``.

LHP-VAL-012: Invalid Source Format
-----------------------------------

**When it occurs:** The ``source`` configuration for an action is not in the
expected format for its type.

**Common causes:**

- Providing a plain string where a configuration object is needed
- Missing the ``type`` field in the source configuration
- Using source configuration from one action type in another

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-012)

   actions:
     - name: load_api_data
       type: load
       source: custom_datasource   # String, but custom_datasource needs a dict

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_api_data
       type: load
       source:
         type: custom_datasource
         module_path: "data_sources/api_source.py"
         custom_datasource_class: "APIDataSource"

.. seealso::

   :doc:`actions/index` for the correct source configuration format for each
   action type.

LHP-VAL-048: B2 Validate batch_id taskValue Absent
---------------------------------------------------

**When it occurs:** The ``validate`` notebook generated for a ``for_each`` flowgroup
reads the ``batch_id`` taskValue from the ``prepare_manifest`` task and finds it
absent (the value returned by ``dbutils.jobs.taskValues.get`` is ``None``).

**Why it is an error:** The ``validate`` task identifies which manifest rows to
inspect by ``batch_id``. Without ``batch_id``, the task cannot construct the
validation SQL and cannot determine whether all iterations completed. Proceeding
would produce a misleading ``status: pass`` on an empty result set.

**Common causes:**

- The ``validate`` task ran before ``prepare_manifest`` completed, or
  ``prepare_manifest`` was skipped altogether (DAB dependency not wired correctly).
- ``prepare_manifest`` succeeded but the ``batch_id`` taskValue was not emitted
  — this can happen if an unhandled exception occurred between the
  ``dbutils.jobs.taskValues.set(key="batch_id", ...)`` call and task completion.
- LHP-MAN-005 caused ``prepare_manifest`` to fail before the taskValues set calls
  were reached, yet the DAB workflow still allowed the downstream ``validate`` task
  to start (check that the task dependency is ``depends_on: prepare_manifest``
  with ``outcome: succeeded``).
- A manual trigger of the ``validate`` task outside a full workflow run, without a
  preceding ``prepare_manifest`` execution in the same job run.

**Resolution:**

1. Confirm that the DAB workflow YAML lists ``prepare_manifest`` as a dependency of
   the ``validate`` task with ``outcome: succeeded``.
2. Check ``prepare_manifest`` logs for LHP-MAN-001, LHP-MAN-005, or any unhandled
   exception that prevented the taskValues set calls from executing.
3. Re-run the full workflow from the beginning — do not re-run ``validate`` in
   isolation unless you first confirm a valid ``batch_id`` is available for the
   current job run.

.. seealso::

   :ref:`LHP-MAN-005 <LHP-MAN-005: Manifest taskValue Payload Exceeds DAB 48 KB Ceiling>`
   for the prepare_manifest failure mode most likely to leave ``batch_id`` unset.

LHP-VAL-049: Parity Check Not Yet Implemented
----------------------------------------------

**When it occurs:** The ``validate`` notebook was generated with
``workflow.parity_check_enabled: true`` but the landed-parquet row-count source
has not shipped. Any attempt to run parity checking would silently pass every
batch because the comparison data source does not exist.

**Why it is an error:** Silently passing a parity check that cannot actually compare
row counts would give operators false confidence in data completeness. Raising
immediately makes the misconfiguration visible before it causes undetected data
quality issues.

**Common causes:**

- Setting ``workflow.parity_check_enabled: true`` in a flowgroup YAML before the
  U6-followup that wires the landing-row-count table or Delta path read has been
  deployed.
- Copying a flowgroup configuration from a future design document that includes
  parity checking as a planned feature.

**Resolution:**

Set ``workflow.parity_check_enabled: false`` (or remove the field, as ``false``
is the default) until the parity-check infrastructure is available. Monitor the
project issue tracker for the follow-up that implements the landed-parquet row
count source.

LHP-VAL-050: B2 Validate Failed
---------------------------------

**When it occurs:** The ``validate`` notebook for a ``for_each`` flowgroup finds
that ``completed_n != expected`` or ``failed_n > 0`` after querying the manifest
and watermark state for the current batch.

**Why it is an error:** One or more iterations in the batch did not complete
successfully. The bronze layer is in a partially-landed state for this batch —
some actions' data arrived, others did not. The downstream pipeline (silver
transforms, gold aggregations) may read an incomplete snapshot if allowed to
proceed.

.. note::

   This code was renamed from ``LHP-VAL-04A`` to ``LHP-VAL-050`` in the B2
   milestone (fix #17). Operators searching Databricks logs for the old code
   should use ``LHP-VAL-050`` in new alerts and runbooks.

**Common causes:**

- JDBC connection failure or timeout for one or more source tables during the
  extraction window.
- Source-side lock contention: a long-running transaction on the source database
  blocked the JDBC read for one action, causing its iteration to time out.
- A DAB retry budget exhaustion: the iteration failed on all ``max_retries``
  attempts and the manifest row remained in ``'failed'`` status.
- LHP-MAN-002 (ownership conflict) or LHP-MAN-004 (completion mirror exhaustion)
  left manifest rows in a non-``'completed'`` terminal state.

**Resolution:**

1. Inspect the ``validate`` summary JSON in the task output. The ``unfinished_actions``
   array lists every action that did not reach ``'completed'`` status, along with
   its ``final_status``.
2. For each failing action, open the corresponding ``for_each_ingest`` iteration in
   the Databricks Jobs UI and inspect the iteration log for the root cause.
3. If the cause is transient (JDBC timeout, cluster preemption), re-run the failed
   iterations using the ``Re-run failed iterations`` button in the DAB Jobs UI.
4. If the cause is a persistent LHP-MAN-002 or LHP-MAN-004 manifest state issue,
   follow the resolution steps in the corresponding error entry to reset the manifest
   rows, then re-run.

.. seealso::

   :ref:`LHP-MAN-002 <LHP-MAN-002: Manifest Claim Ownership Conflict>` and
   :ref:`LHP-MAN-004 <LHP-MAN-004: Completion Mirror MERGE Retry Budget Exhausted>`
   for manifest-side failures that surface as LHP-VAL-050.
   :ref:`LHP-VAL-048 <LHP-VAL-048: B2 Validate batch_id taskValue Absent>` for
   the case where ``validate`` cannot even read the batch_id.

I/O Errors (LHP-IO)
====================

I/O errors indicate problems reading or writing files referenced in your configuration.

LHP-IO-001: File Not Found
---------------------------

**When it occurs:** A file referenced in your configuration does not exist at the
expected path.

**Common causes:**

- Typo in the file path
- Relative path that resolves to the wrong location
- File was moved or deleted after the configuration was written
- Missing file extension

.. code-block:: yaml
   :caption: Before (triggers LHP-IO-001)

   actions:
     - name: transform_orders
       type: transform
       sub_type: sql
       source: v_raw_orders
       target: v_orders
       sql_file: sqls/transform_orders.sql   # File doesn't exist!

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: transform_orders
       type: transform
       sub_type: sql
       source: v_raw_orders
       target: v_orders
       sql_file: sql/transform_orders.sql    # Correct path

.. note::

   File paths are resolved relative to your flowgroup YAML file's directory.
   The error message shows the full resolved path and lists the locations that
   were searched.

LHP-IO-003: Invalid Document Count
-----------------------------------

**When it occurs:** A YAML file that is expected to contain a single document
has zero documents (empty file) or multiple documents separated by ``---``.

**Common causes:**

- An empty YAML file
- Using ``---`` document separators in a file that is loaded as single-document
- Copy-pasting content that includes extra ``---`` separators

.. code-block:: yaml
   :caption: Before (triggers LHP-IO-003) — extra separator in schema file

   name: customer_schema
   columns:
     - name: id
       type: BIGINT
   ---
   name: order_schema
   columns:
     - name: order_id
       type: BIGINT

.. code-block:: yaml
   :caption: After (fixed) — one schema per file

   # schemas/customer_schema.yaml
   name: customer_schema
   columns:
     - name: id
       type: BIGINT

.. tip::

   Multi-document YAML (``---`` separators) is supported for **flowgroup files**
   only. Schema files, expectations files, and substitution files must contain
   exactly one document. See :doc:`multi_flowgroup_guide` for multi-document
   flowgroup syntax.

Action Errors (LHP-ACT)
========================

Action errors indicate that an action type, subtype, or preset name is not recognized.

LHP-ACT-001: Unknown Type
--------------------------

**When it occurs:** A value you provided is not recognized. This covers unknown
action types, subtypes, sink types, source types, and preset names.

**Common causes:**

- Typo in the action type or subtype
- Using an action type that does not exist
- Referencing a preset that is not defined in the ``presets/`` directory

.. code-block:: yaml
   :caption: Before (triggers LHP-ACT-001) — unknown action type

   actions:
     - name: clean_data
       type: transfrm               # Typo!
       sub_type: sql
       source: v_raw

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: clean_data
       type: transform               # Correct spelling
       sub_type: sql
       source: v_raw

The error includes "Did you mean?" suggestions when the provided value is close
to a valid option. It also lists all valid values.

.. tip::

   Run ``lhp list_presets`` to see all available preset names, or check the
   :doc:`actions/index` for valid action types and subtypes.

Dependency Errors (LHP-DEP)
============================

Dependency errors indicate circular references in your pipeline's view graph
or preset inheritance chain.

LHP-DEP-001: Circular Dependency Detected
------------------------------------------

**When it occurs:** Two or more views form a dependency cycle where
view A depends on view B, which depends on view C, which depends back on view A.

**Common causes:**

- A transform action's source references a view that (directly or indirectly)
  depends on the transform's own target
- Copy-paste errors where source and target views were swapped
- Complex multi-step transformations that accidentally create loops

.. code-block:: yaml
   :caption: Before (triggers LHP-DEP-001) — circular dependency

   actions:
     - name: enrich_customers
       type: transform
       sub_type: sql
       source: v_enriched_orders     # Depends on enriched orders
       target: v_enriched_customers
       sql: |
         SELECT * FROM $source

     - name: enrich_orders
       type: transform
       sub_type: sql
       source: v_enriched_customers  # Depends on enriched customers!
       target: v_enriched_orders
       sql: |
         SELECT * FROM $source

.. code-block:: yaml
   :caption: After (fixed) — break the cycle

   actions:
     - name: enrich_customers
       type: transform
       sub_type: sql
       source: v_raw_customers       # Use raw source instead
       target: v_enriched_customers
       sql: |
         SELECT * FROM $source

     - name: enrich_orders
       type: transform
       sub_type: sql
       source: v_enriched_customers
       target: v_enriched_orders
       sql: |
         SELECT * FROM $source

The error message shows the full cycle path (e.g., ``A → B → C → A``) to help
you identify which dependency to remove or redirect.

.. tip::

   Run ``lhp deps --format dot --env <env>`` to generate a visual dependency
   graph that makes cycles easier to spot. See :doc:`dependency_analysis`
   for details.

General Troubleshooting
=======================

State Management
----------------

.. code-block:: bash
   :caption: Debugging state issues

   # Force regeneration of all files
   lhp generate --force-all --env dev

   # Clear state and regenerate everything
   rm .lhp_state.json
   lhp generate --env dev

   # Check what files would be regenerated
   lhp generate --dry-run --env dev --verbose

Dependency Debugging
--------------------

.. code-block:: bash
   :caption: Dependency debugging

   # Show dependency graph
   lhp validate --env dev --show-dependencies

   # Validate for circular dependencies
   lhp validate --env dev --check-cycles

Performance Optimization
------------------------

- Use **include patterns** to limit file scanning scope
- Keep **FlowGroups focused** — avoid overly large YAML files
- Leverage **state management** — don't force regeneration unless needed
- Use **specific targets** when possible instead of full pipeline generation

Getting Help
============

**Error not listed here?** This page documents the most common errors you will
encounter as a pipeline author. Some error codes are used internally for rare
edge cases and are not listed above.

If you encounter an error that is not documented here:

1. Read the error message carefully — every LHP error includes a description,
   context, and numbered fix suggestions directly in the terminal output
2. Run the command again with ``--verbose`` for additional diagnostic information
3. Run ``lhp validate --env <env>`` to check your full configuration
4. Report the issue at https://github.com/MehdiDataHandcraft/LakehousePlumber/issues
