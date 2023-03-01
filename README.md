# Dbw

Project aims to simplify Databricks Workflow development.
To learn more about Databricks Workflows
[follow this link](https://www.databricks.com/blog/2022/05/10/introducing-databricks-workflows.html).

## Concept

Dbw project contains three type of objects: resources, 
tasks, and workflows.

*Resource*. Arbitrary file, usually some piece of
configuration. Resources are located in the [resources folder](./resources).
(not currently implemented)

*Task*. Notebook and DLT tasks are currently supported. 
Tasks located in the [tasks folder](./tasks). Tasks can
be shared across multiple workflows.

*Workflow*. The DAG.

## Configuration

Create `.env` file in the root of the project (see below).

Example environment file:
```
##
## parameters required by framework
##
DATABRICKS_HOST="https://<workspace id>.cloud.databricks.com"
DATABRICKS_TOKEN="<your token here>"

DBW_WORKSPACE_ROOT="/Users/somebody@example.com"
DBW_USE_NAME_SUFFIX="<random alpha-numeric string>"
DBW_DLT_STORAGE_ROOT="dbfs:/tmp/${DBW_USE_NAME_SUFFIX}/dlt"
DBW_RESOURCE_STORAGE_ROOT="dbfs:/tmp/${DBW_USE_NAME_SUFFIX}/res"
DBW_LIBRARY_STORAGE_ROOT="dbfs:/tmp/${DBW_USE_NAME_SUFFIX}/lib"

DBW_DLT_DEBUG_MODE="1"
DBW_USE_CLUSTER_ID="0223-182605-8waludat"

DBW_DEFAULT_SPARK_VERSION="12.1.x-scala2.12"
DBW_DEFAULT_NODE_TYPE_ID="m5.large"
DBW_DEFAULT_DRIVER_NODE_TYPE_ID="m5.large"

#DBW_DLT_POLICY_ID="E06216CAA0000E7A"
#DBW_POLICY_ID="E06216CAA0000D98"

##
## any custom project-specific parameters below
##
# will be accessible in tasks via get_param("dbw.hello")
#DBW_HELLO="world"
```

* Property `DBW_USE_NAME_SUFFIX` is required to provide
uniqueness to the names of Delta Live Table pipelines (and should be generated for each developer).
* Property `DBW_DLT_DEBUG_MODE` enables DLT pipeline cluster to keep running after pipeline finished.
* Property `DBW_USE_CLUSTER_ID` enables Notebook type tasks to be scheduled on the shared cluster
instead of Run scoped.
* Property `DBW_DLT_POLICY_ID` global default policy to use for DLT pipeline clusters.
* Property `DBW_POLICY_ID` global default policy to use for Job scoped clusters.

## Command-line interface

Developer terminal.

Commands:
* info - Get current configuration
* render - Renders workflow JSON representation
* run - Runs the Workflow
* run-task - Runs the task of the workflow (DLT only supported)
* diff (WIP) - Compares local and remote workflow definitions
* synch - Installs the project to the Databricks workspace
* delete - Removes the project from Databricks workspace

To deploy the example workflow provided with this repository use following commands:
* `make run` - will create an example workflow (or update existing) and run it
* `make run-task` - will create an example workflow (or update existing) and run individual workflow task


## Library interface

Pyspark helpers.

Sample DLT task definition:
```python
# ...
with Workflow() as w:
    w.dlt_task(
        name="dlt_sample_task",
        spark_conf={
            "hello": "world",
        },
    )
```

Sample DLT task code:
```python
#...
from dltx.utils import table_name, get_param

# use table_name() helper to generate final DLT name
# using naming convention.
@dlt.table(name=table_name("dlt_experiment_one"))
def dlt_experiment_one():
    # use get_param() helper to get global or 
    # task level defined parameter (see example tasks)
    greeting = get_param("hello") # will have "world" value
    return spark.createDataFrame([])

```

`table_name` formats target table name according to convention.


## ROADMAP

* [ ] Dbt task support (dbtCloud and dbtCore)
* [ ] Refactor the code
* [ ] Add tests
* [ ] Reconciliation: add state backend (file, s3)

## Is it stable?

No. It's just a concept.
