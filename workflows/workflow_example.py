from dltx.workflow import Workflow

with Workflow(
        name="workflow_example",
        schedule="@daily"
) as w:
    # register the job cluster.
    # required section for notebook run if shared cluster id wasn't provided.
    w.job_cluster(
        name="default",

        # cluster default configuration options:
        # policy_id=Variable.get("policy_id"),
        # spark_version="12.1.x-scala2.12",
        # node_type_id="m5d.large",
        # min_workers=1,
        # max_workers=3,
    )

    # create DLT task.
    # task name represent name of the file in ./tasks/ directory
    w.dlt_task(
        name="dlt_sample_task",
        spark_conf={
            "hello": "world",
        },
        # DLT task default configuration options:
        # target="default",
        #
        # Cluster default configuration options:
        # spark_conf={}
        # min_workers=1,
        # max_workers=3,
    )

    # create a Notebook task.
    # Task name represent name of the file in ./tasks/ directory
    w.nb_task(
        name="nb_sample_task",
        depends_on=[{"task_key": "dlt_sample_task"}],
        job_cluster_key="default",
        spark_conf={
            "hello": "world",
        },

        # Cluster default configuration options:
        # spark_conf={}
        # min_workers=1,
        # max_workers=3,
    )
