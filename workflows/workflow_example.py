from dltx.workflow import Workflow

with Workflow(
        name="workflow_example",
        schedule="@daily",
        # max_concurrent_runs=5,
) as w:
    # add resources accessible in spark session
    w.add_files([
        "myconf.json",
    ])

    # register the job cluster.
    # required section for notebook run if shared cluster id wasn't provided.
    w.job_cluster(
        name="default",
        # policy_id="xxxx",
        # spark_version="12.1.x-scala2.12",
        # node_type_id="m5d.large",
        # min_workers=1,
        # max_workers=3,
    )

    # create DLT task.
    # task name represent name of the file in ./tasks/ directory
    w.pipeline_task(
        name="dlt_task",
        notebook="dlt_notebook.py",
        spark_conf={
            "hello": "world",
        },
        # target="default",
        # min_workers=1,
        # max_workers=3,
    )

    # create a Notebook task.
    # Task name represent name of the file in ./tasks/ directory
    w.notebook_task(
        name="notebook_task",
        notebook="submit_notebook",
        depends_on=[{"task_key": "dlt_task"}],
        job_cluster_key="default",
        spark_conf={
            "hello": "world",
        },
        # min_workers=1,
        # max_workers=3,
    )
