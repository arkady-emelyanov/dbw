import json
import time
import datetime

from progress.spinner import Spinner
from rich import print_json
from dltx.service import Service
from dltx.notebook import Notebook
from dltx.job_cluster import JobCluster
from typing import Dict, AnyStr, Any, List


class BaseTask:
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        if not self.name:
            raise Exception("Missing name")
        self.notebook = kwargs.get("notebook")
        if not self.notebook:
            raise Exception(f"Missing 'notebook' parameter for task: '{self.name}'")
        self.params = kwargs

    def get_id(self, service: Service, global_params: Dict[AnyStr, Any]):
        raise Exception("Not supported")

    def get_real_name(self, service: Service, global_params: Dict[AnyStr, Any]):
        prefix = global_params.get("use_name_prefix", "")
        if prefix:
            return f"{prefix}-{self.name}"
        else:
            return self.name

    @staticmethod
    def purge(service: Service, global_params: Dict[AnyStr, Any], task_id):
        pass

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def task_json(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def pipeline_json(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def run_sync(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass


class PipelineTask(BaseTask):

    @staticmethod
    def purge(service: Service, global_params: Dict[AnyStr, Any], task_id):
        print("Deleting the DLT pipeline:", task_id)
        service.pipelines.delete(task_id)

    def get_id(self, service: Service, global_params: Dict[AnyStr, Any]):
        query_resp = service.api_client.perform_query(
            method="GET",
            path="/pipelines/",
            data={
                "filter": f"name like '{self.get_real_name(service, global_params)}'",
            }
        )
        if len(query_resp) == 0:
            return None

        if len(query_resp.get("statuses", [])) > 1:
            raise Exception("Duplicate names found")

        pipeline_id = query_resp["statuses"][0]["pipeline_id"]
        return pipeline_id

    def _get_update_status(
            self,
            pipeline_id,
            request_id,
            service: Service,
            global_params: Dict[AnyStr, Any]
    ) -> Dict[AnyStr, AnyStr]:
        # TODO: if active update exists, ask to cancel first,
        #  before triggering new one
        return service.api_client.perform_query(
            'GET',
            '/pipelines/{pipeline_id}/requests/{request_id}'.format(pipeline_id=pipeline_id, request_id=request_id),
        )

    def run_sync(self, service: Service, global_params: Dict[AnyStr, Any]):
        full_refresh = global_params.get("full_refresh", False)
        pipeline_id = self.get_id(service, global_params)

        resp = service.pipelines.start_update(pipeline_id, full_refresh=full_refresh)
        update_id = resp["update_id"]
        spinner = Spinner()
        last_check = 0
        last_state = None
        while True:
            if last_check % 30:
                s = self._get_update_status(pipeline_id, update_id, service, global_params)
                if s.get("latest_update"):
                    last_state = s.get("latest_update")["state"]
                spinner.message = f"DLT update phase: {last_state} "
                if s["status"] == "TERMINATED":
                    break
            last_check += 1
            time.sleep(0.1)
            spinner.next()
        spinner.finish()
        return last_state

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        notebook = Notebook(self.params["notebook"])
        notebook.synch(service, global_params)

        pipeline_id = self.get_id(service, global_params)
        data = self.pipeline_json(service, global_params)
        if not pipeline_id:
            print("Creating the DLT pipeline:", self.get_real_name(service, global_params))
            create_resp = service.pipeline_create(data)
            global_params["changes"].add_object("pipeline", create_resp["pipeline_id"])
        else:
            print("Updating the DLT pipeline:", self.get_real_name(service, global_params))
            service.pipeline_update(pipeline_id, data)

    def pipeline_json(self, service: Service, global_params: Dict[AnyStr, Any]):
        dlt_debug_mode = global_params.get("dlt_debug_mode")
        dlt_storage_root = global_params.get("dlt_storage_root")
        if not dlt_storage_root:
            raise Exception("dlt_storage_root is not set")

        notebook = Notebook(self.params["notebook"])
        remote_path = notebook.get_remote_path(service, global_params)
        pipeline_name = self.get_real_name(service, global_params)
        # TODO: cluster
        data = {
            "name": pipeline_name,
            "storage": f"{dlt_storage_root}/{pipeline_name}",
            "continuous": False,
            "photon": True,
            "target": "default",
            "clusters": [{
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2,
                    "mode": "LEGACY",
                },
            }],
            "libraries": [{
                "notebook": {
                    "path": remote_path,
                }
            }],
        }

        target = None
        min_workers = 1
        max_workers = 2
        policy_id = self.params.get("policy_id")

        if not policy_id:
            if global_params.get("dlt_policy_id"):
                policy_id = global_params.get("dlt_policy_id")

        if self.params.get("min_workers"):
            min_workers = self.params.get("min_workers")
            del self.params["min_workers"]

        if self.params.get("max_workers"):
            max_workers = self.params.get("max_workers")
            del self.params["max_workers"]

        if self.params.get("target"):
            target = self.params.get("target")
            del self.params["target"]

        data["configuration"] = {}
        data["configuration"].update({
            "dbw.use_name_suffix": global_params.get("use_name_suffix", ""),
            "dbw.resource_storage_root": global_params.get("resource_storage_root", ""),
            "dbw.library_storage_root": global_params.get("library_storage_root", ""),
            "dbw.files_storage_root": global_params.get("files_storage_root", ""),
        })

        if self.params.get("spark_conf"):
            v = self.params.get("spark_conf")
            data["configuration"].update(v)

        if dlt_debug_mode:
            data["development"] = True
            data["configuration"].update({
                "pipelines.clusterShutdown.delay": "60m",
            })

        if target:
            data["target"] = target

        if policy_id:
            data["clusters"][0]["policy_id"] = policy_id

        data["clusters"][0]["autoscale"]["min_workers"] = min_workers
        data["clusters"][0]["autoscale"]["max_workers"] = max_workers
        return data

    def task_json(self, service: Service, global_params: Dict[AnyStr, Any]):
        data = {
            "task_key": self.name,
            "pipeline_task": {
                "pipeline_id": self.get_id(service, global_params)
            }
        }
        depends_on = self.params.get("depends_on", None)
        if depends_on:
            data["depends_on"] = depends_on
        return data


class NotebookTask(BaseTask):

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        notebook = Notebook(self.params["notebook"])
        notebook.synch(service, global_params)

    def run_sync(self, service: Service, global_params: Dict[AnyStr, Any]):
        ts = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        use_cluster_id = global_params.get("use_cluster_id")

        if not use_cluster_id:
            job_clusters: Dict[AnyStr, JobCluster] = global_params.get("job_clusters")
            if not job_clusters or not job_clusters.get("default"):
                raise Exception("'default' cluster should be in the list of clusters")

            k = "new_cluster"
            v = job_clusters["default"].json(
                service,
                global_params,
                new_cluster_only=True
            )
        else:
            k = "existing_cluster_id"
            v = use_cluster_id

        data = {
            "run_name": f"{self.get_real_name(service, global_params)}-{ts}",
            f"{k}": v,
            "notebook_task": self.task_json(
                service,
                global_params,
                notebook_task_only=True,
            ),
        }
        if global_params.get("render_json_and_exit"):
            return print_json(json.dumps(data))

        # TODO: refactor
        run_create_resp = service.jobs.submit_run(**data)
        spinner = Spinner()
        last_check = 0
        run_url_published = False
        while True:
            if last_check % 30 == 0:
                run_status_resp = service.jobs.get_run(run_id=run_create_resp["run_id"])
                run_state = run_status_resp["state"]
                if not run_url_published and run_status_resp["run_page_url"]:
                    run_url_published = True
                    print("Run URL:", run_status_resp["run_page_url"])

                last_check = 0
                if run_state["life_cycle_state"] == "INTERNAL_ERROR":
                    spinner.message = 'Run failed '

                if run_state["life_cycle_state"] == "PENDING":
                    spinner.message = 'Waiting workflow run to start: '

                if run_state["life_cycle_state"] == "RUNNING":
                    spinner.message = 'Waiting workflow run to finish: '

                if run_status_resp.get("end_time", 0) > 0:
                    spinner.finish()
                    break

            time.sleep(0.1)
            last_check += 1
            spinner.next()

    def task_json(self, service: Service, global_params: Dict[AnyStr, Any], **kwargs):
        notebook = Notebook(self.params["notebook"])
        remote_path = notebook.json(service, global_params)
        notebook_task = {
            "notebook_path": remote_path,
            "base_parameters": {  # make it variable
                "dbw.use_name_suffix": global_params.get("use_name_suffix", ""),
                "dbw.resource_storage_root": global_params.get("resource_storage_root", ""),
                "dbw.library_storage_root": global_params.get("library_storage_root", ""),
                "dbw.files_storage_root": global_params.get("files_storage_root", ""),
            }
        }
        spark_conf = self.params.get("spark_conf")
        if spark_conf:
            notebook_task["base_parameters"].update(spark_conf)
        if kwargs.get("notebook_task_only"):
            return notebook_task

        data = {
            "task_key": self.name,
            "source": "WORKSPACE",
            "notebook_task": notebook_task,
        }
        use_cluster_id = global_params.get("use_cluster_id")
        disable_use_cluster_id = kwargs.get("disable_use_cluster")
        if use_cluster_id and not disable_use_cluster_id:
            data["existing_cluster_id"] = use_cluster_id
        else:
            data["job_cluster_key"] = self.params.get("job_cluster_key")

        depends_on = self.params.get("depends_on")
        disable_depends_on = kwargs.get("disable_depends_on")
        if depends_on and not disable_depends_on:
            data["depends_on"] = depends_on
        return data
