import time
import uuid
import functools
import json

from dltx.task import BaseTask, NotebookTask
from dltx.job_cluster import JobCluster
from dltx.changes import Changes
from dltx.service import Service
from dltx.library import Library
from dltx.notebook import Notebook
from dltx.task import PipelineTask
from dltx.file_resource import FileResource

from rich import print_json
from typing import Dict, AnyStr, Any


def inject_workflow_params(method):
    @functools.wraps(method)
    def wrapper(self, service: Service, global_params: Dict[AnyStr, Any]):
        local_params = self.inject_workflow_level_params(service, global_params)
        result = method(self, service, local_params)
        return result

    return wrapper


class Workflow:
    JOBS_API_VERSION = '2.1'

    def __init__(self, **kwargs):
        name = kwargs.get("name", "")
        if name == "":
            raise Exception("Missing name")

        self.name = name
        self.params = kwargs
        self.tasks: Dict[AnyStr, BaseTask] = {}
        self.job_clusters: Dict[AnyStr, JobCluster] = {}
        self.resources: Dict[AnyStr, FileResource] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self

    def job_cluster(self, **kwargs):
        t = JobCluster(**kwargs)
        self.job_clusters[t.name] = t

    def pipeline_task(self, **kwargs):
        t = PipelineTask(**kwargs)
        self.tasks[t.name] = t

    def notebook_task(self, **kwargs):
        t = NotebookTask(**kwargs)
        self.tasks[t.name] = t

    def add_files(self, list_of_files):
        for name in list_of_files:
            self.file(name=name)

    def file(self, **kwargs):
        t = FileResource(**kwargs)
        self.resources[t.name] = t

    def get_real_name(self, service: Service, global_params: Dict[AnyStr, Any]):
        suffix = global_params.get("use_name_suffix")
        if not suffix:
            suffix = uuid.uuid4().hex[:8]
        return f"{self.name}-{suffix}"

    def get_id(self, service: Service, global_params: Dict[AnyStr, Any]):
        limit = 25
        offset = 0
        all_wfls = []
        while True:
            time.sleep(0.1)
            # TODO: filter by "owned by me"?
            resp = service.jobs.list_jobs(
                version=self.JOBS_API_VERSION,
                limit=limit,
                offset=offset,
            )
            offset += limit
            all_wfls.extend(resp["jobs"])
            if not resp["has_more"]:
                break

        found_wfls = [x for x in all_wfls if x["settings"]["name"] == self.get_real_name(service, global_params)]
        if len(found_wfls) > 1:
            raise Exception(f"More than one workflow with same name: '{self.name}'")
        if not found_wfls:
            return None

        return found_wfls[0]["job_id"]

    def inject_workflow_level_params(self, service: Service, global_params: Dict[AnyStr, Any]):
        workflow_params = {}
        local_params = {
            "workflow_name": self.name,
            "use_name_prefix": self.get_real_name(service, global_params),
            "tasks": self.tasks,
            "job_clusters": self.job_clusters,
            "changes": Changes(self.name, global_params)
        }
        workflow_params.update(global_params)
        workflow_params.update(local_params)
        return workflow_params

    @inject_workflow_params
    def delete(self, service: Service, global_params: Dict[AnyStr, Any]):
        state_map = global_params["changes"].get_objects()
        delete_mappings = {
            "library": Library,
            "notebook": Notebook,
            "pipeline": PipelineTask,
            "workflow": Workflow,
            "file_resource": FileResource,
        }
        for k in state_map:
            f = delete_mappings.get(k["obj_type"])
            if not f:
                print(f"Skipping {k}")
            f.purge(service, global_params, k["obj_id"])
        global_params["changes"].reset()

    @inject_workflow_params
    def run_sync(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def get_task(self, task_name):
        for t in self.tasks:
            if self.tasks[t].name == task_name:
                return self.tasks[t]
        raise Exception(f"Unknown task '{task_name}' in workflow '{self.name}'")

    @inject_workflow_params
    def diff(self, service: Service, global_params: Dict[AnyStr, Any]):
        local_json = self.json(service, global_params)
        print(local_json)

    @inject_workflow_params
    def render(self, service: Service, global_params: Dict[AnyStr, Any]):
        data = self.json(service, global_params)
        print("Workflow:", self.get_real_name(service, global_params))
        print_json(json.dumps(data, indent=2))
        for k in self.tasks:
            t = self.tasks[k]
            task_data = t.pipeline_json(service, global_params)
            if task_data:
                print("Task:", t.get_real_name(service, global_params))
                print_json(json.dumps(task_data, indent=2))

    @staticmethod
    def purge(service: Service, global_params: Dict[AnyStr, Any], workflow_id):
        print(f"Deleting the workflow:", workflow_id)
        service.jobs.delete_job(workflow_id)

    @inject_workflow_params
    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        workflow_id = self.get_id(service, global_params)
        print(f"Synchronizing the workflow: {self.get_real_name(service, global_params)}")
        for x in self.tasks:
            self.tasks[x].synch(service, global_params)

        for x in self.job_clusters:
            self.job_clusters[x].synch(service, global_params)

        for x in self.resources:
            self.resources[x].synch(service, global_params)

        data = self.json(service, global_params)
        if not workflow_id:
            create_resp = service.jobs.create_job(**data)
            global_params["changes"].add_object("workflow", create_resp["job_id"])
        else:
            service.jobs.reset_job(workflow_id, new_settings=data)
        print("Synchronization complete.")

    @inject_workflow_params
    def json(self, service: Service, global_params: Dict[AnyStr, Any]):
        return {
            "name": self.get_real_name(service, global_params),
            "max_concurrent_runs": self.params.get("max_concurrent_runs", 1),
            "format": "MULTI_TASK",
            "tasks": [self.tasks[x].task_json(service, global_params) for x in self.tasks],
            "job_clusters": [self.job_clusters[x].json(service, global_params) for x in self.job_clusters],
        }
