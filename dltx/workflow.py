import time
import uuid

from dltx.task import BaseTask, DltTask, NbTask
from dltx.job_cluster import JobCluster
from dltx.service import Service
from typing import Dict, AnyStr, Any


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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self

    def job_cluster(self, **kwargs):
        t = JobCluster(**kwargs)
        self.job_clusters[t.name] = t

    def dlt_task(self, **kwargs):
        t = DltTask(**kwargs)
        self.tasks[t.name] = t

    def nb_task(self, **kwargs):
        t = NbTask(**kwargs)
        self.tasks[t.name] = t

    def get_real_name(self, service: Service, global_params: Dict[AnyStr, Any]):
        suffix = global_params.get("use_name_suffix")
        if not suffix:
            suffix = uuid.uuid4().hex[:8]
        return f"{self.name}-{suffix}"

    def get_id_by_name(self, service: Service, global_params: Dict[AnyStr, Any]):
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

    def get_extra_params(self, service: Service, global_params: Dict[AnyStr, Any]):
        extra_params = {
            "use_name_prefix": self.get_real_name(service, global_params),
            "tasks": self.tasks,
            "job_clusters": self.job_clusters,
        }
        extra_params.update(global_params)
        return extra_params

    def run_task_sync(self, task_name, service: Service, global_params: Dict[AnyStr, Any]):
        extra_params = self.get_extra_params(service, global_params)
        for t in self.tasks:
            if self.tasks[t].name == task_name:
                return self.tasks[t].run_sync(service, extra_params)
        raise Exception(f"Unknown task '{task_name}' in workflow '{self.get_real_name(service, global_params)}'")

    def diff(self, service: Service, global_params: Dict[AnyStr, Any]):
        extra_params = self.get_extra_params(service, global_params)
        local_json = self.json(service, extra_params)
        print(local_json)

    def delete(self, service: Service, global_params: Dict[AnyStr, Any]):
        workflow_id = self.get_id_by_name(service, global_params)
        extra_params = self.get_extra_params(service, global_params)
        print(f"Start deletion of the workflow: {self.get_real_name(service, extra_params)}")
        for x in self.tasks:
            t = self.tasks[x]
            t.delete(service, extra_params)

        for x in self.job_clusters:
            c = self.job_clusters[x]
            c.delete(service, extra_params)

        if workflow_id:
            print(f"Deleting the workflow: {self.get_real_name(service, extra_params)}")
            service.jobs.delete_job(workflow_id)
        print("Deletion complete!")

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        workflow_id = self.get_id_by_name(service, global_params)
        extra_params = self.get_extra_params(service, global_params)
        print(f"Synchronizing the workflow: {self.get_real_name(service, extra_params)}")
        for x in self.tasks:
            self.tasks[x].synch(service, extra_params)

        for x in self.job_clusters:
            self.job_clusters[x].synch(service, extra_params)

        data = self.json(service, extra_params)
        if not workflow_id:
            service.jobs.create_job(**data)
        else:
            service.jobs.reset_job(workflow_id, new_settings=data)
        print("Synchronization complete.")

    def json(self, service: Service, global_params: Dict[AnyStr, Any]):
        extra_params = self.get_extra_params(service, global_params)
        return {
            "name": self.get_real_name(service, extra_params),
            "max_concurrent_runs": 1,
            "format": "MULTI_TASK",
            "tasks": [self.tasks[x].task_json(service, extra_params) for x in self.tasks],
            "job_clusters": [self.job_clusters[x].json(service, extra_params) for x in self.job_clusters],
        }
