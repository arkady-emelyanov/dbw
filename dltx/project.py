import re
import os
import importlib
import importlib.machinery
import importlib.util
import importlib.resources
import json
import sys
import time
from rich import print_json
from typing import List

from progress.spinner import Spinner

from dltx.state import State
from dltx.workflow import Workflow
from dltx.service import Service
from dltx.library import Library
from dltx.notebook import Notebook
from dltx.task import PipelineTask
from dltx.file_resource import FileResource


class Project:

    def __init__(self):
        self.workflows: List[Workflow] = []
        self.service = Service()
        self.params = {}

    def configure(self, params):
        self.params.update(params)
        for n, v in os.environ.items():
            if n.startswith("DBW_"):
                k = re.sub('^DBW_', '', n).lower()
                if not self.params.get(k):
                    self.params[k] = v

        if not self.params.get("workspace_root"):
            raise Exception("Workspace root is not set")
        return self

    def load_workflows(self, workflow_root):
        file_list = os.listdir(workflow_root)
        path_list = [os.path.join(workflow_root, x) for x in file_list]
        for p in path_list:
            if p.endswith(".py"):
                self._load_file(p)

    def _load_file(self, file):
        print("Importing the workflow file:", file)
        mod_name, _ = os.path.splitext(os.path.split(file)[-1])
        if mod_name in sys.modules:
            del sys.modules[mod_name]

        loader = importlib.machinery.SourceFileLoader(mod_name, file)
        spec = importlib.util.spec_from_loader(mod_name, loader)
        new_module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = new_module
        loader.exec_module(new_module)

        for m in new_module.__dict__.values():
            if isinstance(m, Workflow):
                self.workflows.append(m)

    def _find_workflow(self, workflow_name) -> Workflow:
        for x in self.workflows:
            if x.name == workflow_name:
                return x
        raise Exception(f"Unknown workflow: '{workflow_name}'")

    def info(self):
        print("Parameters:")
        print_json(json.dumps(self.params, indent=2))

    def delete_workflow(self, workflow_name):
        state = State(workflow_name, self.params)
        self._delete_using_state(state)

    def render_workflow(self, workflow_name):
        workflow = self._find_workflow(workflow_name)
        workflow.render(self.service, self.params)

    def synch_workflow(self, workflow_name):
        # TODO: application should be loaded
        library = Library(f"app-{self.params.get('use_name_suffix')}")
        library.synch(self.service, self.params)
        self.params.update({
            "library_install_path": library.get_install_path(self.service, self.params),
        })
        workflow = self._find_workflow(workflow_name)
        workflow.synch(self.service, self.params)
        self._synch_state(workflow_name)

    def diff_workflow(self, workflow_name):
        workflow = self._find_workflow(workflow_name)
        workflow.diff(self.service, self.params)

    def run_workflow_sync(self, workflow_name, token):
        workflow = self._find_workflow(workflow_name)
        # workflow.run_sync(self.service, self.params)

        workflow_id = workflow.get_id(self.service, self.params)
        if not workflow_id:
            raise Exception(f"Workflow with name={workflow_name} was not found")

        print(f"Creating a new workflow run...")
        run_create_resp = self.service.jobs.run_now(workflow_id, idempotency_token=token)
        spinner = Spinner()
        last_check = 0
        run_url_published = False
        while True:
            if last_check % 30 == 0:
                run_status_resp = self.service.jobs.get_run(run_id=run_create_resp["run_id"])
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

        print("Finished!")
        print("")
        print("Stats:")
        print("- Job ID:", run_status_resp["job_id"])
        print("- Run ID:", run_status_resp["run_id"])
        print("- Run duration:", run_status_resp["run_duration"] // 1000, "seconds")
        print("")
        print("Tasks:")
        if run_status_resp["format"] == "SINGLE_TASK":
            t = run_status_resp
            print("-- Task lifecycle state:", t["state"]["life_cycle_state"])
            if "result_state" in t["state"] and t["state"]["result_state"]:
                print("-- Task result state:", t["state"]["result_state"])

            if "state_message" in t["state"] and t["state"]["state_message"]:
                print("-- State message:", t["state"]["state_message"])

        else:
            for t in run_status_resp["tasks"]:
                print("-- Task key:", t["task_key"])
                print("-- Task lifecycle state:", t["state"]["life_cycle_state"])
                if "result_state" in t["state"] and t["state"]["result_state"]:
                    print("-- Task result state:", t["state"]["result_state"])

                if "state_message" in t["state"] and t["state"]["state_message"]:
                    print("-- State message:", t["state"]["state_message"])
                print("")

    def run_task_sync(self, workflow_name, task_name):
        workflow = self._find_workflow(workflow_name)
        task = workflow.get_task(task_name)

        task_params = workflow.inject_workflow_level_params(self.service, self.params)
        task.run_sync(self.service, task_params)

    def _synch_state(self, workflow_name):
        state = State(workflow_name, self.params)
        change_event_list = self.service.changes.finalize()
        state.update_state(change_event_list)

    def _delete_using_state(self, state: State):
        state_map = state.get_state_map()
        delete_mappings = {
            "library": Library,
            "notebook": Notebook,
            "pipeline": PipelineTask,
            "workflow": Workflow,
            "file_resource": FileResource,
        }
        for k in state_map:
            f = delete_mappings.get(k)
            if not f:
                print(f"Skipping {k}")
            for object_id in state_map[k]:
                f.purge(self.service, self.params, object_id)
        state.reset_state()
