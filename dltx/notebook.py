from base64 import b64encode
from dltx.service import Service
from typing import Dict, AnyStr, Any


class Notebook:

    def __init__(self, task_name, task_real_name):
        self.task_name = task_name
        self.task_real_name = task_real_name

    @staticmethod
    def purge(service: Service, global_params: Dict[AnyStr, Any], remote_path):
        print("Deleting the workspace notebook:", remote_path)
        service.workspace.delete(remote_path)

    def json(self, service: Service, global_params: Dict[AnyStr, Any]):
        return self.get_remote_path(service, global_params)

    def get_remote_path(self, service: Service, global_params: Dict[AnyStr, Any]):
        workspace_root = global_params.get("workspace_root")
        return f"{workspace_root}/{self.task_real_name}"

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        source_file = f"./tasks/{self.task_name}.py"
        remote_path = self.get_remote_path(service, global_params)
        path_exists = service.workspace_path_exists(remote_path)

        with open(source_file, "rb") as f:
            notebook_header = "\n".join([
                "# Databricks notebook source",
                f"# MAGIC %pip install {global_params.get('library_install_path')}",
                "",
                "# COMMAND ----------",
                "",
            ])
            notebook_body = f.read().decode()
            notebook_footer = "\n".join([
                "",
                "# COMMAND ----------",
                "",
            ])
            notebook = "\n".join([
                notebook_header,
                notebook_body,
                notebook_footer,
            ])
            content = b64encode(bytes(notebook, 'utf-8')).decode()
            print("Importing the workspace notebook:", remote_path)
            service.workspace.import_workspace(
                remote_path,
                format="SOURCE",
                language="PYTHON",
                content=content,
                overwrite=True,
            )

        if not path_exists:
            service.changes.create("notebook", remote_path)
        return remote_path
