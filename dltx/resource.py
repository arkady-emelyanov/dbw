import os
from base64 import b64encode
from dltx.service import Service
from typing import Dict, AnyStr, Any


class Resource:
    def __init__(self, project_full_name):
        self.project_full_name = project_full_name

    def get_remote_path(self, service: Service, global_params: Dict[AnyStr, Any]):
        storage_root = global_params.get("resource_storage_root")
        remote_path = f"{storage_root}/{self.project_full_name}.tar.gz"
        return remote_path

    def load_workflows(self, workflow_root):
        file_list = os.listdir(workflow_root)
        path_list = [os.path.join(workflow_root, x) for x in file_list]
        for p in path_list:
            if os.path.isdir(p):
                continue

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass
        # print("Uploading the library:", "dist/dltx-0.1.tar.gz", remote_path)
        # with open(local_path, "rb") as f:
        #     content = b64encode(f.read()).decode()
        #     service.dbfs.put(remote_path, contents=content, overwrite=True)
