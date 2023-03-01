import os
from base64 import b64encode
from dltx.service import Service
from typing import Dict, AnyStr, Any


class Library:
    def __init__(self, project_full_name):
        self.project_full_name = project_full_name

    def get_remote_path(self, service: Service, global_params: Dict[AnyStr, Any]):
        storage_root = global_params.get("library_storage_root")
        if not storage_root:
            raise Exception("'library_storage_root' is not defined!")

        remote_path = f"{storage_root}/{self.project_full_name}.tar.gz"
        return remote_path

    def get_install_path(self, service: Service, global_params: Dict[AnyStr, Any]):
        remote_path = self.get_remote_path(service, global_params)
        return remote_path.replace("dbfs:", "/dbfs")

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        local_path = "dist/dltx-0.1.tar.gz"
        remote_path = self.get_remote_path(service, global_params)

        if not os.path.exists(local_path):  # FIXME: fix later
            raise Exception("No library archive found")

        print("Uploading the library:", "dist/dltx-0.1.tar.gz", remote_path)
        with open(local_path, "rb") as f:
            content = b64encode(f.read()).decode()
            service.dbfs.put(remote_path, contents=content, overwrite=True)
