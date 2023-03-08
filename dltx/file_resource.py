import os.path

from base64 import b64encode
from dltx.service import Service
from typing import Dict, AnyStr, Any


class FileResource:

    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        if not self.name:
            raise Exception("Name is missing")
        self.params = kwargs

    @staticmethod
    def purge(service: Service, global_params: Dict[AnyStr, Any], remote_path):
        print(f"Deleting the file:", remote_path)
        service.dbfs.delete(remote_path)

    def json(self, service: Service, global_params: Dict[AnyStr, Any]):
        return self.get_remote_path(service, global_params)

    def get_remote_path(self, service: Service, global_params: Dict[AnyStr, Any]):
        storage_root = global_params.get("files_storage_root")
        if not storage_root:
            raise Exception("'files_storage_root' is not defined!")

        remote_path = f"{storage_root}/{self.name}"
        return remote_path

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        source_file = f"./files/{self.name}"
        if not os.path.exists(source_file):
            raise Exception(f"FileResource doesn't exists: {source_file}")

        remote_path = self.get_remote_path(service, global_params)
        path_exists = service.dbfs_path_exists(remote_path)

        print("Uploading the file:", source_file, remote_path)
        with open(source_file, "rb") as f:
            content = b64encode(f.read()).decode()
            service.dbfs.put(remote_path, contents=content, overwrite=True)

        if not path_exists:
            global_params["changes"].add_object("file_resource", remote_path)
