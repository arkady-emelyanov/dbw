from dltx.service import Service
from typing import Dict, AnyStr, Any


class BaseCluster:
    def __init__(self, **kwargs):
        name = kwargs.get("name")
        if not name:
            raise Exception("Missing name")

        self.name = name
        self.params = kwargs

    def delete(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def synch(self, service: Service, global_params: Dict[AnyStr, Any]):
        pass

    def json(self, service: Service, global_params: Dict[AnyStr, Any]):
        spark_version = global_params.get("default_spark_version")
        node_type_id = global_params.get("default_node_type_id")
        driver_node_type_id = global_params.get("default_driver_node_type_id")
        min_workers = 1
        max_workers = 2

        # policy may override default values
        policy_id = self.params.get("policy_id")
        if not policy_id:
            policy_id = global_params.get("policy_id")

        if self.params.get("spark_version"):
            spark_version = self.params.get("spark_version")
            del self.params["spark_version"]

        if self.params.get("node_type_id"):
            node_type_id = self.params.get("node_type_id")
            del self.params["node_type_id"]

        if self.params.get("driver_node_type_id"):
            driver_node_type_id = self.params.get("driver_node_type_id")
            del self.params["driver_node_type_id"]

        if self.params.get("min_workers"):
            min_workers = self.params.get("min_workers")
            del self.params["min_workers"]

        if self.params.get("max_workers"):
            max_workers = self.params.get("max_workers")
            del self.params["max_workers"]

        if spark_version:
            self.params["spark_version"] = spark_version
        if node_type_id:
            self.params["node_type_id"] = node_type_id
        if driver_node_type_id:
            self.params["driver_node_type_id"] = driver_node_type_id
        if policy_id:
            self.params["policy_id"] = policy_id

        self.params["autoscale"] = {
            "min_workers": min_workers,
            "max_workers": max_workers,
        }

        return {
            "job_cluster_key": self.name,
            "new_cluster": self.params,
        }


class JobCluster(BaseCluster):
    pass
