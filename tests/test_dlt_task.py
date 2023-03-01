import unittest

from dltx.task import DltTask
from dltx.service import MockService


class TestDltTask(unittest.TestCase):
    """Task tests"""

    def test_task_json(self):
        task = DltTask(name="mydlttask")
        service_mock = MockService()
        service_mock.api_client.set_perform_query_result({
            "statuses": [{
                "pipeline_id": "100500"
            }]
        })
        r = task.task_json(service_mock, {})
        self.assertEqual(r, {
            "task_key": "mydlttask",
            "pipeline_task": {
                "pipeline_id": "100500",
            }
        })

    def test_pipeline_json(self):
        task = DltTask(name="mydlttask")
        service_mock = MockService()
        service_mock.api_client.set_perform_query_result({
            "statuses": [{
                "pipeline_id": "100500"
            }]
        })
        r = task.pipeline_json(service_mock, {
            "dlt_storage_root": "/tmp",
            "workspace_root": "/root"
        })
        self.assertEqual(r, {
            "name": "mydlttask",
            "storage": "/tmp/mydlttask",
            "continuous": False,
            "photon": True,
            "target": "default",
            "clusters": [{
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2
                },
            }],
            "libraries": [{
                "notebook": {
                    "path": "/root/mydlttask"
                }
            }],
            "configuration": {
                "dbw.use_name_suffix": "",
                "dbw.resource_storage_root": "",
                "dbw.library_storage_root": "",
            }
        })


if __name__ == "__main__":
    unittest.main()
