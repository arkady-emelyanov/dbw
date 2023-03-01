import unittest

from dltx.task import NbTask
from dltx.service import MockService


class TestNbTask(unittest.TestCase):
    """Task tests"""

    def test_task_json(self):
        service_mock = MockService()

        task = NbTask(name="mynbtask")
        r = task.task_json(service_mock, {
            "workspace_root": "/home/user",
        })
        self.assertEqual(r, {
            'task_key': 'mynbtask',
            'source': 'WORKSPACE',
            'job_cluster_key': None,
            'notebook_task': {
                'notebook_path': '/home/user/mynbtask',
                'base_parameters': {
                    'dbw.use_name_suffix': '',
                    'dbw.resource_storage_root': '',
                    'dbw.library_storage_root': ''
                }
            },
        })


if __name__ == "__main__":
    unittest.main()
