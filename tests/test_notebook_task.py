import unittest

from dltx.task import NotebookTask
from dltx.service import MockService


class TestNotebookTask(unittest.TestCase):
    """Task tests"""

    def test_task_json(self):
        service_mock = MockService()

        task = NotebookTask(name="mynbtask", notebook="/notebook.py")
        r = task.task_json(service_mock, {
            "workspace_root": "/home/user",
        })
        self.assertEqual(r, {
            "task_key": "mynbtask",
            "source": "WORKSPACE",
            "job_cluster_key": None,
            "notebook_task": {
                "notebook_path": "/home/user/notebook",
                "base_parameters": {
                    "dbw.use_name_suffix": "",
                    "dbw.resource_storage_root": "",
                    "dbw.library_storage_root": "",
                    "dbw.files_storage_root": ""
                }
            },
        })


if __name__ == "__main__":
    unittest.main()
