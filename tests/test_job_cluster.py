import unittest

from dltx.job_cluster import JobCluster
from dltx.service import MockService


class TestJobCluster(unittest.TestCase):
    """Job Cluster test"""

    def test_json(self):
        service_mock = MockService()

        cluster = JobCluster(name="myjobcluster")
        r = cluster.json(service_mock, {})
        self.assertEqual(r, {
            "job_cluster_key": "myjobcluster",
            "new_cluster": {
                "name": "myjobcluster",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2
                }
            }
        })

    def test_json_overrides(self):
        service_mock = MockService()

        cluster = JobCluster(
            name="myjobcluster",
            min_workers=3,
            max_workers=5,
        )
        r = cluster.json(service_mock, {
            "policy_id": "1234",
            "default_node_type_id": "m5.2xlarge",
            "default_driver_node_type_id": "m5.large",
            "default_spark_version": "3.2.0",
        })

        self.assertEqual(r, {
            "job_cluster_key": "myjobcluster",
            "new_cluster": {
                "name": "myjobcluster",
                "autoscale": {
                    "min_workers": 3,
                    "max_workers": 5
                },
                "driver_node_type_id": "m5.large",
                "node_type_id": "m5.2xlarge",
                "policy_id": "1234",
                "spark_version": "3.2.0",
            }
        })

    def test_new_cluster_only_json(self):
        cluster = JobCluster(name="myjobcluster")
        service_mock = MockService()
        r = cluster.json(service_mock, {}, new_cluster_only=True)

        self.assertEqual(r, {
            "name": "myjobcluster",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2
            }
        })


if __name__ == "__main__":
    unittest.main()
