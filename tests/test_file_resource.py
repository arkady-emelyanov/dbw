import unittest

from dltx.file_resource import FileResource
from dltx.service import MockService


class TestFileResource(unittest.TestCase):
    """Library tests"""

    def test_purge(self):
        service_mock = MockService()

        FileResource.purge(service_mock, {}, "/tmp/myconf.yml")
        self.assertTrue(service_mock.dbfs.delete_calls > 0)

    def test_get_remote_path(self):
        service_mock = MockService()

        obj = FileResource(name="myconf.yml")
        r = obj.get_remote_path(service_mock, {
            "files_storage_root": "dbfs:/tmp"
        })
        self.assertEqual("dbfs:/tmp/myconf.yml", r)

    def test_synch(self):
        self.skipTest("not yet implemented")


if __name__ == "__main__":
    unittest.main()
