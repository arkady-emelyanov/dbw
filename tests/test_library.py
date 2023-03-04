import unittest

from dltx.library import Library
from dltx.service import MockService


class TestLibrary(unittest.TestCase):
    """Library tests"""

    def test_purge(self):
        service_mock = MockService()

        Library.purge(service_mock, {}, "/any")
        self.assertTrue(service_mock.dbfs.delete_calls > 0)

    def test_get_remote_path(self):
        service_mock = MockService()

        obj = Library("mydlttask")
        r = obj.get_remote_path(service_mock, {
            "library_storage_root": "dbfs:/tmp"
        })
        self.assertEqual(r, "dbfs:/tmp/mydlttask.tar.gz")

    def test_get_install_path(self):
        service_mock = MockService()

        obj = Library("mydlttask")
        r = obj.get_install_path(service_mock, {
            "library_storage_root": "dbfs:/tmp"
        })
        self.assertEqual(r, "/dbfs/tmp/mydlttask.tar.gz")

    def test_synch(self):
        self.skipTest("not yet implemented")


if __name__ == "__main__":
    unittest.main()
