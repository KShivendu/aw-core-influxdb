import logging
import unittest
from datetime import datetime

#from aw.client import awclient
from aw.core.models import Event

import aw.datastore
from aw.datastore import Datastore, get_storage_methods

logging.basicConfig(level=logging.DEBUG)

class ParametrizedTestCase(unittest.TestCase):
    @classmethod
    def parametrize(cls, *args, **kwargs):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        # From: http://eli.thegreenplace.net/2011/08/02/python-unit-testing-parametrized-test-cases
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(cls)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(cls(name, *args, **kwargs))
        return suite

class DatastoreTest(ParametrizedTestCase):
    def __init__(self, name, storage_strategy, **kwargs):
        ParametrizedTestCase.__init__(self, name, **kwargs)
        self.storage_strategy = storage_strategy

    def setUp(self):
        self.ds = Datastore(storage_strategy=self.storage_strategy)
        self.bucket = self.ds["test"]

    def test_store_and_retrieve(self):
        l = len(self.bucket.get())
        self.bucket.insert(Event({"label": "test"}))
        self.assertTrue(l+1 == len(self.bucket.get()))

    def test_insert_many(self):
        l = len(self.bucket.get())
        self.bucket.insert([Event({"label": "test"}), Event({"label": "test2"})])
        self.assertTrue(l+2 == len(self.bucket.get()))

if __name__ == "__main__":
    suite = unittest.TestSuite()

    storage_methods = get_storage_methods()
    datastore_tests = [DatastoreTest.parametrize(method) for method in storage_methods]
    suite.addTests(datastore_tests)

    runner = unittest.TextTestRunner()
    runner.run(suite)
