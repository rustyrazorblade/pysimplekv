from unittest import TestCase
import os
import sure
import mock

from pysimplekv import PySimpleKV, Bucket


class BaseTest(TestCase):
    location = "test.pskv"

    def setUp(self):
        # remove the test file if it exists

        try:
            os.remove(self.location)
        except:
            pass

        self.kv = PySimpleKV(self.location, initial_buckets=2, keys_per_bucket=3)

class CreateAndOpenTest(BaseTest):

    def test_create_and_open(self):
        self.kv

    def test_header_is_written(self):
        self.kv


    def test_correct_values_are_read_on_open(self):
        del self.kv
        # make sure the code path his the open

        self.kv = PySimpleKV(self.location)


class PersistenceTest(BaseTest):
    def test_set_and_get(self):
        self.kv.put('test', 'test')

        bucket = self.kv.current_file.get_bucket('test')

        bucket.should.contain("test")

        self.kv.get('test').should.equal('test')



class HashingTest(BaseTest):
    def test_hashing(self):
        bucket = self.kv.current_file.get_bucket("test")
        bucket.should.be.a(Bucket)


