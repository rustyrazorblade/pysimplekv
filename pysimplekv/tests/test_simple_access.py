from unittest import TestCase
import os
import sure
import mock
import mmap

from pysimplekv import PySimpleKV, Page, Record, PAGE_SIZE

class RecordEncodingTest(TestCase):
    def test_key_encoding_and_decoding(self):
        r = Record("name", "jon")
        tmp = r.dumps()
        assert tmp != ""
        r2 = Record.loads(tmp)
        assert r == r2

    def test_equality_check(self):
        r = Record("name", "steve")
        r2 = Record("name", "steve")
        assert r == r2

class BaseTest(TestCase):
    location = "test.pskv"

    def setUp(self):
        # remove the test file if it exists
        try:
            os.remove(self.location)
        except:
            pass

        self.kv = PySimpleKV(self.location, initial_pages=2)

class CreateAndOpenTest(BaseTest):

    def test_create_and_open(self):
        self.kv

    def test_header_is_written(self):
        self.kv


    def test_correct_values_are_read_on_open(self):
        del self.kv
        # make sure the code path his the open

        self.kv = PySimpleKV(self.location)


class HashingTest(BaseTest):
    def test_hashing(self):
        page = self.kv.current_file.get_page("test")
        page.should.be.a(Page)


class PageTest(TestCase):

    def setUp(self):
        self.fp = open('page_test.pskv', 'w+')
        self.fp.write(" " * PAGE_SIZE)
        self.mmf = mmap.mmap(self.fp.fileno(), 0)
        self.page = Page(self.mmf)

    def tearDown(self):
        self.fp.close()


    def test_set_response(self):
        result = self.page.put("test", "blah")
        assert result == 0

        result = self.page.put("test", "blah")
        assert result == 1

    def test_set_and_get(self):
        result = self.page.put("test", "blah")
        assert result == 0

        result = self.page.get("test")
        result.value.should.be("blah")

    def test_write(self):
        self.page.put("test", "blah")
        self.page.write()
        tmp = self.page.mmf
        assert tmp != ""

    def test_load(self):
        self.page.put("test", "blah")
        self.page.write()

        tmp = self.page.mmf

        self.page.load()

    def test_many_writes(self):
        for x in range(500):
            k = str(x)
            self.page.put(k, k)



