"""
Simple K/V store.  Implemented as a hash table.  single file


Data File format:

    Header: 32 bytes
        4 byte: pskv <-- magic string so we know this is a pskv
        2 byte: version (int), unsigned
        2 byte: key length
        2 bytes: entry size

        2 bytes: Pages
        2 bytes: keys per Page

        remainder: reserved

    Hash Table:
        Page size * number of Pages
        Page:
            contains any number of entries
            Entry: <length: short><key length:short><key><value length:short><value>

            # deprecated already
            Entry size * 32 (configurable)
            Entry:  1024 configurable
                Fixed sized key: 128 bytes (configurable)
                Value: 892 bytes by default (entry - header - key size)



File is allocated at an initial fixed size

Resized if it reaches a certain capacity

Files are open memory mapped

When a Page is filled up, rewrite the file


"""
import hashlib

import os
import mmap
import struct
import logging
from gevent.lock import BoundedSemaphore

PAGE_SIZE = mmap.PAGESIZE * 2 # 8K page size

class PySimpleKV(object):

    def __init__(self, location, initial_pages = 64, resize_multiplier=4):

        self.current_file = PySimpleKVFile(location,
                                           pages=initial_pages,
                                           resize_multiplier=resize_multiplier)

    def get(self, key):
        return self.current_file.get(key)

    def put(self, key, value):
        return self.current_file.put(key, value)

    def delete(self, key):
        return self.current_file.delete(key)

    def resize(self, Pages, keys_per_Page):
        return

    def __iter__(self):
        for page in self.current_file.pages.itervalues():
            for record in page.records.itervalues():
                yield record



class PySimpleKVFile(object):
    """
    Manages a hash file once it's been created
    """
    fp = None
    version = 1
    pages = None
    resize_multiplier = None
    keys_per_Page = None

    def __init__(self, location, pages = 64, resize_multiplier=2):
        """
        location is a specific file with a .pskv extension
        page_size is in bytes
        """
        self.location = location
        self.resize_multiplier = resize_multiplier
        self.page_count = pages
        self.pages = {}

        try:
            # open the file, read the header
            self.open()
        except:
            self.create()

    def get_mmf(self, page_num):
        offset = PAGE_SIZE * page_num
        mmf = mmap.mmap(self.fp.fileno(), PAGE_SIZE, offset=offset)
        return mmf

    def create(self):
        self.fp = open(self.location, 'w+b')
        # allocate the file
        for x in range(self.page_count + 1):
            self.fp.write(PAGE_SIZE * " ") # header
        # create first page
        mmf = self.get_mmf(0)
        self.pages[0] = Page(mmf)


    def open(self):
        self.fp = open(self.location, 'r+b')

    def get_page_number(self, key):
        md5 = hashlib.md5()
        md5.update(key)
        return int(md5.hexdigest(), 16) % self.page_count + 1

    def get_page(self, key):
        page_num = self.get_page_number(key)
        try:
            return self.pages[page_num]
        except KeyError as ie:
            offset = page_num * PAGE_SIZE
            mmf = mmap.mmap(self.fp.fileno(), PAGE_SIZE, offset=offset)
            self.pages[page_num] = Page(mmf)
            return self.pages[page_num]

    def get(self, key):
        Page = self.get_page(key)
        return Page.get(key)

    def put(self, key, value):
        page = self.get_page(key)
        assert isinstance(page, Page)
        return page.put(key, value)

    def delete(self, key):
        page = self.get_page(key)

class PageFullException(Exception):
    pass


page_header = struct.Struct("H")

class Page(dict):
    """
    Page format:
    header: record_count <short>

    """
    mmf = None # memory mapped file

    records = None
    lock = None

    def __init__(self, mmf):
        """
        mmf is a memory mapped file pointer
        """
        self.mmf = mmf
        self.records = {}
        self.lock = BoundedSemaphore(1)
        self.load()

    def load(self):
        # loads an entire Page into memory
        # get the number of records
        with self.lock:
            tmp = self.mmf[:].strip()

            if not tmp:
                return

            header = self.mmf[0:2]

            (records) = struct.unpack("H", header)
            body = self.mmf[2:]
            self.records = {}

            # loop over the rows till we extract the correct num
            for x in records:
                # header
                (length,) = struct.unpack("H", body[:2])

                record_data = body[2:2 + length]

                r = Record.loads(record_data)

                key = r.key
                self.records[key] = r
                body = body[2+length:]
                # the rest

    def __len__(self):
        return len(self.records)

    def write(self):
        # writes out the full page back to disk
        # compose the header
        with self.lock:
            header = page_header.pack(len(self.records))
            data = []
            for r in self.records.itervalues():
                assert isinstance(r, Record)
                row_data = r.dumps()

                packed = struct.pack("H%ds" % len(row_data),
                                     len(row_data), row_data)
                data.append(packed)
            self.mmf[:] = (header + "".join(data)).ljust(PAGE_SIZE)
            return

    def put(self, key, value):
        self.is_dirty = True
        if key in self.records:
            response = 1
        else:
            response = 0
        self.records[key] = Record(key, value)
        return response

    def get(self, key, default=None):
        return self.records[key]

    def iteritems(self):
        return []


    def __iter__(self):
        return self.records.itervalues()

    def __contains__(self, key):
        return key in self.records


class Record(object):
    """
    Record encoding
    klen <short> vlen <short> key value
    """
    key = None
    value = None

    def __init__(self, key, value):
        self.key = key
        self.value = value

    @classmethod
    def loads(cls, s):
        """
        returns a Record object
        """
        # read the header out
        (klen, vlen) = struct.unpack("HH", s[0:4])
        key = s[4:4+klen]
        value = s[4 + klen:]
        return Record(key, value)

    def dumps(self):
        klen = len(self.key)
        vlen = len(self.value)

        fmt = "HH%ds%ds" % (klen, vlen)
        return struct.pack(fmt, klen, vlen, self.key, self.value)


    def __eq__(self, other):
        return self.key == other.key and self.value == other.value
