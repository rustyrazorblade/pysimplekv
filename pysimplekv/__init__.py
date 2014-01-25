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

class PySimpleKV(object):

    def __init__(self, location, initial_pages = 128, page_size=8096, resize_multiplier=2):

        self.current_file = PySimpleKVFile(location,
                                           pages=initial_pages,
                                           page_size=page_size,
                                           resize_multiplier=resize_multiplier)

    def get(self, key):
        return self.current_file.get(key)

    def put(self, key, value):
        return self.current_file.put(key, value)

    def delete(self, key):
        return self.current_file.delete(key)

    def resize(self, Pages, keys_per_Page):
        return


class PySimpleKVFile(object):
    """
    Manages a hash file once it's been created
    """
    fp = None
    version = 1
    header = struct.Struct("4s5H")
    pages = None
    resize_multiplier = None
    keys_per_Page = None
    page_size = None

    def __init__(self, location, pages = 64, page_size=8096, resize_multiplier=2):
        """
        location is a specific file with a .pskv extension
        page_size is in bytes
        """
        self.location = location
        self.resize_multiplier = resize_multiplier
        self.pages = pages
        self.page_size = page_size

        try:
            # open the file, read the header
            self.open()
        except:
            self.create()

    def create(self):
        self.fp = open(self.location, 'w+b')
        # write out the header

        header = self.header.pack("pskv", self.version, self.page_size)
        self.fp.write(header)

        # fill out the file
        Page_size = keys_per_Page * entry_size

        # ensure we don't take up memory equal to our new file
        zeroed_Page = struct.pack("c", " ") * Page_size

        for i in range(initial_Pages):
            self.fp.write(zeroed_Page)

    def open(self):
        tmp = open(self.location, 'r+b')
        self.fp = mmap.mmap(tmp.fileno())
        header = self.fp.read(32)

    def get_page_number(self, key):
        md5 = hashlib.md5()
        md5.update(key)
        return int(md5.hexdigest(), 16) % self.Pages

    def get_page(self, key):
        Page_num = self.get_page_number(key)
        self.seek_to_Page(Page_num)
        return Page(self.fp, self.entry_size, self.key_size, self.keys_per_Page)

    def get(self, key):
        Page = self.get_page(key)
        return Page.get(key)

    def put(self, key, value):
        page = self.get_page(key)
        return page.write(key, value)

    def delete(self, key):
        page = self.get_page(key)

    def write_to_page(self, page, key, value):
        return None

    def seek_to_page(self, page):
        location = 32 + (page * self.pages) # 32 byte header

        self.fp.seek(location)


class PageFullException(Exception):
    pass


class Page(dict):
    position = 0
    entry_reader = None
    is_dirty = False
    start = None

    records = None


    def __init__(self, fp, page_size, start):
        self.start = start
        self.fp = fp
        self.records = []

    def load(self):
        # loads an entire Page into memory
        pass

    def write(self, key, value):
        # writes out the full page back to disk
        self.seek_key_position(key)
        value_size = self.entry_size - self.key_size
        packed = self.entry_reader.pack(key.ljust(self.key_size, " "), value.ljust(value_size, " "))
        self.fp.write(packed)
        logging.debug("Packed string: %s", packed)

        return

    def seek_key_position(self, key):
        # finds a position to write a key
        available = None

        for i in range(self.keys_per_page):
            current = self.fp.tell()
            logging.debug("looking for write position (current %d)", current)
            tmp = self.fp.read(self.entry_size)
            (k, _) = self.entry_reader.unpack(tmp)
            k = k.strip()

            # if k isn't set, we can use this as our first available page location
            if key == k:
                logging.debug("found existing key %s", key)
                self.fp.seek(current)
                return

            if k:
                logging.debug("found non matching key %s", k)

            if not k:
                logging.debug("empty key at %d", current)

                if not available:
                    available = current
                    logging.debug("setting available to %s", available)


        if not available:
            raise PageFullException

        logging.debug("Seeking to position %s", available)
        self.fp.seek(available)


    def get(self, key, default=None):

        for (k, v) in self.iteritems():
            logging.debug("examining %s", k)
            if k == key:
                return k

        return default

    def iteritems(self):
        result = []
        for i in range(self.keys_per_page):
            tmp = self.fp.read(self.entry_size)
            (key, value) = self.entry_reader.unpack(tmp)
            if key:
                result.append((key.strip(), value.strip()))

        return iter(result)


    def __iter__(self):
        return iter([x for (x,y) in self.iteritems()])


    def __contains__(self, key):
        for x in self:
            if x == key:
                return True


class Record(object):
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
        return

    def dumps(self):
        return

    def __eq__(self, other):
        return self.key == other.key and self.value == other.value
