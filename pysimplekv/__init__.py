"""
Simple K/V store.  Implemented as a hash table.  single file


Data File format:

    Header: 32 bytes
        4 byte: pskv <-- magic string so we know this is a pskv
        2 byte: version (int), unsigned
        2 byte: key length
        2 bytes: entry size

        2 bytes: buckets
        2 bytes: keys per bucket

        remainder: reserved

    Hash Table:
        Bucket size * number of buckets
        Bucket:
            Entry size * 32 (configurable)
            Entry:  1024 configurable
                Fixed sized key: 128 bytes (configurable)
                Value: 892 bytes by default (entry - header - key size)



File is allocated at an initial fixed size

Resized if it reaches a certain capacity

Files are open memory mapped

When a bucket is filled up, rewrite the file


"""
import hashlib

import os
import mmap
import struct
import logging

class PySimpleKV(object):

    def __init__(self, location, initial_buckets = 128, \
                 keys_per_bucket=32, key_size=128, \
                 entry_size=1024, resize_multiplier=2):
        
        self.current_file = PySimpleKVFile(location, \
                                           initial_buckets, keys_per_bucket=keys_per_bucket,
                                           key_size=key_size, entry_size=entry_size,
                                           resize_multiplier=resize_multiplier)

    def get(self, key):
        return self.current_file.get(key)

    def put(self, key, value):
        return self.current_file.put(key, value)

    def delete(self, key):
        return self.current_file.delete(key)

    def resize(self, buckets, keys_per_bucket):
        return


class PySimpleKVFile(object):
    fp = None
    version = 1
    header = struct.Struct("4s5H")
    buckets = None
    resize_multiplier = None
    keys_per_bucket = None

    def __init__(self, location, initial_buckets = 128, keys_per_bucket=32, key_size=128, entry_size=1024, resize_multiplier=2):
        """
        as a convention, you should use a pskv extension but it's not required
        """
        self.location = location
        self.resize_multiplier = resize_multiplier

        try:
            # open the file, read the header
            self.open()
        except:
            self.create(key_size=key_size, \
                        entry_size=entry_size, \
                        initial_buckets=initial_buckets, \
                        keys_per_bucket=keys_per_bucket)


    def open(self):
        tmp = open(self.location, 'r+b')
        self.fp = mmap.mmap(tmp.fileno())
        header = self.fp.read(32)

    @property
    def value_size(self):
        return self.entry_size - self.key_size

    @property
    def bucket_size(self):
        return self.entry_size * self.keys

    def create(self, key_size, entry_size, initial_buckets, keys_per_bucket):
        self.fp = open(self.location, 'w+b')
        # write out the header
        self.buckets = initial_buckets
        self.key_size = key_size
        self.entry_size = entry_size
        self.keys_per_bucket = keys_per_bucket

        header = self.header.pack("pskv", self.version, key_size, entry_size, \
                                  initial_buckets, keys_per_bucket)
        self.fp.write(header)

        # fill out the file
        bucket_size = keys_per_bucket * entry_size

        # ensure we don't take up memory equal to our new file
        zeroed_bucket = struct.pack("c", " ") * bucket_size

        for i in range(initial_buckets):
            self.fp.write(zeroed_bucket)

    def get_bucket_number(self, key):
        md5 = hashlib.md5()
        md5.update(key)
        return int(md5.hexdigest(), 16) % self.buckets

    def get_bucket(self, key):
        bucket_num = self.get_bucket_number(key)
        self.seek_to_bucket(bucket_num)
        return Bucket(self.fp, self.entry_size, self.key_size, self.keys_per_bucket)

    def get(self, key):
        bucket = self.get_bucket(key)
        return bucket.get(key)

    def put(self, key, value):
        bucket = self.get_bucket(key)
        return bucket.write(key, value)

    def delete(self, key):
        bucket = self.get_bucket(key)

    def write_to_bucket(self, bucket, key, value):
        return None

    def seek_to_bucket(self, bucket):
        location = 32 + (bucket * self.buckets) # 32 byte header

        self.fp.seek(location)


class BucketFullException(Exception):
    pass


class Bucket(dict):
    position = 0
    entry_reader = None

    def __init__(self, fp, entry_size, key_size, keys_per_bucket):
        self.entry_size = entry_size
        self.key_size = key_size
        self.keys_per_bucket = keys_per_bucket
        self.fp = fp
        self.entry_reader = struct.Struct("{}s{}s".format(key_size, entry_size - key_size))

    def write(self, key, value):
        self.seek_key_position(key)
        value_size = self.entry_size - self.key_size
        packed = self.entry_reader.pack(key.ljust(self.key_size, " "), value.ljust(value_size, " "))
        self.fp.write(packed)
        logging.debug("Packed string: %s", packed)

        return

    def seek_key_position(self, key):
        # finds a position to write a key
        available = None

        for i in range(self.keys_per_bucket):
            current = self.fp.tell()
            logging.debug("looking for write position (current %d)", current)
            tmp = self.fp.read(self.entry_size)
            (k, _) = self.entry_reader.unpack(tmp)
            k = k.strip()

            # if k isn't set, we can use this as our first available bucket location
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
            raise BucketFullException

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
        for i in range(self.keys_per_bucket):
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

