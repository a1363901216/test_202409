# coding:utf-8
import time, datetime, traceback, sys

import pickle
import os
import zlib


def write_file(filename, value):
    if os.path.exists(filename):
        os.remove(filename)
    ori = pickle.dumps(value)
    with open(filename, 'wb') as f:
        compressed_data = zlib.compress(ori)
        f.write(compressed_data)

def write_file2(filename, value):
    if os.path.exists(filename):
        os.remove(filename)
    ori = pickle.dumps(value)
    with open(filename, 'wb') as f:
        compressed_data = zlib.compress(ori)
        f.write(compressed_data)

def read_file(filename):
    with open(filename, 'rb') as f:
        compressed_data = f.read()
        ori = zlib.decompress(compressed_data)
        return pickle.loads(ori)
