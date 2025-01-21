# coding:utf-8

import pickle
import os


def write_file(filename, value):
    if os.path.exists(filename):
        os.remove(filename)
    ori = pickle.dumps(value)
    with open(filename, 'wb') as f:
        f.write(ori)


def read_file(filename):
    with open(filename, 'rb') as f:
        return pickle.loads(f.read())

