#!/usr/bin/env python3


import dummy
import numpy
import couchdb



def adapt_array(array):
    """
    Using the numpy.save function to save a binary version of the array,
    and BytesIO to catch the stream of data and convert it into a bson.binary.Binary

    :param numpy.array array: NumPy array to turn into BLOB
    :return: NumPy array as bson.binary.Binary
    :rtype: bson.binary.Binary
    """
    return array.tolist()


def convert_array(blob):
    """
    Using BytesIO to convert the binary version of the array back into a numpy array.

    :param BLOG blob: BLOB containing a NumPy array
    :return: One steaming hot NumPy array
    :rtype: numpy.array
    """

    return numpy.array(blob)


uri = 'http://admin:admin@localhost:5984/'
dbname = 'embeddings'
couchserver = couchdb.Server(uri)

if dbname in couchserver:
    db = couchserver[dbname]
else:
    db = couchserver.create(dbname)

#########
# Write #
#########
for key, emb in dummy.embeddings():
    arr = adapt_array(emb)
    obj = {'key': key, 'emb': arr}
    db[key] = obj

########
# Read #
########
for key, _ in dummy.embeddings():
    obj = db[key]
    emb = convert_array(obj['emb'])
    assert(type(emb) is numpy.ndarray)


del couchserver[dbname]


# couchserver
