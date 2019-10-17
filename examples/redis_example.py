import dummy
import numpy
import redis
import struct
import numpy as np

def adapt_array(array):
    """
    Adapt numpy array for saving into redisDB

    :param numpy.array array: NumPy array to encode
    :return: encoded NumPy array
    """
    array = array.astype(np.float32)
    h = array.shape[0]

    shape = struct.pack('>I',h)
    encoded = shape + array.tobytes()

    return encoded


def convert_array(encoded):
    """

    :param BLOG encoded: encoded NumPy array
    :return: One steaming hot NumPy array
    :rtype: numpy.array
    """
    
    h = struct.unpack('>I',encoded[:4])
    array = np.frombuffer(encoded, dtype=np.float32, offset=4).reshape(h)
    
    return array


db = redis.Redis(host='localhost', port=6379, db=0)


# #########
# # Write #
# #########
for key, emb in dummy.embeddings():
    arr = adapt_array(emb)
    db.set(key, arr)

# ########
# # Read #
# ########
for key, _ in dummy.embeddings():
    obj = db.get(key)
    emb = convert_array(obj)
    assert(type(emb) is numpy.ndarray)


# del couchserver[dbname]