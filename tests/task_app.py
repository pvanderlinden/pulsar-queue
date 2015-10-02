'''Tests the taskqueue local backend.'''
import unittest
import asyncio
from random import random
from unittest.mock import MagicMock, Mock
from pulsar.async.clients import Pool

from pq import TaskApp


class TestTaskApp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        task_app = TaskApp(name='mytest', task_paths=['tests.example.sampletasks.*'])
        task_app.start()
        cls.task_app = task_app

    def test_test(self):
        backend = self.task_app.backend
        pubsub = backend._pubsub
        task_fut = backend.queue_task('addition', a=1, b=2, wait=False)
#         print (pubsub._pubsub._connection)
#         pubsub._pubsub._connection.open()
#         pubsub._pubsub.open()
        client = backend._pubsub._client
        # close
        client.store.pool.close(in_use=True)
        
        # must fail
        try:
            yield from pubsub.flush_queues()
        except:
            print ("failed")
        
        
        # hack
        Pool.connect = Mock(side_effect=ConnectionRefusedError('test'))
        
        
        # open
        from pulsar.apps.data import create_store
        from pq.pubsub import PubSub
        data_store = pubsub.cfg.data_store
        client.store = create_store(data_store)
        backend._pubsub = PubSub(backend)
        yield from pubsub.flush_queues()
        
#         connection = Mock(side_effect=ConnectionRefusedError('test'))
#         connection()
#         pass
        
