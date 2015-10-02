'''Tests the taskqueue local backend.'''
import unittest
import asyncio
from random import random
from unittest.mock import MagicMock, Mock
from pulsar.async.clients import Pool

from pq import TaskApp
from pulsar.apps.data import create_store
from pq.pubsub import PubSub

class TestTaskApp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        task_app = TaskApp(name='mytest', task_paths=['tests.example.sampletasks.*'])
        task_app.start()
        cls.task_app = task_app
        cls.pool_connect = {True: Pool.connect, False: Mock(side_effect=ConnectionRefusedError('test'))} 

    def set_connection(self, enabled=True):
        backend = self.task_app.backend
        client = backend._pubsub._client
        pubsub = backend._pubsub
        client = backend._pubsub._client
        #close
        client.store.pool.close(in_use=True)
        # hack
        Pool.connect = self.pool_connect[enabled]
        # create
        data_store = pubsub.cfg.data_store
        client.store = create_store(data_store)
        backend._pubsub = PubSub(backend)

    def test_test(self):
        backend = self.task_app.backend
        pubsub = backend._pubsub
        
        task_fut = backend.queue_task('addition', a=1, b=2, wait=False)
        
        self.set_connection(True)
        yield from pubsub.flush_queues()
        
        self.set_connection(False)
        yield from pubsub.flush_queues()
        

        
