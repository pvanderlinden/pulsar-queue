import time
import asyncio

from pulsar import is_async, ImproperlyConfigured, CANCELLED_ERRORS

from .task import TaskTimeout
from . import models
from . import states


class ConsumerMixin:
    """A mixin for consuming tasks from a distributed task queue.
    """
    queue = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        o.processed = 0
        o.concurrent_tasks = set()
        return o

    @property
    def num_concurrent_tasks(self):
        '''The number of :attr:`concurrent_tasks`.

        This number is never greater than the :attr:`backlog` attribute.
        '''
        return len(self.concurrent_tasks)

    def info(self):
        return {'concurrent': list(self.concurrent_tasks),
                'processed': self.processed,
                'queue': self.queue}

    def start(self, worker):
        '''Starts consuming tasks
        '''
        self.may_pool_task(worker)
        self.logger.debug('%s started polling tasks', self)

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def may_pool_task(self, worker, next_time=None):
        assert self.queue, 'Task queue not specified, cannot pull tasks'
        if self._closing:
            if not self.num_concurrent_tasks:
                self.logger.warning(self._closing)
                worker._loop.stop()
        else:
            if worker.is_running() and not next_time:
                asyncio.async(self._may_pool_task(worker), loop=worker._loop)
            else:
                next_time = next_time or 0
                worker._loop.call_later(next_time, self.may_pool_task, worker)

    def _may_pool_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = None
        if worker.is_running():
            # executor = worker.executor()
            if self.num_concurrent_tasks < self.cfg.concurrent_tasks:
                max_tasks = self.cfg.max_requests
                if max_tasks and self.processed >= max_tasks:
                    self._closing = ('Processed %s tasks. Restarting.'
                                     % self.processed)

                if not self._closing:
                    try:
                        task = yield from self._pubsub.get_task(self.queue)
                    except ConnectionRefusedError:
                        if worker.is_running():
                            raise
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling tasks')
                        raise
                    if task:    # Got a new task
                        self.processed += 1
                        self.concurrent_tasks.add(task.id)
                        asyncio.async(self._execute_task(worker, task))
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                next_time = 1
        self.may_pool_task(worker, next_time)

    def _execute_task(self, worker, task):
        logger = self.logger
        task_id = task.id
        time_ended = time.time()
        JobClass = self.registry.get(task.name)
        try:
            if not JobClass:
                raise RuntimeError('%s not in registry' % task.name)
            if task.status > states.STARTED:
                expiry = task.expiry
                if expiry and time_ended > expiry:
                    raise TaskTimeout
                else:
                    logger.info('starting %s', task.lazy_info())
                    kwargs = task.kwargs or {}
                    task.status = states.STARTED
                    task.time_started = time_ended
                    task.worker = worker.aid
                    yield from self._pubsub.publish('started', task)
                    # This may block for a while
                    job = JobClass(self, worker, task)
                    result = yield from self._consume(job, kwargs)
                    if is_async(result):
                        task.result = yield from result
                    else:
                        task.result = result
                    task.status = states.SUCCESS
            else:
                logger.error('invalid status for %s', task.lazy_info())
        except TaskTimeout:
            logger.warning('%s timed-out', task.lazy_info())
            task.result = None
            task.status = states.REVOKED
        except Exception as exc:
            logger.exception('failure in %s', task.lazy_info())
            task.result = str(exc)
            task.status = states.FAILURE
        #
        task.time_ended = time.time()
        self.concurrent_tasks.discard(task_id)
        #
        logger.info('finished %s', task.lazy_info())
        yield from self._pubsub.publish('done', task)

    def _consume(self, job, kwargs):
        concurrency = job.concurrency or models.THREAD_IO

        if concurrency == models.ASYNC_IO:
            result = job(**kwargs)
            assert is_async(result), "ASYNC_IO tasks not asynchronous"
            return result

        elif concurrency == models.GREEN_IO:
            return self._green_pool.submit(job, **kwargs)

        elif concurrency == models.THREAD_IO:
            return job._loop.run_in_executor(None, lambda: job(**kwargs))

        elif concurrency == models.CPUBOUND:
            return self._consume_in_subprocess()

        else:
            raise ImproperlyConfigured('invalid concurrency')
