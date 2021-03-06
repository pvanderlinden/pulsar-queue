from datetime import timedelta

import pq


class TestPeriodicJob(pq.PeriodicJob):
    abstract = True
    run_every = timedelta(hours=1)


class TestPeriodic(TestPeriodicJob):
    run_every = timedelta(seconds=5)

    def __call__(self):
        return 'OK'


class TestPeriodicError(TestPeriodicJob):
    run_every = timedelta(seconds=60)

    def __call__(self, msg=None):
        raise Exception(msg or 'kaputt')


class AnchoredEveryHour(TestPeriodicJob):
    anchor = pq.anchorDate(minute=25)

    def __call__(self):   # pragma    nocover
        pass
