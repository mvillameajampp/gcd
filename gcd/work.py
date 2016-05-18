import logging
import multiprocessing as mp
import threading as mt

from queue import Empty, Queue

from gcd.chronos import as_timer


logger = logging.getLogger(__name__)


def on_fork(handler):
    _fork_handlers.append(handler)


def run_fork_handlers():  # Usually called by Process.run.
    for handler in _fork_handlers:
        handler()


_fork_handlers = []


class Process(mp.Process):

    def __init__(self, target=None, *args, daemon=True, **kwargs):
        mp.Process.__init__(self, target=target, daemon=daemon, args=args,
                            kwargs=kwargs)

    def start(self):
        mp.Process.start(self)
        return self

    def run(self, *args, **kwargs):
        run_fork_handlers()
        super().run(*args, **kwargs)


class Thread(mt.Thread):

    def __init__(self, target=None, *args, daemon=True, **kwargs):
        mt.Thread.__init__(self, target=target, daemon=daemon, args=args,
                           kwargs=kwargs)

    def start(self):
        mt.Thread.start(self)
        return self


class Task:

    def __init__(self, period_or_timer, callback, new_process=False):
        timer = as_timer(period_or_timer)
        worker_class = Process if new_process else Thread
        self.worker = worker_class(self._run, timer, callback)

    def start(self):
        self.worker.start()
        return self

    def _run(self, timer, callback):
        while True:
            timer.wait()
            try:
                callback()
            except Exception:
                logger.exception('Error executing task %s',
                                 self.__class__.__name__)


class Batcher(Task):

    def __init__(self, timer, handle, new_process=False, min_batch=1,
                 queue=None, hwm=10000):
        def callback():
            handle(dequeue(self._queue, min_batch))
        Task.__init__(self, timer, callback, new_process)
        if queue is None:
            queue_class = mp.Queue if new_process else Queue
            queue = queue_class(hwm)
        self._queue = queue

    def add(self, obj):
        self._queue.put(obj)


def dequeue(queue, at_least=1):
    for _ in range(at_least):
        yield queue.get()
    try:
        for _ in range(queue.qsize() - at_least):
            yield queue.get_nowait()
    except Empty:
        pass
