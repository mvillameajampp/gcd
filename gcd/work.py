import logging
import heapq
import multiprocessing as mp
import threading as mt

from math import inf
from queue import Empty, Queue

from gcd.etc import identity, repeat_call, Sentinel
from gcd.chronos import as_timer, span


logger = logging.getLogger(__name__)

default_hwm = 5000

default_period = 1


class Process(mp.Process):

    def __init__(self, target, *args, daemon=True, **kwargs):
        super().__init__(target=target, daemon=daemon, args=args,
                         kwargs=kwargs)

    def start(self):
        super().start()
        return self


class Thread(mt.Thread):

    def __init__(self, target, *args, daemon=True, **kwargs):
        super().__init__(target=target, daemon=daemon, args=args,
                         kwargs=kwargs)

    def start(self):
        super().start()
        return self


class Worker:

    def __init__(self, *args, new_process=False, **kwargs):
        worker_class = Process if new_process else Thread
        self.worker = worker_class(*args, *kwargs)

    def start(self):
        self.worker.start()
        return self

    def join(self):
        self.worker.join()


class Cluster:

    def __init__(self, nworkers, target, *args, new_process=True, **kwargs):
        self.workers = [Worker(target, i, *args, new_process=new_process,
                               **kwargs)
                        for i in range(nworkers)]

    def start(self):
        for worker in self.workers:
            worker.start()
        return self

    def join(self):
        for worker in self.workers:
            worker.join()


class Task(Worker):

    class Stop(Exception):
        pass

    def __init__(self, period_or_timer, callback, *args, new_process=False,
                 **kwargs):
        timer = as_timer(period_or_timer)
        super().__init__(self._run, timer, callback, args, kwargs,
                         new_process=new_process)

    def _run(self, timer, callback, args, kwargs):
        while True:
            try:
                timer.wait()
                callback(*args, **kwargs)
            except Task.Stop:
                return
            except Exception:
                logger.exception('Error executing task')


class Batcher(Task):

    def __init__(self, handle_batch, *args, hwm=None, period=None, queue=None,
                 new_process=False, **kwargs):
        self._queue = queue or new_queue(hwm, new_process)
        super().__init__(period or default_period, self._callback,
                         handle_batch, args, kwargs, new_process=new_process)

    def put(self, obj, *args, **kwargs):
        self._queue.put(obj, *args, **kwargs)

    def _callback(self, handle_batch, args, kwargs):
        handle_batch(dequeue(self._queue, 1), *args, **kwargs)


class Streamer(Task):

    Stop = Sentinel('stop')

    def __init__(self, load_batch, *args, hwm=None, period=None,
                 queue=None, new_process=False, **kwargs):
        self._queue = queue or new_queue(hwm, new_process)
        super().__init__(period or default_period, self._callback,
                         load_batch, self._queue.maxsize, period, args, kwargs,
                         new_process=new_process)

    def get(self, *args, **kwargs):
        return self._queue.get(*args, **kwargs)

    def __iter__(self):
        return repeat_call(self.get, until=self.Stop)

    def _callback(self, load_batch, hwm, period, args, kwargs):
        for obj in load_batch(hwm, period, *args, **kwargs):
            self._queue.put(obj)
            if obj is self.Stop:
                raise Task.Stop


def new_queue(hwm=None, shared=False):
    queue_class = mp.Queue if shared else Queue
    return queue_class(hwm or default_hwm)


def dequeue(queue, at_least=0, at_most=None):
    at_most = at_most or queue.qsize()
    for _ in range(at_least):
        yield queue.get()
    try:
        for _ in range(at_most - at_least):
            yield queue.get_nowait()
    except Empty:
        pass


def iter_queue(queue, until=None, times=None):
    return repeat_call(queue.get, until=until, times=times)


def sorted_queue(queue, item=identity, log_period=span(minutes=5),
                 max_ooo=10000):
    if max_ooo < inf and log_period:
        def log():
            nonlocal seen, lost
            if lost:
                logger.info(dict(seen=seen, lost=lost))
                seen = lost = 0
        Task(log_period, log)
    heap = []
    max_seq = -1
    out_seq = seen = lost = 0
    while True:
        seq, data = item(queue.get())
        seen += 1
        if seq < out_seq:
            continue
        max_seq = max(max_seq, seq)
        heapq.heappush(heap, (seq, data))
        while heap:
            seq, data = heap[0]
            if seq > out_seq and max_seq - seq < max_ooo:
                break
            lost += seq - out_seq
            out_seq = seq + 1
            heapq.heappop(heap)
            yield seq, data
