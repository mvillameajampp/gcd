import logging
import heapq
import multiprocessing as mp
import threading as mt

from math import inf
from queue import Empty, Queue

from gcd.etc import identity
from gcd.chronos import as_timer, span


logger = logging.getLogger(__name__)


class Process(mp.Process):

    def __init__(self, target, *args, daemon=True, **kwargs):
        mp.Process.__init__(self, target=target, daemon=daemon, args=args,
                            kwargs=kwargs)

    def start(self):
        mp.Process.start(self)
        return self


class Thread(mt.Thread):

    def __init__(self, target, *args, daemon=True, **kwargs):
        mt.Thread.__init__(self, target=target, daemon=daemon, args=args,
                           kwargs=kwargs)

    def start(self):
        mt.Thread.start(self)
        return self


class Cluster:

    def __init__(self, nworkers, target, *args, new_process=True, **kwargs):
        worker_class = Process if new_process else Thread
        self.workers = [worker_class(target, i, *args, **kwargs)
                        for i in range(nworkers)]

    def start(self):
        for worker in self.workers:
            worker.start()
        return self

    def join(self):
        for worker in self.workers:
            worker.join()


class Task:

    def __init__(self, period_or_timer, callback, *args, new_process=False,
                 **kwargs):
        timer = as_timer(period_or_timer)
        worker_class = Process if new_process else Thread
        self.worker = worker_class(self._run, timer, callback, args, kwargs)

    def start(self):
        self.worker.start()
        return self

    def join(self):
        self.worker.join()

    def _run(self, timer, callback, args, kwargs):
        while True:
            timer.wait()
            try:
                callback(*args, **kwargs)
            except Exception:
                logger.exception('Error executing task %s',
                                 self.__class__.__name__)


class Batcher(Task):

    def __init__(self, period, handle, *args, hwm=10000, shared=False,
                 new_process=False, min_batch=1, **kwargs):
        self._queue = _queue(hwm, shared, new_process)
        super().__init__(period, self._callback, handle, min_batch,
                         args, kwargs, new_process=new_process)

    def add(self, obj):
        self._queue.put(obj)

    def _callback(self, handle, min_batch, args, kwargs):
        handle(dequeue(self._queue, min_batch), *args, **kwargs)


class Streamer(Task):

    def __init__(self, period, chunk, *args, hwm=10000, shared=False,
                 new_process=False, **kwargs):
        self._queue = _queue(hwm, shared, new_process)
        super().__init__(period, self._callback, chunk,
                         args, kwargs, new_process=new_process)

    def get(self):
        return self._queue.get()

    __next__ = get

    def __iter__(self):
        return self

    def _callback(self, chunk, args, kwargs):
        for obj in chunk(*args, **kwargs):
            self._queue.put(obj)


def dequeue(queue, at_least=1):
    for _ in range(at_least):
        yield queue.get()
    try:
        for _ in range(queue.qsize() - at_least):
            yield queue.get_nowait()
    except Empty:
        pass


def sorted_queue(queue, item=identity, log_period=span(minutes=5), hwm=10000):
    if hwm < inf and log_period:
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
            if seq > out_seq and max_seq - seq < hwm:
                break
            lost += seq - out_seq
            out_seq = seq + 1
            heapq.heappop(heap)
            yield seq, data


def _queue(hwm, shared, new_process):
    assert shared or not new_process
    queue_class = mp.Queue if (shared or new_process) else Queue
    return queue_class(hwm)
