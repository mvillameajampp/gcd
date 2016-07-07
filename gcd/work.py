import time
import logging
import heapq
import multiprocessing as mp
import threading as mt

from math import inf
from queue import Empty, Queue

from gcd.etc import identity, repeat_call, Bundle, Sentinel
from gcd.chronos import as_timer, span


logger = logging.getLogger(__name__)

default_queue_size = 1000

default_batch_size = 1000

default_batch_wait = 5


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
            except Exception:
                logger.exception('Error executing task')


class Batcher(Worker):

    def __init__(self, handle_batch, *args, batch_size=None, batch_wait=None,
                 queue_size=None, queue=None, new_process=False, **kwargs):
        batch_size = batch_size or default_batch_size
        batch_wait = batch_wait or default_batch_wait
        queue_size = queue_size or (batch_size + default_queue_size)
        self._queue = queue or globals()['queue'](queue_size, new_process)
        super().__init__(self._run, batch_size, batch_wait, handle_batch,
                         args, kwargs, new_process=new_process)

    def put(self, obj, *args, **kwargs):
        self._queue.put(obj, *args, **kwargs)

    def _run(self, batch_size, batch_wait, handle_batch, args, kwargs):
        while True:
            try:
                batch = list(dequeue(self._queue, batch_size, batch_wait))
                if batch:
                    handle_batch(batch, *args, **kwargs)
            except Exception:
                logger.exception('Error handling batch')


class Streamer(Worker):

    _stop = Sentinel('stop')

    def __init__(self, load_batch, *args, batch_size=None, batch_wait=None,
                 queue_size=None, queue=None, new_process=False, **kwargs):
        batch_size = batch_size or default_batch_size
        batch_wait = batch_wait or default_batch_wait
        queue_size = queue_size or (batch_size + default_queue_size)
        self._queue = queue or globals()['queue'](queue_size, new_process)
        super().__init__(self._run, batch_size, batch_wait, load_batch,
                         args, kwargs, new_process=new_process)

    def get(self, *args, **kwargs):
        return self._queue.get(*args, **kwargs)

    def __iter__(self):
        return repeat_call(self.get, until=self._stop)

    def _run(self, batch_size, batch_wait, load_batch, args, kwargs):
        info = Bundle(batch_size=batch_size, last_full=True, last_obj=None)
        obj = None
        while True:
            try:
                batch = load_batch(info, *args, **kwargs)
                if batch is None:
                    self._queue.put(self._stop)
                    return
                size = 0
                for obj in batch:
                    self._queue.put(obj)
                    size += 1
                info.update(last_obj=obj, last_full=size == batch_size)
                if size < batch_size:
                    time.sleep(batch_wait)
            except Exception:
                logger.exception('Error loading batch')


def queue(queue_size=None, shared=False):
    queue_class = mp.Queue if shared else Queue
    return queue_class(queue_size or default_queue_size)


def dequeue(queue, n=None, wait=None):
    assert n is not None or wait is not None
    n = n or inf
    wait_until = wait and time.time() + wait
    while n > 0:
        try:
            timeout = wait and max(0, wait_until - time.time())
            yield queue.get(timeout=timeout)
            n -= 1
        except Empty:
            return
        try:
            while n > 0:  # Avoid the syscall to time() if possible.
                yield queue.get_nowait()
                n -= 1
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
