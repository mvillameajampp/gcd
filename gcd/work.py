import logging
import os
import subprocess
import multiprocessing as mp
import threading as mt

from queue import Empty, Queue

from gcd.chronos import Timer


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


class Task:

    def __init__(self, timer, callback, new_process=False):
        if type(timer) in (int, float):
            timer = Timer(timer)
        self.ncall = 1
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
                self.ncall += 1
            except Exception:
                logger.exception('Error executing task %s',
                                 self.__class__.__name__)


class PerProcess:

    def __init__(self, factory):
        self._factory = factory
        self._process_init()

    def get(self):
        if not self._created:
            self._value = self._factory()
            self._created = True
        return self._value

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self._pid != os.getpid():
            self._process_init()

    def _process_init(self):
        self._pid = os.getpid()
        self._value = None
        self._created = False


class Batcher(Task):

    def __init__(self, timer, handle, new_process=False, min_batch=1,
                 in_process=False, queue=None, max_queue=10000):
        def callback():
            handle(dequeue(self._queue, min_batch))
        Task.__init__(self, timer, callback, new_process)
        if queue is None:
            queue_class = mp.Queue if new_process else Queue
            queue = queue_class(max_queue)
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


def sh(cmd, input=None, capture=True, lines=False, exec=False):
    assert not ((input or lines) and exec)
    if exec:
        os.execl('/bin/sh', '/bin/sh', '-c', cmd)
    else:
        stdin = subprocess.PIPE if input is not None else None
        stdout = subprocess.PIPE if capture else None
        stderr = subprocess.PIPE if capture else None
        proc = subprocess.Popen(cmd, shell=True, universal_newlines=True,
                                stdin=stdin, stdout=stdout, stderr=stderr)
        output, error = proc.communicate(input)
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, output, error)
        else:
            return output.split('\n')[:-1] if lines else output
