import time
import queue

from unittest import TestCase, main
from itertools import islice

from gcd.work import (Thread, Task, Batcher, Streamer, dequeue, iter_queue,
                      sorted_queue, default_queue_size)


class TestWorkers(TestCase):

    def test_task(self):
        def counter(step):
            nonlocal count
            count += step
        count = 0
        Task(0.1, counter, 2).start()
        self.assertEqual(count, 0)
        time.sleep(0.11)
        self.assertEqual(count, 2)
        time.sleep(0.11)
        self.assertEqual(count, 4)

    def test_batcher(self):
        def handle(batch):
            batches.append(batch)
        batches = []
        batcher = Batcher(handle, batch_size=2, batch_wait=0.1).start()
        self.assertEqual(batcher._queue.maxsize, 2 + default_queue_size)
        batcher.put(1)
        time.sleep(0.11)
        self.assertEqual(batches, [[1]])
        batcher.put(2)
        batcher.put(3)
        time.sleep(0.05)
        self.assertEqual(batches, [[1], [2, 3]])
        batcher.put(4)
        time.sleep(0.11)
        self.assertEqual(batches, [[1], [2, 3], [4]])

    def test_streamer(self):
        def load(last_obj, batch_size):
            nonlocal i
            if i == 3:
                time.sleep(1000000)
            i += 1
            self.assertEqual(last_obj, None if i == 0 else batches[i - 1][-1])
            self.assertEqual(batch_size, 2)
            return batches[i]
        i = -1
        batches = [1], [2, 3], [4], [5]
        streamer = Streamer(load, batch_size=2, batch_wait=0.1).start()
        self.assertEqual(streamer._queue.maxsize, 2 + default_queue_size)
        time.sleep(0.01)
        self.assertEqual(streamer.get(block=False), 1)
        with self.assertRaises(queue.Empty):
            streamer.get(block=False)
        time.sleep(0.11)
        self.assertEqual(streamer.get(block=False), 2)
        self.assertEqual(streamer.get(block=False), 3)
        self.assertEqual(streamer.get(block=False), 4)
        with self.assertRaises(queue.Empty):
            streamer.get(block=False)
        time.sleep(0.11)
        self.assertEqual(streamer.get(block=False), 5)


class TestQueues(TestCase):

    def test_dequeue(self):
        def enqueuer():
            q.put(1)
            time.sleep(0.1)
            q.put(2)
            time.sleep(0.1)
            q.put(3)
            q.put(4)
            q.put(5)
        q = queue.Queue()
        Thread(enqueuer).start()
        self.assertEqual(list(dequeue(q, 3, 0.15)), [1, 2])
        self.assertEqual(list(dequeue(q, 2)), [3, 4])
        self.assertEqual(list(dequeue(q, wait=0)), [5])

    def test_iter_queue(self):
        q = queue.Queue()
        for i in range(10):
            q.put(i)
        self.assertEqual(list(iter_queue(q, 5)), [0, 1, 2, 3, 4])

    def test_sorted_queue(self):
        msgs = [(2, 'c'), (0, 'a'), (1, 'b'), (6, 'g'),
                (4, 'e'), (3, 'd'), (5, 'f')]
        q = queue.Queue()
        for msg in msgs:
            q.put(msg)
        sq = sorted_queue(q, max_ooo=2)
        msgs.remove((3, 'd'))
        self.assertEqual(list(islice(sq, 6)), sorted(msgs))


if __name__ == '__main__':
    main()
