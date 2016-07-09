import time
import queue

from unittest import TestCase, main
from itertools import islice

from gcd.work import (Thread, Task, Batcher, Streamer, dequeue, iter_queue,
                      sorted_queue)


class TestWorkers(TestCase):

    def test_task(self):
        def counter(step):
            nonlocal count
            if count == 2:
                raise Task.Stop
            count += step
        count = 0
        Task(0.1, counter, 2).start()
        self.assertEqual(count, 0)
        time.sleep(0.11)
        self.assertEqual(count, 2)
        time.sleep(0.11)
        self.assertEqual(count, 2)

    def test_batcher(self):
        def handle(batch):
            batches.append(list(batch))
        batches = []
        batcher = Batcher(handle, hwm=2, period=0.1).start()
        batcher.put(1)
        self.assertEqual(batches, [])
        time.sleep(0.11)
        self.assertEqual(batches, [[1]])
        batcher.put(2)
        batcher.put(3)
        with self.assertRaises(queue.Full):
            batcher.put(4, timeout=0)
        self.assertEqual(batches, [[1]])
        time.sleep(0.11)
        self.assertEqual(batches, [[1], [2, 3]])

    def test_streamer(self):
        def load(hwm, period):
            nonlocal i
            self.assertEqual(hwm, 2)
            self.assertEqual(period, 0.1)
            i += 1
            return batches[i]
        i = -1
        batches = [1], [2, 3, 4], [5, 6, Streamer.Stop]
        streamer = Streamer(load, hwm=2, period=0.1).start()
        time.sleep(0.11)
        self.assertEqual(streamer.get(), 1)
        with self.assertRaises(queue.Empty):
            streamer.get(block=False)
        time.sleep(0.11)
        self.assertEqual(streamer.get(), 2)
        self.assertEqual(streamer.get(), 3)
        self.assertEqual(streamer.get(), 4)
        with self.assertRaises(queue.Empty):
            streamer.get(block=False)
        time.sleep(0.11)
        self.assertEqual(list(streamer), [5, 6])


class TestQueues(TestCase):

    def test_dequeue(self):
        def enqueuer():
            q.put(1)
            time.sleep(0.1)
            q.put(2)
            q.put(3)
            q.put(4)
            q.put(5)
        q = queue.Queue()
        Thread(enqueuer).start()
        self.assertEqual(list(dequeue(q)), [1])
        self.assertEqual(list(dequeue(q)), [])
        self.assertEqual(list(dequeue(q, 2)), [2, 3])
        time.sleep(0.05)
        self.assertEqual(list(dequeue(q, at_most=1)), [4])

    def test_iter_queue(self):
        q = queue.Queue()
        for i in range(10):
            q.put(i)
        self.assertEqual(list(iter_queue(q, until=5)), [0, 1, 2, 3, 4])
        self.assertEqual(list(iter_queue(q, times=3)), [6, 7, 8])

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
