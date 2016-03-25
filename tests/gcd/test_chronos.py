import time

from unittest import TestCase
from unittest.mock import Mock, patch

from gcd.chronos import Timer, LeakyBucket
from gcd.work import Task


class TestTimer(TestCase):

    @patch('time.time')
    def test(self, time):
        time.return_value = 10
        timer = Timer(period=5)
        for current_time, is_time in [
                (10, False), (14, False),   # First period.
                (15, True),  (16, False),   # Second period.
                (25, True),  (27, False),   # Third period.
                (30, True)]:                # Last period.
            time.return_value = current_time
            self.assertEqual(timer.is_time, is_time)


class TestTask(TestCase):

    def test(self):
        callback = Mock()
        Task(0.1, callback).start()
        self.assertEqual(callback.call_count, 0)
        time.sleep(0.11)
        self.assertEqual(callback.call_count, 1)
        time.sleep(0.12)
        self.assertEqual(callback.call_count, 2)

    def test_now(self):
        callback = Mock()
        Task(Timer(10, now=True), callback).start()
        time.sleep(0.1)
        self.assertEqual(callback.call_count, 1)


class TestLeakyBucket(TestCase):

    def test(self):
        bucket = LeakyBucket(20)
        t0 = time.time()
        bucket.wait()
        t1 = time.time()
        bucket.wait()
        t2 = time.time()
        time.sleep(0.05)
        t3 = time.time()
        bucket.wait()
        t4 = time.time()
        bucket.wait()
        t5 = time.time()
        self.assertAlmostEqual(t1 - t0, 0, delta=0.01)
        self.assertAlmostEqual(t2 - t1, 0.05, delta=0.01)
        self.assertAlmostEqual(t4 - t3, 0, delta=0.01)
        self.assertAlmostEqual(t5 - t4, 0.05, delta=0.01)
