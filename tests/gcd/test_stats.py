import statistics

from unittest import TestCase
from unittest.mock import patch

from gcd.stats import Statistics, MovingStatistics


class TestStatistics(TestCase):

    def test(self):
        xs = [1, 2, 3]
        ys = [4, 5, 6]
        stats = Statistics()
        stats.add(xs[0]).add(xs[1]).add(xs[2])
        self.assertEqual(stats.n, 3)
        self.assertAlmostEqual(stats.mean, statistics.mean(xs))
        self.assertAlmostEqual(stats.var, statistics.variance(xs))
        stats.add(ys[0]).add(ys[1]).add(ys[2])
        self.assertEqual(stats.n, 6)
        self.assertAlmostEqual(stats.mean, statistics.mean(xs + ys))
        self.assertAlmostEqual(stats.var, statistics.variance(xs + ys))

    def test_agg(self):
        xs = [1, 2, 3]
        ys = [4, 5, 6]
        stats_x = Statistics().add(xs[0]).add(xs[1]).add(xs[2])
        stats_y = Statistics().add(ys[0]).add(ys[1]).add(ys[2])
        stats_z = Statistics()
        stats = Statistics()
        stats.agg(stats_x).agg(stats_y).agg(stats_z)
        self.assertEqual(stats.n, 6)
        self.assertAlmostEqual(stats.mean, statistics.mean(xs + ys))
        self.assertAlmostEqual(stats.var, statistics.variance(xs + ys))


class TestMovingStatistics(TestCase):

    @patch('time.time')
    def test(self, time):

        def checked_add(t, x, mean, first_period, last_period):
            time.return_value = t
            stats.add(x)
            self.assertAlmostEqual(stats.agg().mean, mean)
            self.assertAlmostEqual(stats.cum.mean, mean)
            self.assertEqual(stats.first_period, first_period)
            self.assertEqual(stats.last_period, last_period)

        def checked_agg(mean, from_period, to_period):
            first_period = stats.first_period
            last_period = stats.last_period
            if mean is None:
                with self.assertRaises(AssertionError):
                    stats.agg(from_period, to_period).mean
            else:
                mean_ = stats.agg(from_period, to_period).mean
                self.assertAlmostEqual(mean, mean_)
            self.assertEqual(stats.first_period, first_period)
            self.assertEqual(stats.last_period, last_period)

        time.return_value = 0
        stats = MovingStatistics(period=5, max_periods=3)
        self.assertEqual(stats.start_time, 0)
        self.assertEqual(stats.period, 5)
        self.assertEqual(stats.first_period, 0)
        self.assertEqual(stats.last_period, 0)
        checked_add(t=0, x=1, mean=1.0, first_period=0, last_period=0)
        checked_add(t=1, x=2, mean=1.5, first_period=0, last_period=0)
        checked_agg(1.5, 0, 0)
        checked_agg(None, 0, 1)
        checked_add(t=5, x=3, mean=2.0, first_period=0, last_period=1)
        checked_add(t=8, x=4, mean=2.5, first_period=0, last_period=1)
        checked_add(t=10, x=5, mean=3.0, first_period=0, last_period=2)
        checked_agg(3.5, 1, 1)
        checked_agg(4.0, 1, 2)
        checked_agg(3.0, 0, 2)
        checked_add(t=14, x=6, mean=3.5, first_period=0, last_period=2)
        checked_add(t=16, x=7, mean=5.0, first_period=1, last_period=3)
        checked_add(t=17, x=8, mean=5.5, first_period=1, last_period=3)
        checked_add(t=30, x=9, mean=9.0, first_period=4, last_period=6)
        checked_agg(None, 0, 6)
        checked_agg(9.0, 4, 6)
