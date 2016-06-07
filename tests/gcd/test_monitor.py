import logging
import json
import io
import statistics

from unittest import TestCase, main
from unittest.mock import patch

from gcd.monitor import JsonFormatter, Statistics
from gcd.chronos import day


class TestStatistics(TestCase):

    def test(self):
        xs = 1, 2, 3
        stats = Statistics()
        for x in xs:
            stats.add(x)
        self.assertEqual(stats.mean, statistics.mean(xs))
        self.assertEqual(stats.stdev, statistics.stdev(xs))
        self.assertEqual(stats.min, min(xs))
        self.assertEqual(stats.max, max(xs))
        self.assertEqual(stats.n, len(xs))

    @patch('gcd.monitor.time')
    def test_memory(self, time_):
        time_.time.return_value = 0
        stats = Statistics((0.5, day))
        stats.add(2)
        time_.time.return_value = day
        stats.add(3)
        stats.add(4, 0)
        n = 0.5 + 1 + 0.5
        self.assertAlmostEqual(stats.mean, (2 * 0.5 + 3 + 4 * 0.5) / n)
        self.assertAlmostEqual(stats.n, n)


class TestJsonFormatter(TestCase):

    def test_msg(self):
        logger, log = self.logger()
        logger.info('hi %s %s', 1, 2)
        self.assertEqual(json.loads(log.getvalue()),
                         {'message': 'hi 1 2'})

    def test_dict(self):
        logger, log = self.logger()
        logger.info({'x': 1, 'y': 2})
        self.assertEqual(json.loads(log.getvalue()),
                         {'x': 1, 'y': 2})

    def test_attrs(self):
        logger, log = self.logger(['name', 'levelname'])
        logger.info({'x': 1})
        self.assertEqual(json.loads(log.getvalue()),
                         {'x': 1, 'name': 'test', 'levelname': 'INFO'})

    def test_exc(self):
        logger, log = self.logger()
        try:
            raise TypeError
        except:
            logger.exception('')
        self.assertIn('exc_info', json.loads(log.getvalue()))
        self.assertIn('TypeError', log.getvalue())

    def logger(self, attrs=[]):
        log = io.StringIO()
        logger = logging.getLogger('test')
        handler = logging.StreamHandler(log)
        handler.setFormatter(JsonFormatter(attrs))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger, log


if __name__ == '__main__':
    main()
