import logging
import json
import io

from unittest import TestCase, main

from gcd.monitor import JsonFormatter, Statistics, Forgetter


class TestStatistics(TestCase):

    def test(self):
        xs = 1, 2, 3, 4
        ws = 1, 2, 1, 2
        ts = 19, 5, 1, 20
        memory = 0.9

        ms = [w * memory**(max(ts) - t) for w, t in zip(ws, ts)]
        en = sum(ms)
        emean = sum(x * m for x, m in zip(xs, ms)) / en
        estdev = (sum((x - emean)**2 * m for x, m in zip(xs, ms)) / en)**0.5
        emin = min(x * m for x, m in zip(xs, ms))
        emax = max(x * m for x, m in zip(xs, ms))

        stats = Statistics(memory, True)
        for x, w, t in zip(xs, ws, ts):
            stats.add(x, w, t)
        self.assertAlmostEqual(stats.n, en)
        self.assertAlmostEqual(stats.mean, emean)
        self.assertAlmostEqual(stats.stdev, estdev)
        self.assertAlmostEqual(stats.min, emin)
        self.assertAlmostEqual(stats.max, emax)

        forgetter = Forgetter(memory)
        stats = Statistics(forgetter, True)
        for x, w, t in zip(xs, ws, ts):
            forgetter.forget(w, t)
            stats.add(x)
        self.assertAlmostEqual(stats.n, en)
        self.assertAlmostEqual(stats.mean, emean)
        self.assertAlmostEqual(stats.stdev, estdev)
        self.assertAlmostEqual(stats.min, emin)
        self.assertAlmostEqual(stats.max, emax)



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
