import logging
import json
import io

from unittest import TestCase

from gcd.utils import product, chunks, as_many, Bundle, JsonFormatter


class TestMisc(TestCase):

    def test_product(self):
        self.assertEqual(product([2, 5]), 10)
        self.assertEqual(product([2, 5], start=2), 20)

    def test_chunks(self):
        self.assertEqual(list(map(list, chunks([1, 2, 3, 4, 5], 2))),
                         [[1, 2], [3, 4], [5]])

    def test_as_many(self):
        self.assertEqual(as_many(1), (1,))
        self.assertEqual(as_many(1, list), [1])
        self.assertEqual(as_many([1]), [1])
        self.assertEqual(as_many((1,)), (1,))

    def test_bundle(self):
        bundle1 = Bundle(a=1, b=2)
        self.assertEqual(bundle1.a, 1)
        self.assertEqual(bundle1.b, 2)
        bundle2 = Bundle(a=1, b=2)
        self.assertEqual(bundle1, bundle2)


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
        logger, log = self.logger(['name', 'levelname'])
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
