import logging

from unittest import TestCase, main

from gcd.etc import product, chunks, as_many, retry_on, Bundle


class TestFunctions(TestCase):

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

    def test_retry_on(self):
        def f():
            nonlocal ncalls
            ncalls += 1
            if ncalls < 3:
                raise ValueError
            if ncalls < 6:
                raise KeyError
            raise TypeError
        logger = logging.getLogger()
        level = logger.level
        try:
            logger.setLevel(logging.CRITICAL)
            ncalls = 0
            retry_on((ValueError, KeyError), 5)(f)()
            self.assertEqual(ncalls, 5)
            with self.assertRaises(TypeError):
                ncalls = 0
                retry_on((ValueError, KeyError), 15)(f)()
                self.assertEqual(ncalls, 6)
        finally:
            logger.setLevel(level)


if __name__ == '__main__':
    main()
