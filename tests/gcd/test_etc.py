from unittest import TestCase

from gcd.etc import product, chunks, as_many, Bundle


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
