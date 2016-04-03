import time

from unittest import TestCase, main
from unittest.mock import Mock, patch, call

from gcd.cache import NA, Miss, Cache, AsynCache


class TestCache(TestCase):

    @patch('time.time')
    def test(self, time_):
        time_.return_value = 0
        store = {'a': 1, 'b': 2}
        cache = Cache(tts=10, ttl=5)
        get = Mock(side_effect=lambda k: store.get(k, NA))
        cache._get = get

        # 'a' and 'b' enter cache, 'c' is NA.
        self.assertEqual(cache['a'], 1)
        self.assertEqual(cache['b'], 2)
        with self.assertRaises(KeyError):
            cache['c']
        get.assert_has_calls([call('a'), call('b'), call('c')])

        # 'a' is still fresh and alive.
        time_.return_value = 3
        get.reset_mock()
        cache.clean_up()
        self.assertEqual(cache['a'], 1)  # This is as keep-alive for 'a'.
        get.assert_has_calls([])

        # 'a' is fresh and alive, 'b' is dead.
        time_.return_value = 8
        get.reset_mock()
        cache.clean_up()
        self.assertEqual(cache['a'], 1)
        self.assertEqual(cache['b'], 2)
        get.assert_has_calls([call('b')])

        # 'a' is stale, 'b' is fresh and alive.
        time_.return_value = 12
        get.reset_mock()
        self.assertEqual(cache['a'], 1)
        cache.clean_up()
        self.assertEqual(cache['b'], 2)
        get.assert_has_calls([call('a')])


class TestAsynCache(TestCase):

    def test(self):
        store = {'a': 1, 'b': 2}
        cache = AsynCache(tts=10, ttl=5)
        get = Mock(side_effect=lambda ks: ((k, store[k])
                                           for k in ks if k in store))
        cache._get_batch = get

        with self.assertRaises(Miss):
            cache['a']
        with self.assertRaises(Miss):
            cache['b']
        with self.assertRaises(Miss):
            cache['c']

        time.sleep(0.1)
        self.assertEqual(cache['a'], 1)
        self.assertEqual(cache['b'], 2)
        with self.assertRaises(KeyError):
            cache['c']


if __name__ == '__main__':
    main()
