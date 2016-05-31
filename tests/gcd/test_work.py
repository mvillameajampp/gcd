import queue

from unittest import TestCase, main

from gcd.work import sortedq


class TestSortedQueue(TestCase):

    def test(self):
        q = queue.Queue()
        q.put((2, 'c'))
        q.put((0, 'a'))
        q.put((1, 'b'))
        q.put((6, 'g'))
        q.put((4, 'e'))
        q.put((3, 'd'))
        q.put((5, 'f'))
        sq = sortedq(q, 2)
        self.assertEqual([next(sq) for _ in range(6)],
                         ['a', 'b', 'c', 'e', 'f', 'g'])


if __name__ == '__main__':
    main()
