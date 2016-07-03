import queue

from unittest import TestCase, main
from itertools import islice

from gcd.work import sorted_queue


class TestSortedQueue(TestCase):

    def test(self):
        msgs = [(2, 'c'), (0, 'a'), (1, 'b'), (6, 'g'),
                (4, 'e'), (3, 'd'), (5, 'f')]
        q = queue.Queue()
        for msg in msgs:
            q.put(msg)
        sq = sorted_queue(q, hwm=2)
        msgs.remove((3, 'd'))
        self.assertEqual(list(islice(sq, 6)), sorted(msgs))


if __name__ == '__main__':
    main()
