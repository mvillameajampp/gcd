from unittest import main

from gcd.etc import Flattenable
from gcd.store import PgTestCase, PgRecordStore, PgFlattener


class TestPgRecordStore(PgTestCase):

    class Data(Flattenable):

        _attrs = {0: ['id']}

        def __init__(self, id):
            self.id = id

        def __eq__(self, other):
            return self.id == other.id

    def test(self):
        flattener = PgFlattener('jsonb', self.Data)
        store = PgRecordStore(flattener, self.pool).create()

        row1, row2, row3 = ((i, self.Data(i)) for i in range(1, 4))
        store.add([row1, row2])
        store.add([row3])

        self.assertEqual(list(store.get()), [row1, row2, row3])
        self.assertEqual(list(store.get(2)), [row2, row3])
        self.assertEqual(list(store.get(None, 3)), [row1, row2])
        self.assertEqual(list(store.get(2, 3)), [row2])
        self.assertEqual(list(store.get(where=('(data->>1)::int = %s', 2))),
                         [row2])


if __name__ == '__main__':
    main()
