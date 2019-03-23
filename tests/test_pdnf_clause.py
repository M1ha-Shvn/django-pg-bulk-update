from django.db.models import Q
from django.test import TestCase

from django_pg_bulk_update import pdnf_clause
from tests.models import TestModel


class PDNFClauseTest(TestCase):
    fixtures = ['test_model']

    def test_assertions(self):
        # field_names
        with self.assertRaises(TypeError):
            pdnf_clause(123, [])

        with self.assertRaises(TypeError):
            pdnf_clause([123], [])

        # field_values
        with self.assertRaises(TypeError):
            pdnf_clause(['id'], 123)

        with self.assertRaises(TypeError):
            pdnf_clause(['id'], [123])

        with self.assertRaises(ValueError):
            pdnf_clause(['id'], [{'invalid': 123}])

        # Operators
        with self.assertRaises(TypeError):
            pdnf_clause(['id'], [], key_fields_ops=123)

        with self.assertRaises(TypeError):
            pdnf_clause(['id'], [], key_fields_ops=[123])

        with self.assertRaises(ValueError):
            pdnf_clause(['id'], [], key_fields_ops=["invalid"])

        with self.assertRaises(ValueError):
            pdnf_clause(['id'], [], key_fields_ops={"id": "invalid"})

    def _test_filter(self, expected_res, field_names, field_values, operations=()):
        clause = pdnf_clause(field_names, field_values, key_fields_ops=operations)
        self.assertIsInstance(clause, Q)
        res = set(TestModel.objects.filter(clause).values_list('id', flat=True))
        self.assertSetEqual(expected_res, res)

    def test_field_names_format(self):
        self._test_filter({1, 3, 5, 7, 9}, ['id', 'int_field'], [(i, i) for i in range(1, 10, 2)])
        self._test_filter({2, 4, 6, 8}, 'int_field', [[i] for i in range(2, 10, 2)])

    def test_operations_format(self):
        self._test_filter({1, 3, 5, 7, 9}, ['id'], [[{1, 3}], [{5, 7, 9}]], operations=['in'])
        self._test_filter({2, 4, 6, 8}, ['id'], [[{2, 4}], [{6, 8}]], operations={'id': 'in'})

    def test_is_null(self):
        # Create model who has null value
        TestModel.objects.create(id=10)

        self._test_filter({10}, ['id', 'int_field'], [(1, True), (10, True)], operations=['=', 'is_null'])
        self._test_filter({1}, ['id', 'int_field'], [(1, False), (10, False)], operations=['=', 'is_null'])

    def test_in(self):
        self._test_filter({2, 4, 6, 8}, ['id'], [[{2, 4}], [{6, 8}]], operations=['in'])

    def test_not_in(self):
        self._test_filter({1, 2, 3, 4, 6, 7, 8, 9}, ['id'], [[{1, 3, 5}], [{5, 7, 9}]], operations=['!in'])

    def test_eq(self):
        self._test_filter({2, 4, 6, 8}, ['id'], [[i] for i in range(2, 10, 2)], operations=['eq'])
        self._test_filter({2, 4, 6, 8}, ['id'], [[i] for i in range(2, 10, 2)])

    def test_not_eq(self):
        # int_field = 2 only with i = 2. Other variants give false
        self._test_filter({2}, ['id', 'int_field'], [[i, 2] for i in range(2, 10, 2)], operations=['!eq'])

    def test_gt(self):
        self._test_filter({6, 7, 8, 9}, ['id'], [[6], [5]], operations=['gt'])

    def test_lt(self):
        self._test_filter({1, 2, 3, 4, 5}, ['id'], [[6], [5]], operations=['lt'])

    def test_gte(self):
        self._test_filter({5, 6, 7, 8, 9}, ['id'], [[6], [5]], operations=['gte'])

    def test_lte(self):
        self._test_filter({1, 2, 3, 4, 5, 6}, ['id'], [[6], [5]], operations=['lte'])

    def test_between(self):
        self._test_filter({2, 3, 4}, ['id'], [[[2, 4]]], operations=['between'])


class TestReadmeExample(TestCase):
    def test_example(self):
        # Skip bulk_update section (tested in other test), and init data as bulk_update_or_create start
        TestModel.objects.bulk_create([
            TestModel(pk=1, name="updated1", int_field=2),
            TestModel(pk=2, name="updated2", int_field=3),
            TestModel(pk=3, name="incr_concat1", int_field=4),
            TestModel(pk=4, name="concat2", int_field=5)
        ])

        cond = pdnf_clause(['id', 'name'], [([1, 2, 3], 'updated2'),
                                            ([3, 4, 5], 'concat2'),
                                            ([2, 3, 4], 'updated1')], key_fields_ops={'id': 'in'})
        data = TestModel.objects.filter(cond).order_by('int_field').values_list('int_field', flat=True)
        self.assertListEqual([3, 5], list(data))
