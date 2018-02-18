from django.db.models import Q
from django.test import TestCase

from tests.models import TestModel
from django_pg_bulk_update import pdnf_clause


class PDNFClauseTest(TestCase):
    fixtures = ['test_model']

    def test_assertions(self):
        # field_names
        with self.assertRaises(AssertionError):
            pdnf_clause(123, [])

        with self.assertRaises(AssertionError):
            pdnf_clause([123], [])

        # field_values
        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], 123)

        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [123])

        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [{'invalid': 123}])

        # Operations
        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [], operators=123)

        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [], operators=[123])

        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [], operators=["invalid"])

        with self.assertRaises(AssertionError):
            pdnf_clause(['id'], [], operators={"id": "invalid"})

    def _test_filter(self, expected_res, field_names, field_values, operations=()):
        clause = pdnf_clause(field_names, field_values, operators=operations)
        self.assertIsInstance(clause, Q)
        res = set(TestModel.objects.filter(clause).values_list('id', flat=True))
        self.assertSetEqual(expected_res, res)

    def test_field_names_format(self):
        self._test_filter({1, 3, 5, 7, 9}, ['id', 'int_field'], [(i, i) for i in range(1, 10, 2)])
        self._test_filter({2, 4, 6, 8}, 'int_field', [[i] for i in range(2, 10, 2)])

    def test_operations_format(self):
        self._test_filter({1, 3, 5, 7, 9}, ['id'], [[{1, 3}], [{5, 7, 9}]], operations=['in'])
        self._test_filter({2, 4, 6, 8}, ['id'], [[{2, 4}], [{6, 8}]], operations={'id': 'in'})

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
