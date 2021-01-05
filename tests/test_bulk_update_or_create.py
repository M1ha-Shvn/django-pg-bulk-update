from datetime import datetime, timedelta, date
from unittest import skipIf

import pytz
from django.test import TestCase
from django.utils.timezone import now

from django_pg_bulk_update.compatibility import jsonb_available, array_available, hstore_available
from django_pg_bulk_update.query import bulk_update_or_create
from django_pg_bulk_update.set_functions import ConcatSetFunction
from tests.models import TestModel, UniqueNotPrimary, UpperCaseModel, AutoNowModel, TestModelWithSchema


class TestInputFormats(TestCase):
    fixtures = ['test_model']

    def test_model(self):
        with self.assertRaises(TypeError):
            bulk_update_or_create(123, [])

        with self.assertRaises(TypeError):
            bulk_update_or_create('123', [])

    def test_values(self):
        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, 123)

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [123])

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, {(1, 2): {'id': 10}})

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, {1: {'id': 10}}, key_fields=('id', 'name'))

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'name': 'test'}])

        self.assertEqual(2, bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'abc'}, {'id': 21, 'name': 'create'}]))

        self.assertEqual(2, bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'abc', 'int_field': 2},
                        {'id': 20, 'name': 'abc', 'int_field': 3}], key_fields=('id', 'name')))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, {1: {'name': 'abc'}, 19: {'name': 'created'}}))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, {(1,): {'name': 'abc'}, (18,): {'name': 'created'}}))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, {(2, 'test2'): {'int_field': 2}, (17, 'test2'): {'int_field': 4}}, key_fields=('id', 'name')))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, {('test33',): {'int_field': 2}, ('test3',): {'int_field': 2}}, key_fields='name',
            key_is_unique=False))

    def test_key_fields(self):
        values = [{
            'id': 1,
            'name': 'bulk_update_or_create_1'
        }, {
            'id': 10,
            'name': 'bulk_update_or_create_2'
        }]

        self.assertEqual(2, bulk_update_or_create(TestModel, values))
        values[1]['id'] += 1
        self.assertEqual(2, bulk_update_or_create(TestModel, values, key_fields='id'))
        values[1]['id'] += 1
        self.assertEqual(2, bulk_update_or_create(TestModel, values, key_fields=['id']))
        values[1]['id'] += 1
        # All fields to update are in key_fields. So we can skip update
        self.assertEqual(1, bulk_update_or_create(TestModel, values, key_fields=['id', 'name']))
        values[1]['id'] += 1
        values[1]['name'] += '1'
        self.assertEqual(2, bulk_update_or_create(TestModel, values, key_fields='name', key_is_unique=False))
        values[1]['id'] += 1
        values[1]['name'] += '1'
        self.assertEqual(2, bulk_update_or_create(TestModel, values, key_fields=['name'], key_is_unique=False))

    def test_using(self):
        values = [{
            'id': 1,
            'name': 'bulk_update_or_create_1'
        }, {
            'id': 10,
            'name': 'bulk_update_or_create_2'
        }]

        self.assertEqual(2, bulk_update_or_create(TestModel, values))
        values[1]['id'] += 1
        self.assertEqual(2, bulk_update_or_create(TestModel, values, using='default'))

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, values, using='invalid')

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, values, using=123)

    def test_set_functions(self):
        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions=123)

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions=[123])

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={1: 'test'})

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={'id': 1})

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={'invalid': 1})

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'int_field': 1}], set_functions={'int_field': 'invalid'})

        # I don't test all set functions here, as there is another TestCase for this: TestSetFunctions
        self.assertEqual(2, bulk_update_or_create(
            TestModel, [{'id': 2, 'name': 'test1'}, {'id': 10, 'name': 'test1'}],
            set_functions={'name': ConcatSetFunction()}
        ))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, [{'id': 2, 'name': 'test1'}, {'id': 11, 'name': 'test1'}], set_functions={'name': '||'}))

    def test_update(self):
        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], update=123)

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': ['test1']}], update='123')

        self.assertEqual(1, bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'test30'}, {'id': 20, 'name': 'test30'}], update=False))
        self.assertEqual(2, bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'test30'}, {'id': 19, 'name': 'test30'}], update=True))

    def test_batch(self):
        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size='abc')

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=-2)

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=2.5)

        with self.assertRaises(TypeError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=1, batch_delay='abc')

        with self.assertRaises(ValueError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=1, batch_delay=-2)


class TestSimple(TestCase):
    fixtures = ['test_model', 'test_upper_case_model', 'auto_now_model']
    multi_db = True
    databases = ['default', 'secondary']

    def test_update(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_upper_case(self):
        res = bulk_update_or_create(UpperCaseModel, [{
            'id': 1,
            'UpperCaseName': 'BulkUpdate1'
        }, {
            'id': 3,
            'UpperCaseName': 'BulkUpdate3'
        }, {
            'id': 4,
            'UpperCaseName': 'BulkUpdate4'
        }])
        self.assertEqual(3, res)

        # 3 from fixture + 1 created
        self.assertEqual(4, UpperCaseModel.objects.all().count())

        for pk, name in UpperCaseModel.objects.all().order_by('id').values_list('id', 'UpperCaseName'):
            if pk in {1, 3, 4}:
                self.assertEqual('BulkUpdate%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

    def test_empty(self):
        res = bulk_update_or_create(TestModel, [])
        self.assertEqual(0, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)

    def test_quotes(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': '\''
        }, {
            'id': 11,
            'name': '"'
        }])
        self.assertEqual(2, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk == 1:
                self.assertEqual('\'', name)
            elif pk == 11:
                self.assertEqual('"', name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk != 11:
                self.assertEqual(pk, int_field)
            else:
                self.assertIsNone(int_field)

    def test_key_update(self):
        res = bulk_update_or_create(TestModel, {
            (1, 'test1'): {
                'id': 1,
                'name': 'bulk_update_1'
            },
            (5, 'test5'): {
                'id': 5,
                'name': 'bulk_update_5'
            },
            (11, 'test11'): {
                'id': 11,
                'name': 'bulk_update_11'
            }
        }, key_fields=('id', 'name'))
        self.assertEqual(3, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5}:
                # Note due to insert on conflict restrictions key fields will be prior to update ones on insert.
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk != 11:
                self.assertEqual(pk, int_field)
            else:
                self.assertIsNone(int_field)

    def test_using(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], using='secondary')
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().using('secondary').count())
        self.assertEqual(9, TestModel.objects.all().using('default').count())

        for pk, name, int_field in TestModel.objects.all().using('secondary').order_by('id').\
                values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)

    def test_batch(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], batch_size=1)
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

        # Test for empty values correct
        res = bulk_update_or_create(TestModel, [], batch_size=10)
        self.assertEqual(0, res)

    def test_unique_not_primary(self):
        """
        Test for issue https://github.com/M1hacka/django-pg-bulk-update/issues/19
        :return:
        """
        # Test object
        UniqueNotPrimary.objects.create(int_field=1)

        res = bulk_update_or_create(UniqueNotPrimary, [{
            'int_field': 1,
        }, {
            'int_field': 2,
        }], key_fields='int_field')

        self.assertEqual(1, res)
        self.assertSetEqual({1, 2}, set(UniqueNotPrimary.objects.values_list('int_field', flat=True)))

    def test_returning(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], returning=('id', 'name', 'int_field'))

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (1, 'bulk_update_1', 1),
            (5, 'bulk_update_5', 5),
            (11, 'bulk_update_11', None),
        }, set(res.values_list('id', 'name', 'int_field')))

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_returning_no_update(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], returning=('id', 'name', 'int_field'), update=False)

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({(11, 'bulk_update_11', None)}, set(res.values_list('id', 'name', 'int_field')))

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk == 11:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_returning_all(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], returning='*')

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (1, 'bulk_update_1', 1),
            (5, 'bulk_update_5', 5),
            (11, 'bulk_update_11', None),
        }, set(res.values_list('id', 'name', 'int_field')))

    def test_returning_empty(self):
        res = bulk_update_or_create(TestModel, [], returning='id')
        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertEqual(0, res.count())

    def test_returning_not_unique(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], returning=('id', 'name', 'int_field'), key_is_unique=False)

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (1, 'bulk_update_1', 1),
            (5, 'bulk_update_5', 5),
            (11, 'bulk_update_11', None),
        }, set(res.values_list('id', 'name', 'int_field')))

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_returning_not_unique_empty(self):
        res = bulk_update_or_create(TestModel, [], returning='id', key_is_unique=False)
        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertEqual(0, res.count())

    def test_auto_now(self):
        res = bulk_update_or_create(AutoNowModel, [{
            'id': 1,
            'checked': None
        }, {
            'id': 11,
            'checked': datetime(2020, 1, 2, 0, 0, 0, tzinfo=pytz.utc)
        }])
        self.assertEqual(2, res)

        # 1 from fixture + 1 created
        self.assertEqual(2, AutoNowModel.objects.all().count())

        for instance in AutoNowModel.objects.all():
            self.assertEqual(instance.updated, datetime.now(pytz.utc).date())

            if instance.pk <= 10:
                print(instance.pk)
                self.assertEqual(datetime(2019, 1, 1, 0, 0, 0, tzinfo=pytz.utc), instance.created)
            else:
                self.assertGreaterEqual(instance.created, now() - timedelta(seconds=1))
                self.assertLessEqual(instance.created, now() + timedelta(seconds=1))

    def test_quoted_table_name(self):
        # Test for https://github.com/M1ha-Shvn/django-pg-bulk-update/issues/63
        self.assertEqual(2, bulk_update_or_create(
            TestModelWithSchema, [{'id': 1, 'name': 'abc'}, {'id': 21, 'name': 'create'}]))


class TestReadmeExample(TestCase):
    def test_example(self):
        # Skip bulk_create and bulk_update section (tested in other test), and init data as bulk_update_or_create start
        TestModel.objects.bulk_create([
            TestModel(pk=1, name="updated1", int_field=2),
            TestModel(pk=2, name="updated2", int_field=3),
            TestModel(pk=3, name="incr", int_field=4),
        ])

        res = bulk_update_or_create(TestModel, [{
            "id": 3,
            "name": "_concat1",
            "int_field": 4
        }, {
            "id": 4,
            "name": "concat2",
            "int_field": 5
        }], set_functions={'name': '||'})
        self.assertEqual(2, res)

        self.assertListEqual([
            {"id": 1, "name": "updated1", "int_field": 2},
            {"id": 2, "name": "updated2", "int_field": 3},
            {"id": 3, "name": "incr_concat1", "int_field": 4},
            {"id": 4, "name": "concat2", "int_field": 5},
        ], list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))


class TestSetFunctions(TestCase):
    fixtures = ['test_model']

    def test_incr(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'int_field': 1
        }, {
            'id': 5,
            'int_field': 5
        }, {
            'id': 11,
            'int_field': 11
        }], set_functions={'int_field': '+'})
        self.assertEqual(3, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5}:
                self.assertEqual(2 * pk, int_field)
            else:
                self.assertEqual(pk, int_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    def test_concat_str(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }], set_functions={'name': '||'})
        self.assertEqual(3, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5}:
                self.assertEqual('test%dbulk_update_%d' % (pk, pk), name)
            elif pk == 11:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk != 11:
                self.assertEqual(pk, int_field)
            else:
                self.assertIsNone(int_field)

    def _test_concat_array(self, iteration, res):
        self.assertEqual(4, res)
        for pk, name, array_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'array_field'):
            if pk in {1, 2, 11}:
                self.assertListEqual([pk] * iteration, array_field)
            elif pk == 4:
                self.assertListEqual([], array_field)
            else:
                self.assertIsNone(array_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_concat_array(self):
        for i in range(1, 5):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'array_field': [1]},
                                                    {'id': 2, 'array_field': [2]},
                                                    {'id': 11, 'array_field': [11]},
                                                    {'id': 4, 'array_field': []}], set_functions={'array_field': '||'})
            self._test_concat_array(i, res)

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_concat_empty(self):
        res = bulk_update_or_create(TestModel, [{'id': 11, 'big_array_field': [2147483649]}],
                                    set_functions={'big_array_field': '||'})
        self.assertEqual(1, res)
        self.assertListEqual([2147483649], TestModel.objects.get(pk=11).big_array_field)

    def _test_union_array(self, iteration, res):
        self.assertEqual(4, res)
        for pk, name, array_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'array_field'):
            if pk == 1:
                self.assertListEqual([pk], array_field)
            elif pk in {2, 11}:
                # Union doesn't save order, let's sort result
                array_field.sort()
                self.assertListEqual(list(range(1, iteration + 1)), array_field)
            elif pk == 4:
                self.assertListEqual([], array_field)
            else:
                self.assertIsNone(array_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_union_array(self):
        for i in range(1, 5):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'array_field': [1]},
                                                    {'id': 2, 'array_field': [i]},
                                                    {'id': 11, 'array_field': [i]},
                                                    {'id': 4, 'array_field': []}], set_functions={'array_field': 'union'})
            self._test_union_array(i, res)

    def _test_concat_dict(self, iteration, res, field_name, val_as_str=False):
        self.assertEqual(4, res)
        for pk, name, dict_field in TestModel.objects.all().order_by('id').values_list('id', 'name', field_name):
            if pk in {1, 2, 11}:
                # Note that JSON standard uses only strings as keys. So json.dumps will convert it
                expected = {str(i): str(pk) if val_as_str else pk for i in range(1, iteration + 1)}
                self.assertDictEqual(expected, dict_field)
            elif pk == 4:
                self.assertDictEqual({}, dict_field)
            else:
                self.assertIsNone(dict_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    @skipIf(not hstore_available(), "HStoreField is available in Django 1.8+")
    def test_concat_hstore(self):
        for i in range(1, 5):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'hstore_field': {i: 1}},
                                                    {'id': 2, 'hstore_field': {i: 2}},
                                                    {'id': 11, 'hstore_field': {i: 11}},
                                                    {'id': 4, 'hstore_field': {}}],
                                        set_functions={'hstore_field': '||'})
            self._test_concat_dict(i, res, 'hstore_field', val_as_str=True)

    @skipIf(not jsonb_available(), "JSONB type is available in Postgres 9.4+ and django 1.9+ only")
    def test_concat_jsonb(self):
        for i in range(1, 5):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'json_field': {i: 1}},
                                                    {'id': 2, 'json_field': {i: 2}},
                                                    {'id': 11, 'json_field': {i: 11}},
                                                    {'id': 4, 'json_field': {}}], set_functions={'json_field': '||'})
            self._test_concat_dict(i, res, 'json_field')

    def test_eq_not_null(self):
        res = bulk_update_or_create(TestModel, [{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': None
        }, {
            'id': 11,
            'name': None
        }], set_functions={'name': 'eq_not_null'})
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1}:
                self.assertEqual('bulk_update_%d' % pk, name)
            elif pk == 11:
                # Default name, not None, look https://github.com/M1hacka/django-pg-bulk-update/issues/2
                self.assertEqual('', name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_array_remove(self):
        def _test_array_remove(kwargs):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'array_field': 1},
                                                    {'id': 2, 'array_field': 2},
                                                    {'id': 13, 'array_field': 13}],
                                        set_functions={'array_field': 'array_remove'}, **kwargs)
            self.assertEqual(3, res)

            for pk, array_field in TestModel.objects.filter(id__in=[1, 2, 13]).values_list('pk', 'array_field'):
                if pk == 1:
                    self.assertEqual([2], array_field)
                elif pk == 2:
                    self.assertEqual([1], array_field)
                elif pk == 13:
                    self.assertEqual(None, array_field)

        TestModel.objects.all().update(array_field=[1, 2])
        _test_array_remove({'key_is_unique': False})  # Force 3-step query

        TestModel.objects.filter(id=13).delete()
        TestModel.objects.all().update(array_field=[1, 2])
        _test_array_remove({'key_is_unique': True})


class TestManager(TestCase):
    fixtures = ['test_model']
    multi_db = True
    databases = ['default', 'secondary']

    def test_bulk_update_or_create(self):
        res = TestModel.objects.pg_bulk_update_or_create([{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_using(self):
        res = TestModel.objects.db_manager('secondary').pg_bulk_update_or_create([{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().using('secondary').count())
        self.assertEqual(9, TestModel.objects.all().using('default').count())

        for pk, name, int_field in TestModel.objects.all().using('secondary').order_by('id'). \
                values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
                self.assertEqual('bulk_update_%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk == 11:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)


class TestFieldTypes(TestCase):
    fixtures = ['test_model']

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_array(self):
        res = bulk_update_or_create(TestModel, [{'id': 1, 'array_field': [1]},
                                                {'id': 2, 'array_field': [2]},
                                                {'id': 11, 'array_field': [11]},
                                                {'id': 4, 'array_field': []}])
        self.assertEqual(4, res)
        for pk, name, array_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'array_field'):
            if pk in {1, 2, 11}:
                self.assertListEqual([pk], array_field)
            elif pk == 4:
                self.assertListEqual([], array_field)
            else:
                self.assertIsNone(array_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    @skipIf(not jsonb_available(), "JSONB type is available in Postgres 9.4+ and django 1.9+ only")
    def test_jsonb(self):
        res = bulk_update_or_create(TestModel, [{'id': 1, 'json_field': {'test': '1'}},
                                                {'id': 2, 'json_field': {'test': '2'}},
                                                {'id': 11, 'json_field': {'test': '11'}},
                                                {'id': 4, 'json_field': {}},
                                                {'id': 5, 'json_field': {'single': "'", "multi": '"'}}])
        self.assertEqual(5, res)
        for pk, name, json_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'json_field'):
            if pk in {1, 2, 11}:
                self.assertDictEqual({'test': str(pk)}, json_field)
            elif pk == 4:
                self.assertDictEqual({}, json_field)
            elif pk == 5:
                self.assertDictEqual({'single': "'", "multi": '"'}, json_field)
            else:
                self.assertIsNone(json_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertEqual('', name)

    @skipIf(not hstore_available(), "HStoreField is available in Django 1.8+")
    def test_hstore(self):
        res = bulk_update_or_create(TestModel, [{'id': 1, 'hstore_field': {'test': '1'}},
                                                {'id': 2, 'hstore_field': {'test': '2'}},
                                                {'id': 11, 'hstore_field': {'test': '11'}},
                                                {'id': 4, 'hstore_field': {}},
                                                {'id': 5, 'hstore_field': {'single': "'", "multi": '"'}}])
        self.assertEqual(5, res)
        for item in TestModel.objects.all().order_by('id'):
            if item.pk in {1, 2, 11}:
                self.assertDictEqual({'test': str(item.pk)}, item.hstore_field)
            elif item.pk == 4:
                self.assertDictEqual({}, item.hstore_field)
            elif item.pk == 5:
                self.assertDictEqual({'single': "'", "multi": '"'}, item.hstore_field)
            else:
                self.assertIsNone(item.hstore_field)

            if item.pk != 11:
                self.assertEqual('test%d' % item.pk, item.name)
            else:
                self.assertEqual('', item.name)
