from datetime import timedelta, date, datetime
import pytz
from unittest import skipIf

from django.test import TestCase
from django.utils.timezone import now

from django_pg_bulk_update.compatibility import jsonb_available, hstore_available, array_available
from django_pg_bulk_update.query import bulk_create
from django_pg_bulk_update.set_functions import ConcatSetFunction
from tests.models import TestModel, UpperCaseModel, AutoNowModel, TestModelWithSchema


class TestInputFormats(TestCase):
    fixtures = ['test_model']

    def test_model(self):
        with self.assertRaises(TypeError):
            bulk_create(123, [])

        with self.assertRaises(TypeError):
            bulk_create('123', [])

    def test_values(self):
        with self.assertRaises(TypeError):
            bulk_create(TestModel, 123)

        with self.assertRaises(TypeError):
            bulk_create(TestModel, [123])

        with self.assertRaises(TypeError):
            bulk_create(TestModel, {(1, 2): {'id': 10}})

        self.assertEqual(1, bulk_create(TestModel, [{'name': 'abc'}]))
        self.assertEqual(1, bulk_create(TestModel, [{'name': 'abc', 'int_field': 2}]))

    def test_using(self):
        values = [{
            'name': 'bulk_update_1'
        }]

        self.assertEqual(1, bulk_create(TestModel, values))
        self.assertEqual(1, bulk_create(TestModel, values, using='default'))

        with self.assertRaises(ValueError):
            bulk_create(TestModel, values, using='invalid')

        with self.assertRaises(TypeError):
            bulk_create(TestModel, values, using=123)

    def test_set_functions(self):
        with self.assertRaises(TypeError):
            bulk_create(TestModel, [{'name': 'test1'}], set_functions=123)

        with self.assertRaises(TypeError):
            bulk_create(TestModel, [{'name': 'test1'}], set_functions=[123])

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'name': 'test1'}], set_functions={1: 'test'})

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'name': 'test1'}], set_functions={'id': 1})

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'name': 'test1'}], set_functions={'invalid': 1})

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'int_field': 1}], set_functions={'int_field': 'invalid'})

        # I don't test all set functions here, as there is another TestCase for this: TestSetFunctions
        self.assertEqual(1, bulk_create(TestModel, [{'name': 'test1'}],
                                        set_functions={'name': ConcatSetFunction()}))
        self.assertEqual(1, bulk_create(TestModel, [{'name': 'test1'}], set_functions={'name': '||'}))

    def test_batch(self):
        with self.assertRaises(TypeError):
            bulk_create(TestModel, [{'id': 100, 'name': 'test1'}], batch_size='abc')

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'id': 101, 'name': 'test1'}], batch_size=-2)

        with self.assertRaises(TypeError):
            bulk_create(TestModel, [{'id': 102, 'name': 'test1'}], batch_size=2.5)

        with self.assertRaises(TypeError):
            bulk_create(TestModel, [{'id': 103, 'name': 'test1'}], batch_size=1, batch_delay='abc')

        with self.assertRaises(ValueError):
            bulk_create(TestModel, [{'id': 104, 'name': 'test1'}], batch_size=1, batch_delay=-2)


class TestSimple(TestCase):
    fixtures = ['test_model', 'test_upper_case_model']
    multi_db = True
    databases = ['default', 'secondary']

    def test_create(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_create_11'
        }, {
            'id': 12,
            'name': 'bulk_create_12'
        }, {
            'id': 13,
            'name': 'bulk_create_13'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_create_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_auto_id(self):
        res = bulk_create(TestModel, [{
            'name': 'bulk_create'
        }, {
            'name': 'bulk_create'
        }, {
            'name': 'bulk_create'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk > 9:
                self.assertEqual('bulk_create', name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_upper_case(self):
        res = bulk_create(UpperCaseModel, [{
            'id': 11,
            'UpperCaseName': 'BulkUpdate11'
        }, {
            'id': 12,
            'UpperCaseName': 'BulkUpdate12'
        }, {
            'id': 13,
            'UpperCaseName': 'BulkUpdate13'
        }])
        self.assertEqual(3, res)

        # 3 from fixture + 3 created
        self.assertEqual(6, UpperCaseModel.objects.all().count())

        for pk, name in UpperCaseModel.objects.all().order_by('id').values_list('id', 'UpperCaseName'):
            if pk > 10:
                self.assertEqual('BulkUpdate%d' % pk, name)
            else:
                self.assertEqual('test%d' % pk, name)

    def test_empty(self):
        res = bulk_create(TestModel, [])
        self.assertEqual(0, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)

    def test_quotes(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': '\''
        }, {
            'id': 12,
            'name': '"'
        }])
        self.assertEqual(2, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk == 11:
                self.assertEqual('\'', name)
            elif pk == 12:
                self.assertEqual('"', name)
            else:
                self.assertEqual('test%d' % pk, name)

            if pk > 10:
                self.assertIsNone(int_field)
            else:
                self.assertEqual(pk, int_field)

    def test_using(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }], using='secondary')
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().using('secondary').count())
        self.assertEqual(9, TestModel.objects.all().using('default').count())

        for pk, name, int_field in TestModel.objects.all().using('secondary').order_by('id'). \
                values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)

    def test_batch(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }], batch_size=1)
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

        # Test for empty values correct
        res = bulk_create(TestModel, [], batch_size=10)
        self.assertEqual(0, res)

    def test_returning(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }], returning=('id', 'name', 'int_field'))

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (11, 'bulk_update_11', None),
            (12, 'bulk_update_12', None),
            (13, 'bulk_update_13', None),
        }, set(res.values_list('id', 'name', 'int_field')))

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_returning_all(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }], returning='*')

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (11, 'bulk_update_11', None),
            (12, 'bulk_update_12', None),
            (13, 'bulk_update_13', None),
        }, set(res.values_list('id', 'name', 'int_field')))

    def test_returning_empty(self):
        res = bulk_create(TestModel, [], returning='id')
        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertEqual(0, res.count())

    def test_auto_now(self):
        res = bulk_create(AutoNowModel, [{
            'id': 11
        }])
        self.assertEqual(1, res)
        instance = AutoNowModel.objects.get(pk=11)
        self.assertGreaterEqual(instance.created, now() - timedelta(seconds=1))
        self.assertLessEqual(instance.created, now() + timedelta(seconds=1))
        self.assertEqual(instance.updated, datetime.now(pytz.utc).date())
        self.assertIsNone(instance.checked)

    def test_quoted_table_name(self):
        # Test for https://github.com/M1ha-Shvn/django-pg-bulk-update/issues/63
        self.assertEqual(1, bulk_create(TestModelWithSchema, [{'name': 'abc'}]))


class TestReadmeExample(TestCase):
    def test_example(self):
        # Skip bulk_update and bulk_update_or_create sections (tested in other test)
        res = TestModel.objects.pg_bulk_create([
            {'id': i, 'name': "item%d" % i, 'int_field': 1}
            for i in range(1, 4)
        ])
        self.assertEqual(3, res)

        self.assertListEqual([
            {"id": 1, "name": "item1", "int_field": 1},
            {"id": 2, "name": "item2", "int_field": 1},
            {"id": 3, "name": "item3", "int_field": 1}
        ], list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))

        res = TestModel.objects.pg_bulk_create([
            {'id': i, 'name': "item%d" % i, 'int_field': 1} for i in range(4, 6)
        ], returning='*')

        from django_pg_returning import ReturningQuerySet
        self.assertIsInstance(res, ReturningQuerySet)
        self.assertSetEqual({
            (4, "item4", 1),
            (5, "item5", 1)
        }, set(res.values_list('id', 'name', 'int_field')))


class TestSetFunctions(TestCase):
    """
    I don't test most part of library's set functions as they are useless in bulk_create.
    Just some of them, as example
    """
    fixtures = ['test_model']

    def test_incr(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'int_field': 11
        }], set_functions={'int_field': '+'})
        self.assertEqual(1, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual(pk, int_field)
            if pk == 11:
                self.assertEqual('', name)
            else:
                self.assertEqual('test%d' % pk, name)

    def test_concat_str(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': 'bulk_update_11'
        }], set_functions={'name': '||'})
        self.assertEqual(1, res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk == 11:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_eq_not_null(self):
        res = bulk_create(TestModel, [{
            'id': 11,
            'name': None
        }], set_functions={'name': 'eq_not_null'})
        self.assertEqual(1, res)

        # 9 from fixture + 1 created
        self.assertEqual(10, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk == 11:
                # Default name, not None, look https://github.com/M1hacka/django-pg-bulk-update/issues/2
                self.assertEqual('', name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_now(self):
        res = bulk_create(AutoNowModel, [{
            'id': 1
        }], set_functions={'checked': 'now'})
        self.assertEqual(1, res)
        instance = AutoNowModel.objects.get(pk=1)
        self.assertGreaterEqual(instance.checked, now() - timedelta(seconds=1))
        self.assertLessEqual(instance.checked, now() + timedelta(seconds=1))


class TestManager(TestCase):
    fixtures = ['test_model']
    multi_db = True
    databases = ['default', 'secondary']

    def test_bulk_create(self):
        res = TestModel.objects.pg_bulk_create([{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().count())

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

    def test_using(self):
        res = TestModel.objects.db_manager('secondary').pg_bulk_create([{
            'id': 11,
            'name': 'bulk_update_11'
        }, {
            'id': 12,
            'name': 'bulk_update_12'
        }, {
            'id': 13,
            'name': 'bulk_update_13'
        }])
        self.assertEqual(3, res)

        # 9 from fixture + 3 created
        self.assertEqual(12, TestModel.objects.all().using('secondary').count())
        self.assertEqual(9, TestModel.objects.all().using('default').count())

        for pk, name, int_field in TestModel.objects.all().using('secondary').order_by('id'). \
                values_list('id', 'name', 'int_field'):
            if pk > 10:
                self.assertEqual('bulk_update_%d' % pk, name)
                self.assertIsNone(int_field)
            else:
                self.assertEqual('test%d' % pk, name)
                self.assertEqual(pk, int_field)

        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            self.assertEqual('test%d' % pk, name)
            self.assertEqual(pk, int_field)


class TestFieldTypes(TestCase):
    fixtures = ['test_model']

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_array(self):
        res = bulk_create(TestModel, [{'id': 11, 'array_field': [11]},
                                      {'id': 12, 'array_field': []}])
        self.assertEqual(2, res)
        for pk, name, array_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'array_field'):
            if pk == 11:
                self.assertListEqual([pk], array_field)
            elif pk == 12:
                self.assertListEqual([], array_field)
            else:
                self.assertIsNone(array_field)

            if pk > 10:
                self.assertEqual('', name)
            else:
                self.assertEqual('test%d' % pk, name)

    @skipIf(not jsonb_available(), "JSONB type is available in Postgres 9.4+ and django 1.9+ only")
    def test_jsonb(self):
        res = bulk_create(TestModel, [{'id': 11, 'json_field': {'test': '11'}},
                                      {'id': 12, 'json_field': {}},
                                      {'id': 13, 'json_field': {'single': "'", "multi": '"'}}])
        self.assertEqual(3, res)
        for pk, name, json_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'json_field'):
            if pk == 11:
                self.assertDictEqual({'test': str(pk)}, json_field)
            elif pk == 12:
                self.assertDictEqual({}, json_field)
            elif pk == 13:
                self.assertDictEqual({'single': "'", "multi": '"'}, json_field)
            else:
                self.assertIsNone(json_field)

            if pk > 10:
                self.assertEqual('', name)
            else:
                self.assertEqual('test%d' % pk, name)

    @skipIf(not hstore_available(), "HStoreField is available in Django 1.8+")
    def test_hstore(self):
        res = bulk_create(TestModel, [{'id': 11, 'hstore_field': {'test': '11'}},
                                      {'id': 12, 'hstore_field': {}},
                                      {'id': 13, 'hstore_field': {'single': "'", "multi": '"'}}])
        self.assertEqual(3, res)
        for item in TestModel.objects.all().order_by('id'):
            if item.pk == 11:
                self.assertDictEqual({'test': str(item.pk)}, item.hstore_field)
            elif item.pk == 12:
                self.assertDictEqual({}, item.hstore_field)
            elif item.pk == 13:
                self.assertDictEqual({'single': "'", "multi": '"'}, item.hstore_field)
            else:
                self.assertIsNone(item.hstore_field)

            if item.pk > 10:
                self.assertEqual('', item.name)
            else:
                self.assertEqual('test%d' % item.pk, item.name)
