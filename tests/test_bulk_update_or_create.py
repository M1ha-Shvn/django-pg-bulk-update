from unittest import skipIf

from django.test import TestCase

from django_pg_bulk_update.compatibility import jsonb_available, array_available, hstore_available
from django_pg_bulk_update.query import bulk_update_or_create
from django_pg_bulk_update.set_functions import ConcatSetFunction
from tests.models import TestModel


class TestInputFormats(TestCase):
    fixtures = ['test_model']

    def test_model(self):
        with self.assertRaises(AssertionError):
            bulk_update_or_create(123, [])

        with self.assertRaises(AssertionError):
            bulk_update_or_create('123', [])

    def test_values(self):
        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, 123)

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [123])

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, {(1, 2): {'id': 10}})

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, {1: {'id': 10}}, key_fields=('id', 'name'))

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'name': 'test'}])

        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'abc'}, {'id': 21, 'name': 'create'}]))

        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'abc', 'int_field': 2},
                        {'id': 20, 'name': 'abc', 'int_field': 3}], key_fields=('id', 'name')))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, {1: {'name': 'abc'}, 19: {'name': 'created'}}))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, {(1,): {'name': 'abc'}, (18,): {'name': 'created'}}))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, {(2, 'test2'): {'int_field': 2}, (17, 'test2'): {'int_field': 4}}, key_fields=('id', 'name')))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, {('test33',): {'int_field': 2}, ('test3',): {'int_field': 2}}, key_fields='name'))

    def test_key_fields(self):
        values = [{
            'id': 1,
            'name': 'bulk_update_or_create_1'
        }, {
            'id': 10,
            'name': 'bulk_update_or_create_2'
        }]

        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values))
        values[1]['id'] += 1
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, key_fields='id'))
        values[1]['id'] += 1
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, key_fields=['id']))
        values[1]['id'] += 1
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, key_fields=['id', 'name']))
        values[1]['id'] += 1
        values[1]['name'] += '1'
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, key_fields='name'))
        values[1]['id'] += 1
        values[1]['name'] += '1'
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, key_fields=['name']))

    def test_using(self):
        values = [{
            'id': 1,
            'name': 'bulk_update_or_create_1'
        }, {
            'id': 10,
            'name': 'bulk_update_or_create_2'
        }]

        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values))
        values[1]['id'] += 1
        self.assertTupleEqual((1, 1), bulk_update_or_create(TestModel, values, using='default'))

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, values, using='invalid')

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, values, using=123)

    def test_set_functions(self):
        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions=123)

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions=[123])

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={1: 'test'})

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={'id': 1})

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={'invalid': 1})

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], set_functions={'int_field': 'invalid'})

        # int_field is not in update keys here, set_function
        with self.assertRaises(AssertionError):
            self.assertEqual(1, bulk_update_or_create(TestModel, [{'id': 2, 'name': 'test1'}],
                                                      set_functions={'int_field': '+'}))

        # I don't test all set functions here, as there is another TestCase for this: TestSetFunctions
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, [{'id': 2, 'name': 'test1'}, {'id': 10, 'name': 'test1'}],
            set_functions={'name': ConcatSetFunction()}
        ))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, [{'id': 2, 'name': 'test1'}, {'id': 11, 'name': 'test1'}], set_functions={'name': '||'}))

    def test_update(self):
        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], update=123)

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': ['test1']}], update='123')

        self.assertTupleEqual((1, 0), bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'test30'}, {'id': 20, 'name': 'test30'}], update=False))
        self.assertTupleEqual((1, 1), bulk_update_or_create(
            TestModel, [{'id': 1, 'name': 'test30'}, {'id': 19, 'name': 'test30'}], update=True))

    def test_batch(self):
        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size='abc')

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=-2)

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=2.5)

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=1, batch_delay='abc')

        with self.assertRaises(AssertionError):
            bulk_update_or_create(TestModel, [{'id': 1, 'name': 'test1'}], batch_size=1, batch_delay=-2)


class TestSimple(TestCase):
    fixtures = ['test_model']
    multi_db = True

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
        self.assertTupleEqual((1, 2), res)

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

    def test_empty(self):
        res = bulk_update_or_create(TestModel, [])
        self.assertTupleEqual((0, 0), res)
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
        self.assertTupleEqual((1, 1), res)
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
            ('test1',): {
                'id': 1,
                'name': 'bulk_update_1'
            },
            ('test5',): {
                'id': 5,
                'name': 'bulk_update_5'
            },
            ('bulk_update_11',): {
                'id': 11,
                'name': 'bulk_update_11'
            }
        }, key_fields='name')
        self.assertTupleEqual((1, 2), res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5, 11}:
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
        self.assertTupleEqual((1, 2), res)

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
        self.assertTupleEqual((1, 2), res)

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
        self.assertTupleEqual((0, 0), res)


class TestReadmeExample(TestCase):
    def test_example(self):
        # Skip bulk_update section (tested in other test), and init data as bulk_update_or_create start
        TestModel.objects.bulk_create([
            TestModel(pk=1, name="updated1", int_field=2),
            TestModel(pk=2, name="updated2", int_field=3),
            TestModel(pk=3, name="incr", int_field=4),
        ])

        inserted, updated = bulk_update_or_create(TestModel, [{
            "id": 3,
            "name": "_concat1",
            "int_field": 4
        }, {
            "id": 4,
            "name": "concat2",
            "int_field": 5
        }], set_functions={'name': '||'})
        self.assertEqual(1, inserted)
        self.assertEqual(1, updated)

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
        self.assertTupleEqual((1, 2), res)
        for pk, name, int_field in TestModel.objects.all().order_by('id').values_list('id', 'name', 'int_field'):
            if pk in {1, 5}:
                self.assertEqual(2 * pk, int_field)
            else:
                self.assertEqual(pk, int_field)

            if pk != 11:
                self.assertEqual('test%d' % pk, name)
            else:
                self.assertIsNone(name)

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
        self.assertTupleEqual((1, 2), res)
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
        self.assertTupleEqual((1, 3) if iteration == 1 else (0, 4), res)
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
                self.assertIsNone(name)

    @skipIf(not array_available(), "ArrayField is available in Django 1.8+")
    def test_concat_array(self):
        for i in range(1, 5):
            res = bulk_update_or_create(TestModel, [{'id': 1, 'array_field': [1]},
                                                    {'id': 2, 'array_field': [2]},
                                                    {'id': 11, 'array_field': [11]},
                                                    {'id': 4, 'array_field': []}], set_functions={'array_field': '||'})
            self._test_concat_array(i, res)

    def _test_concat_dict(self, iteration, res, field_name, val_as_str=False):
        self.assertTupleEqual((1, 3) if iteration == 1 else (0, 4), res)
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
                self.assertIsNone(name)

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


class TestManager(TestCase):
    fixtures = ['test_model']
    multi_db = True

    def test_bulk_update_or_create(self):
        res = TestModel.objects.bulk_update_or_create([{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }])
        self.assertTupleEqual((1, 2), res)

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
        res = TestModel.objects.db_manager('secondary').bulk_update_or_create([{
            'id': 1,
            'name': 'bulk_update_1'
        }, {
            'id': 5,
            'name': 'bulk_update_5'
        }, {
            'id': 11,
            'name': 'bulk_update_11'
        }])
        self.assertTupleEqual((1, 2), res)

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
        self.assertTupleEqual((1, 3), res)
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
                self.assertIsNone(name)

    @skipIf(not jsonb_available(), "JSONB type is available in Postgres 9.4+ and django 1.9+ only")
    def test_jsonb(self):
        res = bulk_update_or_create(TestModel, [{'id': 1, 'json_field': {'test': '1'}},
                                                {'id': 2, 'json_field': {'test': '2'}},
                                                {'id': 11, 'json_field': {'test': '11'}},
                                                {'id': 4, 'json_field': {}},
                                                {'id': 5, 'json_field': {'single': "'", "multi": '"'}}])
        self.assertTupleEqual((1, 4), res)
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
                self.assertIsNone(name)

    @skipIf(not hstore_available(), "HStoreField is available in Django 1.8+")
    def test_hstore(self):
        res = bulk_update_or_create(TestModel, [{'id': 1, 'hstore_field': {'test': '1'}},
                                                {'id': 2, 'hstore_field': {'test': '2'}},
                                                {'id': 11, 'hstore_field': {'test': '11'}},
                                                {'id': 4, 'hstore_field': {}},
                                                {'id': 5, 'hstore_field': {'single': "'", "multi": '"'}}])
        self.assertTupleEqual((1, 4), res)
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
                self.assertIsNone(item.name)
