from unittest import skipIf

import django
from django.db import connection
from django.db.models import AutoField, IntegerField
from django.test import TestCase

from django_pg_bulk_update.compatibility import get_field_db_type, get_model_fields
from tests.models import RelationModel, TestModel, UniqueNotPrimary


class FieldWithDefault(IntegerField):
    def db_type(self, connection):
        return 'integer DEFAULT 123'

    def rel_db_type(self, connection):
        return 'integer'


class GetFieldDbTypeTest(TestCase):
    def test_auto_field(self):
        f = AutoField()
        self.assertEqual('integer', get_field_db_type(f, connection))

    @skipIf(django.VERSION < (1, 10), "BigAutoField is available since django 1.10")
    def test_big_auto_field(self):
        from django.db.models import BigAutoField
        f = BigAutoField()
        self.assertEqual('bigint', get_field_db_type(f, connection))

    def test_default(self):
        f = FieldWithDefault()
        self.assertEqual('integer', get_field_db_type(f, connection))


class GetModelFieldsTest(TestCase):
    @staticmethod
    def _get_field_names(model_cls, **kwargs):
        return {f.name for f in get_model_fields(model_cls, **kwargs)}

    def test_simple(self):
        self.assertSetEqual({'id', 'int_field'}, self._get_field_names(UniqueNotPrimary))

    def test_not_concrete(self):
        self.assertSetEqual({'id', 'int_field', 'fk', 'm2m', 'o2o'}, self._get_field_names(RelationModel))

    def test_m2m(self):
        self.assertNotIn('m2m', self._get_field_names(RelationModel, concrete=True))
        self.assertNotIn('m2m', self._get_field_names(TestModel, concrete=True))

    def test_relations(self):
        self.assertIn('fk', self._get_field_names(RelationModel, concrete=True))
        self.assertNotIn('fk', self._get_field_names(TestModel, concrete=True))

    def test_concrete(self):
        self.assertNotIn('m2m', self._get_field_names(RelationModel, concrete=True))
        self.assertNotIn('m2m', self._get_field_names(TestModel, concrete=True))

        self.assertIn('fk', self._get_field_names(RelationModel, concrete=True))
        self.assertNotIn('fk', self._get_field_names(TestModel, concrete=True))
