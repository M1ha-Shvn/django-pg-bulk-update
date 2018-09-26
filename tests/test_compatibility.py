from django.db import connection
from django.db.models import AutoField, BigAutoField, IntegerField
from django.test import TestCase

from django_pg_bulk_update.compatibility import get_field_db_type


class FieldWithDefault(IntegerField):
    def db_type(self, connection):
        return 'integer DEFAULT 123'

    def rel_db_type(self, connection):
        return 'integer'


class GetFieldDbTypeTest(TestCase):
    def test_auto_field(self):
        f = AutoField()
        self.assertEqual('integer', get_field_db_type(f, connection))

    def test_big_auto_field(self):
        f = BigAutoField()
        self.assertEqual('bigint', get_field_db_type(f, connection))

    def test_default(self):
        f = FieldWithDefault()
        self.assertEqual('integer', get_field_db_type(f, connection))

