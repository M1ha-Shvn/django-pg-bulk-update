from unittest import skipIf

import django
from django.db import connection
from django.db.models import AutoField, IntegerField
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

    @skipIf(django.VERSION < (1, 10), "BigAutoField is available since django 1.10")
    def test_big_auto_field(self):
        from django.db.models import BigAutoField
        f = BigAutoField()
        self.assertEqual('bigint', get_field_db_type(f, connection))

    def test_default(self):
        f = FieldWithDefault()
        self.assertEqual('integer', get_field_db_type(f, connection))

