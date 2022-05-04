from django.test import SimpleTestCase

from django_pg_bulk_update.types import FieldDescriptor
from django_pg_bulk_update.utils import lazy_import


class ImportFieldClassTest(SimpleTestCase):
    def test_class(self):
        cls = lazy_import(FieldDescriptor)
        self.assertEqual(FieldDescriptor, cls)

    def test_invalid_module(self):
        cls = lazy_import('django_pg_bulk_update.invalid_module.InvalidClass')
        self.assertIsNone(cls)

    def test_invalid_class(self):
        cls = lazy_import('django_pg_bulk_update.types.InvalidClass')
        self.assertIsNone(cls)

    def test_valid(self):
        cls = lazy_import('django_pg_bulk_update.types.FieldDescriptor')
        self.assertEqual(FieldDescriptor, cls)
