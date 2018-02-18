"""
This file contains sample models to use in tests
"""
from django.contrib.postgres.fields import HStoreField, JSONField, ArrayField
from django.db import models

from django_pg_bulk_update import BulkUpdateManager
from django_pg_bulk_update.utils import get_postgres_version


class TestModelBase(models.Model):
    class Meta:
        abstract = True

    objects = BulkUpdateManager()

    name = models.CharField(max_length=50, null=True, blank=True)
    int_field = models.IntegerField(null=True, blank=True)
    hstore_field = HStoreField(null=True, blank=True)
    array_field = ArrayField(models.IntegerField(null=True, blank=True))


# JSONB type is available in PostgreSQL 9.4+ only
if get_postgres_version(as_tuple=False) < 90400:
    class TestModel(TestModelBase):
        pass
else:
    class TestModel(TestModelBase):
        json_field = JSONField(null=True, blank=True)
