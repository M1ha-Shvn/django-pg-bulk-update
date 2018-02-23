"""
This file contains sample models to use in tests
"""
from django.contrib.postgres.fields import HStoreField, ArrayField
from django.db import models

from django_pg_bulk_update.manager import BulkUpdateManager
from django_pg_bulk_update.compatibility import jsonb_available


class TestModelBase(models.Model):
    class Meta:
        abstract = True

    objects = BulkUpdateManager()

    name = models.CharField(max_length=50, null=True, blank=True)
    int_field = models.IntegerField(null=True, blank=True)
    hstore_field = HStoreField(null=True, blank=True)
    array_field = ArrayField(models.IntegerField(null=True, blank=True))


# JSONB type is available in Postgres 9.4+ only
# JSONField is available in Django 1.9+
if not jsonb_available():
    class TestModel(TestModelBase):
        pass
else:
    from django.contrib.postgres.fields import JSONField

    class TestModel(TestModelBase):
        json_field = JSONField(null=True, blank=True)
