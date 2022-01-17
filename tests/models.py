"""
This file contains sample models to use in tests
"""
import uuid
from django.db import models

from django_pg_bulk_update.manager import BulkUpdateManager
from django_pg_bulk_update.compatibility import jsonb_available, hstore_available, array_available, \
    import_pg_field_or_dummy


class Meta:
    unique_together = ['id', 'name']


# Not all fields are available in different django and postgres versions
model_attrs = {
    'name': models.CharField(max_length=50, null=True, blank=True, default=''),
    'int_field': models.IntegerField(null=True, blank=True),
    'objects': BulkUpdateManager(),
    'Meta': Meta,
    '__module__': __name__
}

if array_available():
    from django.contrib.postgres.fields import ArrayField
    model_attrs['array_field'] = ArrayField(models.IntegerField(null=True, blank=True))
    model_attrs['big_array_field'] = ArrayField(models.BigIntegerField(), default=list)

if hstore_available():
    from django.contrib.postgres.fields import HStoreField
    model_attrs['hstore_field'] = HStoreField(null=True, blank=True)

if jsonb_available():
    JSONField = import_pg_field_or_dummy('JSONField', jsonb_available)
    model_attrs['json_field'] = JSONField(null=True, blank=True)

TestModel = type('TestModel', (models.Model,), model_attrs)


class MetaWithSchema:
    db_table = '"appschema"."testmodel"'
    unique_together = ['id', 'name']


class TestModelWithSchema(models.Model):
    """
    Test model for https://github.com/M1ha-Shvn/django-pg-bulk-update/issues/63
    """
    class Meta:
        db_table = '"appschema"."testmodel"'
        unique_together = ['id', 'name']

    name = models.CharField(max_length=50, null=True, blank=True, default='')
    int_field = models.IntegerField(null=True, blank=True)
    objects = BulkUpdateManager()


class UpperCaseModel(models.Model):
    """
    Test model for https://github.com/M1hacka/django-pg-bulk-update/issues/46
    """
    UpperCaseName = models.CharField(max_length=30)


class UniqueNotPrimary(models.Model):
    """
    Test model for https://github.com/M1hacka/django-pg-bulk-update/issues/19
    """
    int_field = models.IntegerField(unique=True)


class RelationModel(models.Model):
    """
    Test model for https://github.com/M1hacka/django-pg-bulk-update/issues/36
    """
    int_field = models.IntegerField()
    m2m = models.ManyToManyField(TestModel)
    fk = models.ForeignKey(TestModel, on_delete=models.CASCADE, related_name='fk')
    o2o = models.OneToOneField(TestModel, on_delete=models.CASCADE, related_name='o2o')


class AutoNowModel(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateField(auto_now=True)
    checked = models.DateTimeField(null=True, blank=True)


class UUIDFieldPrimaryModel(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    key_field = models.IntegerField(unique=True)
    int_field = models.IntegerField(default=1)
