"""
This file contains django manager and a mixin to integrate library with your models

Examples:
    from django.db import models
    from django_pg_bulk_update import

    # Simple manager
    class TestModel(models.Model):
        objects = BulkUpdateManager()

    # Custom manager
    class CustomManager(models.Manager, BulkUpdateManagerMixin):
        pass  # Your functionality here

    class TestModel(models.Model):
        objects = CustomManager()
"""
from django.db import models
from typing import Tuple, Optional, Iterable

from django.db.models.manager import BaseManager

from .types import TUpdateValues, TFieldNames, TSetFunctions, TOperators
from .query import bulk_update, bulk_update_or_create


class BulkUpdateMixin:
    """
    A mixin, adding bulk updates methods to any django manager
    It automatically fetches using and model parameters from manager.
    You can set database alias to use directly by db_manager() method
    """
    def bulk_update(self, values, key_fields='id', set_functions=None, key_fields_ops=(), returning=None,
                    batch_size=None, batch_delay=0):
        # type: (TUpdateValues, TFieldNames, TSetFunctions, TOperators, Optional[Iterable[str]], Optional[int], float) -> int
        """
        Updates multiple records of a given model, finding them by key_fields.

        Example:
        # Test model
        class TestModel(models.Model):
            name = models.CharField(max_length=50)
            int_field = models.IntegerField(default=1)

        # Create test data
        TestModel.objects.bulk_create([TestModel(pk=i, name="item%d" % i) for i in range(1, 4)])

        # Call update. Does only 1 database query.
        updated = bulk_update(TestModel, {
            "item1": {
                "name": "updated1",
                "int_field": 1
            },
            "item2": {
                "name": "updated2",
                "int_field": 2
            }
        }, key_fields="name")

        print(updated)
        # Outputs: 2

        print(list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))
        # Outputs: [
        #     {"id": 1, "name": "updated1", "int_field": 2},
        #     {"id": 2, "name": "updated1", "int_field": 3},
        #     {"id": 3, "name": "item3", "int_field": 0}
        # ]

        :param values: Data to update. All items must update same fields!!!
            It can come in 2 forms:
            + Iterable of dicts. Each dict is update or create data. Each dict must contain all key_fields as keys.
                You can't update key_fields with this format.
            + Dict of key_values: update_fields_dict
                - key_values can be iterable or single object.
                - If iterable, key_values length must be equal to key_fields length.
                - If single object, key_fields is expected to have 1 element
        :param key_fields: Field names, by which items would be selected.
            It can be a string, if there's only one key field or iterable of strings for multiple keys
        :param set_functions: Functions to set values.
            Should be a dict of field name as key, function as value.
            Default function is eq.
            Functions: [eq, =; incr, +; concat, ||]
            Example: {'name': 'eq', 'int_fields': 'incr'}
        :param key_fields_ops: Key fields compare operators.
            It can be dict with field_name from key_fields as key, operation name as value
            Or an iterable of operations in key_fields order.
            The default operator is eq (it will be used for all fields, not set directly).
            Operators: [in; !in; gt, >; lt, <; gte, >=; lte, <=; !eq, <>, !=; eq, =, ==]
            Example: ('eq', 'in') or {'a': 'eq', 'b': 'in'}.
        :param returning: Optional. If given, returns updated values of fields, listed in parameter.
        :param batch_size: Optional. If given, data is split it into batches of given size.
            Each batch is queried independently.
        :param batch_delay: Delay in seconds between batches execution, if batch_size is not None.
        :return: Number of records updated
        """
        self._for_write = True
        using = self.db

        if getattr(self, '_lookup_joins', False):
            raise Exception('joins in lookups are restricted in bulk update methods')

        where = getattr(self, 'where', None)

        return bulk_update(self.model, values, key_fields=key_fields, using=using, set_functions=set_functions,
                           key_fields_ops=key_fields_ops, where=where, returning=returning,
                           batch_size=batch_size, batch_delay=batch_delay)

    def bulk_update_or_create(self, values, key_fields='id', set_functions=None, update=True, key_is_unique=True,
                              returning=None, batch_size=None, batch_delay=0):
        # type: (TUpdateValues, TFieldNames, TSetFunctions, bool, bool, Optional[Iterable[str]], Optional[int], float) -> int
        """
        Searches for records, given in values by key_fields. If records are found, updates them from values.
        If not found - creates them from values. Note, that all fields without default value must be present in values.

        :param values: Data to update. All items must update same fields!!!
            It can come in 2 forms:
            + Iterable of dicts. Each dict is update or create data. Each dict must contain all key_fields as keys.
                You can't update key_fields with this format.
            + Dict of key_values: update_fields_dict
                - key_values can be iterable or single object.
                - If iterable, key_values length must be equal to key_fields length.
                - If single object, key_fields is expected to have 1 element
        :param key_fields: Field names, by which items would be selected.
            It can be a string, if there's only one key field or iterable of strings for multiple keys
        :param set_functions: Functions to set values.
            Should be a dict of field name as key, function as value.
            Default function is eq.
            Functions: [eq, =; incr, +; concat, ||]
            Example: {'name': 'eq', 'int_fields': 'incr'}
        :param update: If this flag is not set, existing records will not be updated
        :param key_is_unique: Settings this flag to False forces library to use 3-query transactional update,
            not INSERT ... ON CONFLICT.
        :param returning: Optional. If given, returns updated values of fields, listed in parameter.
        :param batch_size: Optional. If given, data is split it into batches of given size.
            Each batch is queried independently.
        :param batch_delay: Delay in seconds between batches execution, if batch_size is not None.
        :return: Number of records created or updated
        """
        self._for_write = True
        using = self.db

        return bulk_update_or_create(self.model, values, key_fields=key_fields, using=using,
                                     set_functions=set_functions, update=update, key_is_unique=key_is_unique,
                                     returning=returning, batch_size=batch_size, batch_delay=batch_delay)


class BulkUpdateQuerySet(models.QuerySet, BulkUpdateMixin):
    pass


class BulkUpdateManager(models.Manager, BulkUpdateMixin):
    """
    A manager, adding bulk update methods to any model
    """
    pass


# DEPRECATED, for back compatibility
BulkUpdateManagerMixin = BulkUpdateMixin
