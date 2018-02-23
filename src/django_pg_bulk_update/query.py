"""
This file contains bulk_update query functions
"""

import inspect
import json
from collections import Iterable, OrderedDict

import six
from django.contrib.postgres.fields import HStoreField
from django.db import transaction, connection, connections, DefaultConnectionProxy
from django.db.models import Model, Q
from typing import Any, Type, Iterable as TIterable, Union, Optional, List, Tuple

from .clause_operators import AbstractClauseOperator, EqualClauseOperator
from .compatibility import zip_longest, hstore_serialize
from .set_functions import EqualSetFunction, AbstractSetFunction
from .types import TOperators, TFieldNames, TUpdateValues, TSetFunctions, TOperatorsValid, TUpdateValuesValid, \
    TSetFunctionsValid


def _validate_field_names(parameter_name, field_names):
    # type: (str, TFieldNames) -> List[str]
    """
    Validates field_names.
    It can be a string for single field or an iterable of strings for multiple fields.
    :param parameter_name: A name of parameter validated to output in exception
    :param field_names: Field names to validate
    :return: A list of strings - formatted field types
    :raises AssertionError: If validation is not passed
    """
    error_message = "'%s' parameter must be iterable of strings" % parameter_name

    if isinstance(field_names, six.string_types):
        return [field_names]
    elif isinstance(field_names, Iterable):
        field_names = list(field_names)
        for name in field_names:
            assert isinstance(name, six.string_types), error_message
        return field_names
    else:
        raise AssertionError(error_message)


def _validate_operators(field_names, operators, param_name='values'):
    # type: (List[str], TOperators, str) -> TOperatorsValid
    """
    Validates operators and gets a dict of database filters with field_name as key
    Order of dict is equal to field_names order
    :param field_names: A list of field_names, already validated
    :param operators: Operations, not validated.
    :param param_name: Name of parameter to output in exception
    :return: An ordered dict of field_name: (db_filter pairs, inverse)
    """
    # Format operations as dictionary by field name
    if isinstance(operators, dict):
        for name in field_names:
            if name not in operators:
                operators[name] = EqualClauseOperator()
    else:
        assert isinstance(operators, Iterable), \
            "'%s' parameter must be iterable of strings or AbstractClauseOperator instances" % param_name
        operators = dict(zip_longest(field_names, operators, fillvalue=EqualClauseOperator()))

    assert len(set(field_names)) == len(set(operators.keys())), "Some operators are not present in %s" % param_name

    res = OrderedDict()
    for name in field_names:
        if isinstance(operators[name], AbstractClauseOperator):
            res[name] = operators[name]
        elif isinstance(operators[name], six.string_types):
            res[name] = AbstractClauseOperator.get_operation_by_name(operators[name])()
        else:
            raise AssertionError("Invalid operator '%s'" % str(operators[name]))

    return res


def _validate_update_values(key_fields, values, param_name='values'):
    # type: (List[str], TUpdateValues, str) -> Tuple[Tuple[str], TUpdateValuesValid]
    """
    Parses and validates input data for bulk_update and bulk_update_or_create.
    It can come in 2 forms:
        + Iterable of dicts. Each dict is update or create data. Each dict must contain all key_fields as keys.
            You can't update key_fields with this format.
        + Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fields: Field names, by which items would be selected, already validated.
    :param values: Input data as given
    :param param_name: Name of parameter containing values to use in exception
    :return: Returns a tuple:
        + A tuple with names of keys to update (which are not in key_fields)
        + A dict, keys are tuples of key_fields values, and values are update_values
    """
    upd_keys_tuple = tuple()
    result = {}
    if isinstance(values, dict):
        for keys, updates in values.items():

            # Single one key can be given as is, not tuple
            if not isinstance(keys, tuple):
                keys = (keys,)

            if len(keys) != len(key_fields):
                raise AssertionError("Length of key tuple is not equal to key_fields length")

            # First element. Let's think, that it's fields are updates
            if not upd_keys_tuple:
                upd_keys_tuple = tuple(sorted(updates.keys()))

            # Not first element. Check that all updates have equal fields
            elif tuple(sorted(updates.keys())) != upd_keys_tuple:
                raise AssertionError("All update data must update same fields")

            # keys may have changed it's format
            result[keys] = updates

    elif isinstance(values, Iterable):
        for item in values:
            assert isinstance(item, dict), "All items of iterable must be dicts"

            if set(key_fields) - set(item.keys()):
                raise AssertionError("One of update items doesn't contain all key fields")

            # First element. Let's think, that it's fields are updates
            if not upd_keys_tuple:
                upd_keys_tuple = tuple(set(item.keys()) - set(key_fields))

            # Not first element. Check that all updates have equal fields
            elif set(upd_keys_tuple) | set(key_fields) != set(item.keys()):
                raise AssertionError("All update data must update same fields")

            # Split into keys and update values
            upd_key_values = []
            for f in key_fields:
                if isinstance(item[f], dict):
                    raise AssertionError("Dict is currently not supported as key field")
                elif isinstance(item[f], Iterable) and not isinstance(item[f], six.string_types):
                    upd_key_values.append(tuple(item[f]))
                else:
                    upd_key_values.append(item[f])
            upd_values = {f: item[f] for f in upd_keys_tuple}
            result[tuple(upd_key_values)] = upd_values

    else:
        raise AssertionError("'%s' parameter must be dict or Iterable" % param_name)

    return upd_keys_tuple, result


def _validate_set_functions(model, upd_keys_tuple, functions, param_name='set_functions'):
    # type: (Type[Model], Tuple[str], TSetFunctions, str) -> TSetFunctionsValid
    """
    Validates set functions.
    It should be a dict with field name as key and function name or AbstractSetFunction instance as value
    Default set function is EqualSetFunction
    :param model: Model updated
    :param upd_keys_tuple: A tuple of field names to update
    :param functions: Functions to validate
    :param param_name: Name of the parameter to use in exception
    :return: A dict with field name as key and AbstractSetFunction instance as value
    """
    functions = functions or {}
    assert isinstance(functions, dict), "'%s' must be a dict instance" % param_name
    upd_keys_set = set(upd_keys_tuple)

    res = {}
    for field_key, func in functions.items():
        assert field_key in upd_keys_set, "'%s' parameter has field name '%s' which will not be updated" \
                                          % (param_name, field_key)
        if isinstance(func, six.string_types):
            set_func = AbstractSetFunction.get_function_by_name(func)()
        elif isinstance(func, AbstractSetFunction):
            set_func = func
        else:
            raise AssertionError("'%s[%s]' parameter must be string or AbstractSetFunction subclass"
                                 % (param_name, field_key))

        field = model._meta.get_field(field_key)
        assert set_func.field_is_supported(field), "'%s' doesn't support '%s' field" \
                                                   % (set_func.__class__.__name__, field_key)

        res[field_key] = set_func

    # Set default function
    for key in upd_keys_set - set(res.keys()):
        res[key] = EqualSetFunction()

    return res


def pdnf_clause(field_names, field_values, operators=()):
    # type: (TFieldNames, TIterable[Union[TIterable[Any], dict]], TOperators) -> Q
    """
    Forms WHERE query condition as Principal disjunctive normal form:
    WHERE (a = x AND b = y AND ...) OR (a = x1 AND b = y1  AND ...) OR ...
    If field_values are not given condition for empty result is returned.
    :param field_names: Iterable of database field names ('a', 'b', ...)
    :param field_values: Field values. A list of tuples ( (x, y), (x1, y1), ...) or dicts ({'a': x, 'b': y}, ...)
    :param operators: Field compare operators.
        It can be dict with field_name as key, operation name as value
        Or an iterable of operations in field_names order.
        The default operator is eq (it will be used for all fields, not set directly).
        Operators: [in, gt, lt, eq, gte, lte, !in, !eq]
        Example: ('eq', 'in') or {'a': 'eq', 'b': 'in'}.
    :return: Django Q-object (it can be used as Model.objects.filter(Q(...))
        https://docs.djangoproject.com/en/2.0/topics/db/queries/#complex-lookups-with-q-objects
    """
    # Validate input data
    field_names = _validate_field_names("field_names", field_names)
    operators = _validate_operators(field_names, operators, param_name='operators')

    assert isinstance(field_values, Iterable), "field_values must be iterable of tuples or dicts"
    field_values = list(field_values)

    if len(field_values) == 0:
        # Empty condition should return empty result
        return ~Q()

    or_cond = Q()
    for values_item in field_values:
        assert isinstance(values_item, (dict, Iterable)), "Each field_values item must be dict or iterable"
        assert len(values_item) == len(field_names), \
            "All field_values must contain all fields from 'field_names' parameter"

        and_cond = Q()
        for i, name in enumerate(field_names):
            if isinstance(values_item, dict):
                assert name in values_item, "field_values dict '%s' doesn't have key '%s'" \
                                            % (json.dumps(values_item), name)
                value = values_item[name]
            elif isinstance(values_item, Iterable):
                values_item = list(values_item)
                value = values_item[i]
            else:
                raise AssertionError("Each field_values item must be dict or iterable")

            op = operators[name]
            kwargs = {op.get_django_filter(name): value}
            and_cond &= ~Q(**kwargs) if op.inverse else Q(**kwargs)

        or_cond |= and_cond

    return or_cond


def _bulk_update_no_validation(model, values, conn, set_functions, key_fields_ops):
    # type: (Type[Model], TUpdateValuesValid, DefaultConnectionProxy, TSetFunctionsValid, TOperatorsValid) -> int
    """
    Does bulk update, skipping parameters validation.
    It is used for speed up in bulk_update_or_create, where parameters are already formatted.
    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values_tuple: update_fields_dict
    :param conn: Database connection used
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function class as value.
    :param key_fields_ops: Key fields compare operators.
        A dict with field_name from key_fields as key, operation name as value
    :return: Number of records updated
    """
    key_fields = list(key_fields_ops.keys())
    upd_keys_tuple = tuple(set_functions.keys())

    # No any values to update. Return that everything is done.
    if not upd_keys_tuple or not values:
        return len(values)

    # Query template. We will form its substitutes in next sections
    query = """
        UPDATE %s AS t SET %s
        FROM (
            VALUES %s
        ) AS sel(%s)
        WHERE %s;
    """

    # Table we save data to
    db_table = model._meta.db_table

    # Form data for VALUES() select.
    # It includes both keys and update data: keys will be used in WHERE section, while update data in SET section
    values_items = []
    values_update_params = []

    # Bug fix. Postgres wants to know exact type of field to save it
    # This fake update value is used for each saved column in order to get it's type
    select_type_query = '(SELECT "{key}" FROM "{table}" LIMIT 0)'
    null_fix_value_item = [select_type_query.format(key=k, table=db_table) for k in upd_keys_tuple]
    null_fix_value_item.extend([key_fields_ops[k].get_null_fix_sql(model, k, conn) for k in key_fields])
    values_items.append(null_fix_value_item)

    for keys, updates in values.items():
        upd_item = []

        # Prepare update fields values
        for name, val in updates.items():
            f = model._meta.get_field(name)
            set_func = set_functions[name]
            item_sql, item_upd_params = set_func.format_field_value(f, val, conn)
            values_update_params.extend(item_upd_params)
            upd_item.append(item_sql)

        # Prepare key fields values
        for name, val in zip(key_fields, keys):
            f = model._meta.get_field(name)
            item_sql, item_upd_params = key_fields_ops[name].format_field_value(f, val, conn)
            values_update_params.extend(item_upd_params)
            upd_item.append(item_sql)

        values_items.append(upd_item)
    values_items_sql = ['(%s)' % ', '.join(item) for item in values_items]

    # NOTE. No extra brackets here or VALUES will return nothing
    values_sql = '%s' % ', '.join(values_items_sql)

    # Form data for AS sel() section
    # Names in key_fields can intersect with upd_keys_tuple and should be prefixed
    sel_items = ["upd__%s" % field_name for field_name in upd_keys_tuple]
    sel_key_items = ["key__%s" % field_name for field_name in key_fields]
    sel_sql = ', '.join(sel_items + sel_key_items)

    # Form data for WHERE section
    # Remember that field names in sel table have prefixes.
    where_items = []
    for key_field, sel_field in zip(key_fields, sel_key_items):
        table_field = '"t"."%s"' % model._meta.get_field(key_field).column
        prefixed_sel_field = '"sel"."%s"' % sel_field
        where_items.append(key_fields_ops[key_field].get_sql(table_field, prefixed_sel_field))
    where_sql = ' AND '.join(where_items)

    # Form data for SET section
    set_items, set_params = [], []
    for field_name, func_cls in set_functions.items():
        f = model._meta.get_field(field_name)
        func_sql, params = func_cls.get_sql(f, '"sel"."upd__%s"' % field_name, conn, val_as_param=False)
        set_items.append(func_sql)
        set_params.extend(params)
    set_sql = ', '.join(set_items)

    # Substitute query placeholders
    query = query % ('"%s"' % db_table, set_sql, values_sql, sel_sql, where_sql)

    # Execute query
    cursor = conn.cursor()
    cursor.execute(query, params=set_params + values_update_params)
    return cursor.rowcount


def bulk_update(model, values, key_fields='id', using=None, set_functions=None, key_fields_ops=()):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, TOperators) -> int
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

    :param model: Model to update, a subclass of django.db.models.Model
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
    :param using: Database alias to make query to.
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
    :return: Number of records updated
    """
    # Validate data
    assert inspect.isclass(model), "model must be django.db.models.Model subclass"
    assert issubclass(model, Model), "model must be django.db.models.Model subclass"
    assert using is None or isinstance(using, six.string_types) and using in connections, \
        "using parameter must be None or existing database alias"

    key_fields = _validate_field_names("key_fields", key_fields)
    upd_keys_tuple, values = _validate_update_values(key_fields, values)
    key_fields_ops = _validate_operators(key_fields, key_fields_ops, param_name='key_fields_ops')
    set_functions = _validate_set_functions(model, upd_keys_tuple, set_functions)
    conn = connection if using is None else connections[using]

    return _bulk_update_no_validation(model, values, conn, set_functions, key_fields_ops)


def bulk_update_or_create(model, values, key_fields='id', using=None, set_functions=None, update=True):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, bool) -> Tuple[int, int]
    """
    Searches for records, given in values by key_fields. If records are found, updates them from values.
    If not found - creates them from values. Note, that all fields without default value must be present in values.

    :param model: Model to update, a subclass of django.db.models.Model
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
    :param using: Database alias to make query to.
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function as value.
        Default function is eq.
        Functions: [eq, =; incr, +; concat, ||]
        Example: {'name': 'eq', 'int_fields': 'incr'}
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple (number of records created, number of records updated)
    """
    # Validate data
    assert inspect.isclass(model), "model must be django.db.models.Model subclass"
    assert issubclass(model, Model), "model must be django.db.models.Model subclass"
    assert using is None or isinstance(using, six.string_types) and using in connections, \
        "using parameter must be None or existing database alias"
    assert type(update) is bool, "update parameter must be boolean"

    key_fields = _validate_field_names("key_fields", key_fields)
    upd_keys_tuple, values = _validate_update_values(key_fields, values)
    set_functions = _validate_set_functions(model, upd_keys_tuple, set_functions)
    conn = connection if using is None else connections[using]

    # bulk_update_or_create searches values by key equality only. No difficult filters here
    key_fields_ops = OrderedDict()
    for key in key_fields:
        key_fields_ops[key] = EqualClauseOperator()

    with transaction.atomic(using=using):
        # Find existing values
        key_items = list(values.keys())
        qs = model.objects.filter(pdnf_clause(key_fields, key_items)).using(using).select_for_update()
        existing_values_dict = {
            tuple([item[key] for key in key_fields]): item
            for item in qs.values()
        }

        # Split into to collections: to create and to update
        create_items, update_items = [], {}
        for key, updates in values.items():
            if key in existing_values_dict:
                # Form a list of updates, if they are enabled
                if update:
                    update_items[key] = updates
            else:
                # Form a list of model objects for bulk_create() method
                kwargs = dict(zip(key_fields, key))
                kwargs.update(updates)

                # Django before 1.10 doesn't convert HStoreField values to string automatically
                # Which causes a bug in cursor.execute(). Let's do it here
                kwargs = {
                    key: hstore_serialize(value) if isinstance(model._meta.get_field(key), HStoreField) else value
                    for key, value in kwargs.items()
                }

                create_items.append(model(**kwargs))

        # Update existing records
        updated = _bulk_update_no_validation(model, update_items, conn, set_functions, key_fields_ops)

        # Create abscent records
        created = len(model.objects.db_manager(using).bulk_create(create_items))

    return created, updated
