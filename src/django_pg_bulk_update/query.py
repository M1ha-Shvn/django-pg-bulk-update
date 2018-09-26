"""
This file contains bulk_update query functions
"""

import inspect
import json
from collections import Iterable
from itertools import chain
from logging import getLogger

import six
from django.db import transaction, connection, connections, DefaultConnectionProxy
from django.db.models import Model, Q, AutoField
from typing import Any, Type, Iterable as TIterable, Union, Optional, List, Tuple

from .compatibility import get_postgres_version, get_model_fields
from .set_functions import AbstractSetFunction
from .types import TOperators, TFieldNames, TUpdateValues, TSetFunctions, TOperatorsValid, TUpdateValuesValid, \
    TSetFunctionsValid, FieldDescriptor
from .utils import batched_operation


__all__ = ['pdnf_clause', 'bulk_update', 'bulk_update_or_create']
logger = getLogger('django-pg-bulk-update')


def _validate_field_names(field_names):
    # type: (TFieldNames) -> Tuple[FieldDescriptor]
    """
    Validates field_names.
    It can be a string for single field or an iterable of strings for multiple fields.
    :param field_names: Field names to validate
    :return: A tuple of strings - formatted field types
    :raises AssertionError: If validation is not passed
    """
    error_message = "'key_fields' parameter must be iterable of strings"

    if isinstance(field_names, six.string_types):
        return FieldDescriptor(field_names),  # comma is not a bug, I need tuple returned
    elif isinstance(field_names, Iterable):
        field_names = list(field_names)
        for name in field_names:
            if not isinstance(name, six.string_types):
                raise TypeError(error_message)
        return tuple(FieldDescriptor(name) for name in field_names)
    else:
        raise TypeError(error_message)


def _validate_operators(key_fds, operators):
    # type: (Tuple[FieldDescriptor], TOperators) -> TOperatorsValid
    """
    Validates operators and gets a dict of database filters with field_name as key
    Order of dict is equal to field_names order
    :param key_fds: A tuple of FieldDescriptor objects. These objects will be modified.
    :param operators: Operations, not validated.
    :return: A tuple of field descriptors with operators
    """
    # Format operators (field name, AbstractClauseOperator())
    if isinstance(operators, dict):
        if len(set(operators.keys()) - {f.name for f in key_fds}) != 0:
            raise ValueError("Some operators are not present in 'key_field_ops'")
        for fd in key_fds:
            fd.key_operator = operators.get(fd.name)
    else:
        if not isinstance(operators, Iterable):
            raise TypeError("'key_field_ops' parameter must be iterable of strings or AbstractClauseOperator instances")
        operators = tuple(operators)
        for i, fd in enumerate(key_fds):
            fd.key_operator = operators[i] if i < len(operators) else None

    # Add prefix to all descriptors
    for i, fd in enumerate(key_fds):
        fd.set_prefix('key', index=i)

    return key_fds


def _validate_update_values(key_fds, values):
    # type: (Tuple[FieldDescriptor], TUpdateValues) -> Tuple[Tuple[FieldDescriptor], TUpdateValuesValid]
    """
    Parses and validates input data for bulk_update and bulk_update_or_create.
    It can come in 2 forms:
        + Iterable of dicts. Each dict is update or create data. Each dict must contain all key_fields as keys.
            You can't update key_fields with this format.
        + Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fds: A tuple of FieldDescriptor objects, by which data will be selected
    :param values: Input data as given
    :return: Returns a tuple:
        + A tuple with FieldDescriptor objects to update (which are not in key_field_descriptors)
        + A dict, keys are tuples of key_fields values, and values are update_values
    """
    upd_keys_tuple = tuple()
    result = {}
    if isinstance(values, dict):
        for keys, updates in values.items():

            # Single one key can be given as is, not tuple
            if not isinstance(keys, tuple):
                keys = (keys,)

            if len(keys) != len(key_fds):
                raise ValueError("Length of key tuple is not equal to key_fields length")

            # First element. Let's think, that it's fields are updates
            if not upd_keys_tuple:
                upd_keys_tuple = tuple(sorted(updates.keys()))

            # Not first element. Check that all updates have equal fields
            elif tuple(sorted(updates.keys())) != upd_keys_tuple:
                raise ValueError("All update data must update same fields")

            # keys may have changed it's format
            result[keys] = updates

    elif isinstance(values, Iterable):
        for item in values:
            if not isinstance(item, dict):
                raise TypeError("All items of iterable must be dicts")

            # First element. Let's think, that it's fields are updates
            key_field_names = {f.name for f in key_fds}
            if key_field_names - set(item.keys()):
                raise ValueError("One of update items doesn't contain all key fields")

            if not upd_keys_tuple:
                upd_keys_tuple = tuple(set(item.keys()) - key_field_names)

            # Not first element. Check that all updates have equal fields
            elif set(upd_keys_tuple) | key_field_names != set(item.keys()):
                raise ValueError("All update data must update same fields")

            # Split into keys and update values
            upd_key_values = []
            for fd in key_fds:
                if isinstance(item[fd.name], dict):
                    raise TypeError("Dict is currently not supported as key field")
                elif isinstance(item[fd.name], Iterable) and not isinstance(item[fd.name], six.string_types):
                    upd_key_values.append(tuple(item[fd.name]))
                else:
                    upd_key_values.append(item[fd.name])
            upd_values = {f: item[f] for f in upd_keys_tuple}
            result[tuple(upd_key_values)] = upd_values

    else:
        raise TypeError("'values' parameter must be dict or Iterable")

    descriptors = tuple(FieldDescriptor(name) for name in upd_keys_tuple)

    # Add prefix to all descriptors
    for name in descriptors:
        name.set_prefix('upd')

    return descriptors, result


def _validate_set_functions(model, upd_fds, functions):
    # type: (Type[Model], Tuple[FieldDescriptor], TSetFunctions) -> TSetFunctionsValid
    """
    Validates set functions.
    It should be a dict with field name as key and function name or AbstractSetFunction instance as value
    Default set function is EqualSetFunction
    :param model: Model updated
    :param upd_fds: A tuple of FieldDescriptors to update. It will be modified.
    :param functions: Functions to validate
    :return: A tuple of FieldDescriptor objects with set functions.
    """
    functions = functions or {}
    if not isinstance(functions, dict):
        raise TypeError("'set_functions' must be a dict instance")

    for k, v in functions.items():
        if not isinstance(k, six.string_types):
            raise ValueError("'set_functions' keys must be strings")

        if not isinstance(v, (six.string_types, AbstractSetFunction)):
            raise ValueError("'set_functions' values must be string or AbstractSetFunction instance")


    for f in upd_fds:
        f.set_function = functions.get(f.name)
        if not f.set_function.field_is_supported(f.get_field(model)):
            raise ValueError("'%s' doesn't support '%s' field" % (f.set_function.__class__.__name__, f.name))

    return upd_fds


def pdnf_clause(key_fields, field_values, key_fields_ops=()):
    # type: (TFieldNames, TIterable[Union[TIterable[Any], dict]], TOperators) -> Q
    """
    Forms WHERE query condition as Principal disjunctive normal form:
    WHERE (a = x AND b = y AND ...) OR (a = x1 AND b = y1  AND ...) OR ...
    If field_values are not given condition for empty result is returned.
    :param key_fields: Iterable of database field names ('a', 'b', ...)
    :param field_values: Field values. A list of tuples ( (x, y), (x1, y1), ...) or dicts ({'a': x, 'b': y}, ...)
    :param key_fields_ops: Field compare operators.
        It can be dict with field_name as key, operation name as value
        Or an iterable of operations in field_names order.
        The default operator is eq (it will be used for all fields, not set directly).
        Operators: [in, gt, lt, eq, gte, lte, !in, !eq]
        Example: ('eq', 'in') or {'a': 'eq', 'b': 'in'}.
    :return: Django Q-object (it can be used as Model.objects.filter(Q(...))
        https://docs.djangoproject.com/en/2.0/topics/db/queries/#complex-lookups-with-q-objects
    """
    # Validate input data
    key_fds = _validate_field_names(key_fields)
    key_fds = _validate_operators(key_fds, key_fields_ops)

    if not isinstance(field_values, Iterable):
        raise TypeError("field_values must be iterable of tuples or dicts")
    field_values = list(field_values)

    if len(field_values) == 0:
        # Empty condition should return empty result
        return ~Q()

    or_cond = Q()
    for values_item in field_values:
        if not isinstance(values_item, (dict, Iterable)):
            raise TypeError("Each field_values item must be dict or iterable")
        if len(values_item) != len(key_fds):
            raise ValueError("All field_values must contain all fields from 'field_names' parameter")

        and_cond = Q()
        for i, fd in enumerate(key_fds):
            if isinstance(values_item, dict):
                if fd.name not in values_item:
                    raise ValueError("field_values dict '%s' doesn't have key '%s'" % (json.dumps(values_item), fd.name))
                value = values_item[fd.name]
            elif isinstance(values_item, Iterable):
                values_item = list(values_item)
                value = values_item[i]
            else:
                raise TypeError("Each field_values item must be dict or iterable")

            kwargs = fd.key_operator.get_django_filters(fd.name, value)
            and_cond &= ~Q(**kwargs) if fd.key_operator.inverse else Q(**kwargs)

        or_cond |= and_cond

    return or_cond


def _get_default_fds(model, existing_fds):
    # type: (Type[Model], Tuple[FieldDescriptor]) -> Tuple[FieldDescriptor]
    """
    Finds model fields not absent in existing_fds and returns a Tuple of FieldDescriptors for them
    :param model: Model instance
    :param existing_fds: Already defined FileDescriptor objects
    :return: A tuple of FileDescriptor objects
    """
    existing_fields = {fd.get_field(model) for fd in existing_fds}
    result = []

    for f in get_model_fields(model):
        if f not in existing_fields and not isinstance(f, AutoField):
            desc = FieldDescriptor(f.attname)
            desc.set_prefix('def')
            result.append(desc)
    return tuple(result)


def _generate_fds_sql(model, conn, fds, values, for_set, cast_type):
    # type: (Type[Model], DefaultConnectionProxy, Tuple[FieldDescriptor], Iterable[Any], bool, bool) -> Tuple[List[str], List[Any]]
    """
    Generates
    :param fds:
    :param for_set:
    :return:
    """
    sql_list, params_list = [], []
    for fd, val in zip(fds, values):
        # These would not be different for different update objects and can be generated once
        field = fd.get_field(model)
        format_base = fd.set_function if for_set else fd.key_operator
        item_sql, item_upd_params = format_base.format_field_value(field, val, conn, cast_type=cast_type)
        sql_list.append(item_sql)
        params_list.extend(item_upd_params)

    return sql_list, params_list


def _with_values_query_part(model, values, conn, key_fds, upd_fds, default_fds=()):
    # type: (Type[Model], TUpdateValuesValid, DefaultConnectionProxy, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Tuple[FieldDescriptor]) -> Tuple[str, List[Any]]
    """
    Forms query part, selecting input values
    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values_tuple: update_fields_dict
    :param conn: Database connection used
    :return: Names of fields in select. A tuple of sql and it's parameters
    """
    tpl = "WITH vals(%s) AS (VALUES %s)"

    # Form data for VALUES section
    # It includes both keys and update data: keys will be used in WHERE section, while update data in SET section
    values_items = []
    values_update_params = []

    # Prepare default values to insert into database, if they are not provided in updates or keys
    # Dictionary keys list all db column names to be inserted.
    if default_fds:
        default_vals = [fd.get_field(model).get_default() for fd in default_fds]
        defaults_sql_items, defaults_params = _generate_fds_sql(model, conn, default_fds, default_vals, True, True)
    else:
        defaults_sql_items = ''
        defaults_params = []

    first = True
    for keys, updates in values.items():
        # For field sql and params
        upd_values = [updates[fd.name] for fd in upd_fds]
        upd_sql_items, upd_params = _generate_fds_sql(model, conn, upd_fds, upd_values, True, first)
        key_sql_items, key_params = _generate_fds_sql(model, conn, key_fds, keys, False, first)

        sql_items = key_sql_items + upd_sql_items
        if default_fds:
            sql_items.extend(defaults_sql_items)

        values_items.append(sql_items)
        values_update_params.extend(chain(key_params, upd_params, defaults_params))
        first = False

    values_items_sql = ['(%s)' % ', '.join(item) for item in values_items]

    # NOTE. No extra brackets here or VALUES will return nothing
    values_sql = '%s' % ', '.join(values_items_sql)

    sel_sql = ', '.join([fd.prefixed_name for fd in chain(key_fds, upd_fds, default_fds)])

    return tpl % (sel_sql, values_sql), values_update_params


def _bulk_update_query_part(model, conn, key_fds, upd_fds):
    # type: (Type[Model], DefaultConnectionProxy,Tuple[FieldDescriptor], Tuple[FieldDescriptor]) -> Tuple[str, List[Any]]
    """
    Forms bulk update query part without values, counting that all keys and values are already in vals table
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :return: A tuple of sql and it's parameters
    """

    # Query template. We will form its substitutes in next sections
    query = """
        UPDATE %s AS t SET %s
        FROM vals
        WHERE %s;
    """

    # Table we save data to
    db_table = model._meta.db_table

    # Form data for WHERE section
    # Remember that field names in sel table have prefixes.
    where_items = []
    for fd in key_fds:
        table_field = '"t"."%s"' % fd.get_field(model).column
        prefixed_sel_field = '"vals"."%s"' % fd.prefixed_name
        where_items.append(fd.key_operator.get_sql(table_field, prefixed_sel_field))
    where_sql = ' AND '.join(where_items)

    # Form data for SET section
    set_items, set_params = [], []
    for fd in upd_fds:
        func_sql, params = fd.set_function.get_sql(fd.get_field(model),
                                                   '"vals"."%s"' % fd.prefixed_name, conn, val_as_param=False)
        set_items.append(func_sql)
        set_params.extend(params)
    set_sql = ', '.join(set_items)

    # Substitute query placeholders and concatenate with VALUES section
    query = query % ('"%s"' % db_table, set_sql, where_sql)
    return query, set_params


def _bulk_update_no_validation(model, values, conn, key_fds, upd_fds):
    # type: (Type[Model], TUpdateValuesValid, DefaultConnectionProxy, Tuple[FieldDescriptor], Tuple[FieldDescriptor]) -> int
    """
    Does bulk update, skipping parameters validation.
    It is used for speed up in bulk_update_or_create, where parameters are already formatted.
    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values_tuple: update_fields_dict
    :param conn: Database connection used
    :return: Number of records updated
    """
    # No any values to update. Return that everything is done.
    if not upd_fds or not values:
        return len(values)
    values_sql, values_params = _with_values_query_part(model, values, conn, key_fds, upd_fds)
    upd_sql, upd_params = _bulk_update_query_part(model, conn, key_fds, upd_fds)

    # Execute query
    logger.debug('EXECUTING STATEMENT:\n        %sWITH PARAMETERS [%s]\n'
                 % (values_sql + upd_sql, ', '.join(str(v) for v in values_params + upd_params)))
    cursor = conn.cursor()
    cursor.execute(values_sql + upd_sql, params=values_params + upd_params)
    return cursor.rowcount


def bulk_update(model, values, key_fds='id', using=None, set_functions=None, key_fields_ops=(),
                batch_size=None, batch_delay=0):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, TOperators, Optional[int], float) -> int
    """
    Updates multiple records of a given model, finding them by key_fields.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        It can come in 2 forms:
        + Iterable of dicts. Each dict is update or create data. Each dict must contain all key_fields as keys.
            You can't update key_fields with this format.
        + Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fds: Field names, by which items would be selected.
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
    :param batch_size: Optional. If given, data is split it into batches of given size.
        Each batch is queried independently.
    :param batch_delay: Delay in seconds between batches execution, if batch_size is not None.
    :return: Number of records updated
    """
    # Validate data
    if not inspect.isclass(model):
        raise TypeError("model must be django.db.models.Model subclass")
    if not issubclass(model, Model):
        raise TypeError("model must be django.db.models.Model subclass")
    if using is not None and not isinstance(using, six.string_types):
        raise TypeError("using parameter must be None or string")
    if using and using not in connections:
        raise ValueError("using parameter must be existing database alias")

    key_fds = _validate_field_names(key_fds)
    upd_fds, values = _validate_update_values(key_fds, values)

    if len(values) == 0:
        return 0

    key_fds = _validate_operators(key_fds, key_fields_ops)
    upd_fds = _validate_set_functions(model, upd_fds, set_functions)
    conn = connection if using is None else connections[using]

    batched_result = batched_operation(_bulk_update_no_validation, values,
                                       args=(model, None, conn, key_fds, upd_fds),
                                       data_arg_index=1, batch_size=batch_size, batch_delay=batch_delay)
    return sum(batched_result)


def _bulk_update_or_create_no_validation(model, values, key_fds, upd_fds, using, update):
    # type: (Type[Model], TUpdateValues, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[str], bool) -> int
    """
    Searches for records, given in values by key_fields. If records are found, updates them from values.
    If not found - creates them from values. Note, that all fields without default value must be present in values.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fds: Field names, by which items would be selected (tuple)
    :param using: Database alias to make query to.
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function as value.
        Default function is eq.
        Functions: [eq, =; incr, +; concat, ||]
        Example: {'name': 'eq', 'int_fields': 'incr'}
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple (number of records created, number of records updated)
    """
    conn = connection if using is None else connections[using]

    with transaction.atomic(using=using):
        # Find existing values
        key_items = list(values.keys())
        qs = model.objects.filter(pdnf_clause([fd.name for fd in key_fds], key_items)).using(using).select_for_update()
        existing_values_dict = {
            tuple([item[fd.name] for fd in key_fds]): item
            for item in qs.values()
        }

        # Split into to collections: to create and to update
        create_items, update_items = [], {}
        for key, updates in values.items():
            if key in existing_values_dict:
                # Form a list of updates, if they are enabled
                if update and upd_fds:
                    update_items[key] = updates
            else:
                # Form a list of model objects for bulk_create() method
                # Insert on conflict and bulk update should work in a same way.
                # So key values will be prior over update on insert
                kwargs = updates
                kwargs.update(dict(zip([fd.name for fd in key_fds], key)))

                for fd in upd_fds:
                    fd.set_function.modify_create_params(model, fd.name, kwargs)

                create_items.append(model(**kwargs))

        # Update existing records
        updated = _bulk_update_no_validation(model, update_items, conn, key_fds, upd_fds)

        # Create absent records
        created = len(model.objects.db_manager(using).bulk_create(create_items))

        return created + updated


def _insert_on_conflict_query_part(model, conn, key_fds, upd_fds, default_fds, update):
    # type: (Type[Model], DefaultConnectionProxy, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Tuple[FieldDescriptor], bool) -> Tuple[str, List[Any]]
    """
    Forms bulk update query part without values, counting that all keys and values are already in vals table
    :param model: Model to update, a subclass of django.db.models.Model
    :param sel_key_items: Names of field in vals table.
        Key fields are prefixed with key_%d__
        Values fields are prefixed with upd__
    :param conn: Database connection used
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function class as value.
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple of sql and it's parameters
    """
    query = """
    INSERT INTO %s (%s)
    SELECT %s FROM vals
    ON CONFLICT (%s) %s
    """

    # Table we save data to
    db_table = model._meta.db_table

    # Form update data. It would be used in SET section, if values updated and INSERT section if created
    set_items, set_params = [], []
    set_columns = []
    for fd in upd_fds:
        set_columns.append('"%s"' % fd.get_field(model).column)
        func_sql, params = fd.set_function.get_sql_value(fd.get_field(model), '"vals"."%s"' % fd.prefixed_name, conn,
                                                         val_as_param=False, with_table=True)
        set_items.append(func_sql)
        set_params.extend(params)

    where_columns = []
    where_items = []
    for fd in key_fds:
        where_columns.append('"%s"' % fd.prefixed_name)
        where_items.append('EXCLUDED."%s"' % fd.get_field(model).column)

    set_sql = '(%s) = (SELECT %s FROM "vals" WHERE (%s) = (%s))' \
              % (', '.join(set_columns), ', '.join(set_items), ', '.join(where_columns), ', '.join(where_items))

    if update and upd_fds:
        conflict_action ='DO UPDATE SET %s' % set_sql
        conflict_action_params = set_params
    else:
        conflict_action = 'DO NOTHING'
        conflict_action_params = []

    # Columns to insert to table
    #upd_fields = {fd.get_field(model) for fd in upd_fds}
    #insert_fds = list(chain([fd for fd in key_fds if fd.get_field(model) not in upd_fields], upd_fds, default_fds))
    key_fields = {fd.get_field(model) for fd in key_fds}
    insert_fds = list(chain(key_fds, [fd for fd in upd_fds if fd.get_field(model) not in key_fields], default_fds))

    columns = ['"%s"' % fd.get_field(model).column for fd in insert_fds]
    columns = ', '.join(columns)

    # Columns to select from values
    val_columns, val_columns_params = [], []
    for fd in insert_fds:
        val = '"vals"."%s"' % fd.prefixed_name
        func_sql, params = fd.set_function.get_sql_value(fd.get_field(model), val, conn, val_as_param=False,
                                                         for_update=False)
        val_columns.append(func_sql)
        val_columns_params.extend(params)
    val_columns = ', '.join(val_columns)

    # Conflict columns
    key_columns = ', '.join(['"%s"' % fd.get_field(model).column for fd in key_fds])

    sql = query % (db_table, columns, val_columns, key_columns, conflict_action)
    return sql, val_columns_params + conflict_action_params


def _insert_on_conflict_no_validation(model, values, key_fds, upd_fds, using, update):
    # type: (Type[Model], TUpdateValues, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[str], bool) -> int
    """
    Searches for records, given in values by key_fields. If records are found, updates them from values.
    If not found - creates them from values. Note, that all fields without default value must be present in values.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fields: Field names, by which items would be selected (tuple)
    :param using: Database alias to make query to.
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function as value.
        Default function is eq.
        Functions: [eq, =; incr, +; concat, ||]
        Example: {'name': 'eq', 'int_fields': 'incr'}
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple (number of records created, number of records updated)
    """
    conn = connection if using is None else connections[using]

    default_fds = _get_default_fds(model, tuple(chain(key_fds, upd_fds)))
    val_sql, val_params = _with_values_query_part(model, values, conn, key_fds, upd_fds, default_fds)
    upd_sql, upd_params = _insert_on_conflict_query_part(model, conn, key_fds, upd_fds, default_fds, update)

    # Execute query
    logger.debug('EXECUTING STATEMENT:\n        %sWITH PARAMETERS [%s]\n'
                 % (val_sql + upd_sql, ', '.join(str(v) for v in val_params + upd_params)))
    cursor = conn.cursor()
    cursor.execute(val_sql + upd_sql, params=val_params + upd_params)
    return cursor.rowcount


def bulk_update_or_create(model, values, key_fields='id', using=None, set_functions=None, update=True,
                          key_is_unique=True, batch_size=None, batch_delay=0):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, bool, bool, Optional[int], float) -> int
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
    :param key_is_unique: Settings this flag to False forces library to use 3-query transactional update,
            not INSERT ... ON CONFLICT.
    :param batch_size: Optional. If given, data is split it into batches of given size.
        Each batch is queried independently.
    :param batch_delay: Delay in seconds between batches execution, if batch_size is not None.
    :return: Number of records created or updated
    """
    # Validate data
    if not inspect.isclass(model):
        raise TypeError("model must be django.db.models.Model subclass")
    if not issubclass(model, Model):
        raise TypeError("model must be django.db.models.Model subclass")
    if using is not None and not isinstance(using, six.string_types):
        raise TypeError("using parameter must be None or existing database alias")
    if using is not None and using not in connections:
        raise ValueError("using parameter must be None or existing database alias")
    if type(update) is not bool:
        raise TypeError("update parameter must be boolean")
    if type(key_is_unique) is not bool:
        raise TypeError("key_is_unique must be boolean")

    key_fds = _validate_field_names(key_fields)

    # Add prefix to all descriptors
    for i, f in enumerate(key_fds):
        f.set_prefix('key', index=i)

    upd_fds, values = _validate_update_values(key_fds, values)

    if len(values) == 0:
        return 0

    upd_fds = _validate_set_functions(model, upd_fds, set_functions)

    # Insert on conflict is supported in PostgreSQL 9.5 and only with constraint
    if get_postgres_version(using=using) >= (9, 5) and key_is_unique:
        batch_func = _insert_on_conflict_no_validation
    else:
        batch_func = _bulk_update_or_create_no_validation

    batched_result = batched_operation(batch_func, values,
                                       args=(model, None, key_fds, upd_fds, using, update),
                                       data_arg_index=1, batch_size=batch_size, batch_delay=batch_delay)

    return sum(batched_result)
