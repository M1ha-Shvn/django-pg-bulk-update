"""
This file contains bulk_update query functions
"""

import inspect
import json
from collections import Iterable
from itertools import chain
from logging import getLogger
from typing import Any, Type, Iterable as TIterable, Union, Optional, List, Tuple

from django.db import transaction, connection, connections
from django.db.models import Model, Q, AutoField, Field
from django.db.models.sql import UpdateQuery
from django.db.models.sql.where import WhereNode

from .compatibility import get_postgres_version, get_model_fields, returning_available, string_types
from .set_functions import AbstractSetFunction, NowSetFunction
from .types import TOperators, TFieldNames, TUpdateValues, TSetFunctions, TOperatorsValid, TUpdateValuesValid, \
    TSetFunctionsValid, TDatabase, FieldDescriptor, AbstractFieldFormatter
from .utils import batched_operation, is_auto_set_field


__all__ = ['pdnf_clause', 'bulk_update', 'bulk_update_or_create', 'bulk_create']
logger = getLogger('django-pg-bulk-update')


def _validate_field_names(field_names, param_name='key_fields'):
    # type: (TFieldNames, str) -> Tuple[FieldDescriptor]
    """
    Validates field_names.
    It can be a string for single field or an iterable of strings for multiple fields.
    :param field_names: Field names to validate
    :param param_name: A field name, which would be returned in exception.
    :return: A tuple of strings - formatted field types
    :raises AssertionError: If validation is not passed
    """
    error_message = "'%s' parameter must be iterable of strings" % param_name

    if isinstance(field_names, string_types):
        return FieldDescriptor(field_names),  # comma is not a bug, I need tuple returned
    elif isinstance(field_names, Iterable):
        field_names = list(field_names)
        for name in field_names:
            if not isinstance(name, string_types):
                raise TypeError(error_message)
        return tuple(FieldDescriptor(name) for name in field_names)
    else:
        raise TypeError(error_message)


def _validate_returning(model, returning):
    # type: (Type[Model], Optional[TFieldNames]) -> Optional[Tuple[FieldDescriptor]]
    """
    Validates returning statement to be correct
    :param model: Model to get fields from
    :param returning: Optional iterable of field names to return
    :return: None, if returning is None, a tuple of validated fds otherwise
    """
    if returning is None:
        return None
    elif returning == '*':
        ret_fds = tuple(FieldDescriptor(f.name) for f in get_model_fields(model, concrete=True))
    else:
        returning_available(raise_exception=True)

        ret_fds = _validate_field_names(returning, param_name='returning')

    for i, f in enumerate(ret_fds):
        f.set_prefix('ret', i)

    return ret_fds


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


def _validate_update_values(model, key_fds, values):
    # type: (Type[Model], Tuple[FieldDescriptor], TUpdateValues) -> Tuple[Tuple[FieldDescriptor], TUpdateValuesValid]
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
        if not key_fds:
            raise TypeError("'values' parameter can not be dict for create only operation")

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
        for i, item in enumerate(values):
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
                elif isinstance(item[fd.name], Iterable) and not isinstance(item[fd.name], string_types):
                    upd_key_values.append(tuple(item[fd.name]))
                else:
                    upd_key_values.append(item[fd.name])
            upd_values = {f: item[f] for f in upd_keys_tuple}

            if not upd_key_values:
                upd_key_values = (i,)

            result[tuple(upd_key_values)] = upd_values

    else:
        raise TypeError("'values' parameter must be dict or Iterable")

    descriptors = tuple(FieldDescriptor(name) for name in upd_keys_tuple)
    fd_names = {fd.name for fd in descriptors}

    # Add field names which are added automatically
    descriptors += tuple(
        FieldDescriptor(f.name)
        for f in get_model_fields(model)
        if is_auto_set_field(f) and f.name not in fd_names
    )

    # Add prefix to all descriptors
    for name in descriptors:
        name.set_prefix('upd')

    return descriptors, result


def _validate_set_functions(model, fds, functions):
    # type: (Type[Model], Tuple[FieldDescriptor], TSetFunctions) -> TSetFunctionsValid
    """
    Validates set functions.
    It should be a dict with field name as key and function name or AbstractSetFunction instance as value
    Default set function is EqualSetFunction
    :param model: Model updated
    :param fds: A tuple of FieldDescriptors to update. It will be modified.
    :param functions: Functions to validate
    :return: A tuple of FieldDescriptor objects with set functions.
    """
    functions = functions or {}
    if not isinstance(functions, dict):
        raise TypeError("'set_functions' must be a dict instance")

    for k, v in functions.items():
        if not isinstance(k, string_types):
            raise ValueError("'set_functions' keys must be strings")

        if not isinstance(v, (string_types, AbstractSetFunction)):
            raise ValueError("'set_functions' values must be string or AbstractSetFunction instance")

    for f in fds:
        field = f.get_field(model)
        if getattr(field, 'auto_now', False):
            f.set_function = NowSetFunction(if_null=False)
        elif getattr(field, 'auto_now_add', False):
            f.set_function = NowSetFunction(if_null=True)
        else:
            f.set_function = functions.get(f.name)

        if not f.set_function.field_is_supported(field):
            raise ValueError("'%s' doesn't support '%s' field" % (f.set_function.__class__.__name__, f.name))

    # Add functions which doesn't require values
    fd_names = {fd.name for fd in fds}
    no_value_fds = []
    for k, v in functions.items():
        if k not in fd_names:
            fd = FieldDescriptor(k, set_function=v)
            if not fd.set_function.needs_value:
                no_value_fds.append(fd)

    return fds + tuple(no_value_fds)


def _validate_where(model, where, using):
    # type: (Type[Model], Optional[WhereNode], Optional[str]) -> Tuple[str, tuple]
    """
    Validates where clause (if given).
    Translates it into sql + params tuple
    :param model: Model, where clause is applied to
    :param where: WhereNode instance as django generates it from QuerySet
    :param using: Database alias to use
    :return: Sql, params tuple
    """
    if where is None:
        return '', tuple()

    if not isinstance(where, WhereNode):
        raise TypeError("'where' must be a WhereNode instance")

    # In Django 1.7 there is no method
    if hasattr(where, 'contains_aggregate') and where.contains_aggregate:
        raise ValueError("'where' should not contain aggregates")

    query = UpdateQuery(model)
    conn = connections[using] if using else connection
    compiler = query.get_compiler(connection=conn)
    sql, params = where.as_sql(compiler, conn)

    # I change table name to "t" inside queries
    if sql:
        sql = sql.replace('"%s"' % model._meta.db_table, '"t"')

    return sql, params


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
                    raise ValueError("field_values dict '%s' doesn't have key '%s'"
                                     % (json.dumps(values_item), fd.name))
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

    for f in get_model_fields(model, concrete=True):
        if f not in existing_fields and not isinstance(f, AutoField) and f.has_default():
            desc = FieldDescriptor(f.name)
            desc.set_prefix('def')
            result.append(desc)
    return tuple(result)


def _generate_fds_sql(conn, fields, format_bases, values, cast_type):
    # type: (TDatabase, Tuple[Field], Iterable[AbstractFieldFormatter], Iterable[Any], bool) -> Tuple[Tuple[str], Tuple[Any]]
    if not fields:
        return tuple(), tuple()

    sql_list, params_list = zip(*(
        format_base.format_field_value(field, val, conn, cast_type=cast_type)
        for field, format_base, val in zip(fields, format_bases, values)
    ))

    return sql_list, tuple(chain(*params_list))


def _with_values_query_part(model, values, conn, key_fds, upd_fds, default_fds=()):
    # type: (Type[Model], TUpdateValuesValid, TDatabase, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Tuple[FieldDescriptor]) -> Tuple[str, List[Any]]
    """
    Forms query part, selecting input values
    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values_tuple: update_fields_dict
    :param conn: Database connection used
    :return: Names of fields in select. A tuple of sql and it's parameters
    """
    tpl = "WITH vals(%s) AS (VALUES %s)%s"

    # Form data for VALUES section
    # It includes both keys and update data: keys will be used in WHERE section, while update data in SET section
    values_items = []
    values_update_params = []

    if default_fds:
        # Prepare default values to insert into database, if they are not provided in updates or keys
        # Dictionary keys list all db column names to be inserted.
        defaults_sel_sql = ', '.join('"%s"' % fd.prefixed_name for fd in default_fds)
        defaults_vals = (fd.get_field(model).get_default() for fd in default_fds)
        defaults_fields = tuple(fd.get_field(model) for fd in default_fds)
        defaults_format_bases = tuple(fd.set_function for fd in default_fds)
        defaults_sql_items, defaults_params = _generate_fds_sql(conn, defaults_fields, defaults_format_bases,
                                                                defaults_vals, True)
        defaults_sql = ",\n default_vals(%s) AS (VALUES (%s))" % (defaults_sel_sql, ', '.join(defaults_sql_items))
    else:
        defaults_sql = ''
        defaults_params = []

    first = True
    key_fields = tuple(fd.get_field(model) for fd in key_fds)
    key_format_bases = tuple(fd.key_operator for fd in key_fds)
    upd_fields = tuple(fd.get_field(model) for fd in upd_fds)
    upd_format_bases = tuple(fd.set_function for fd in upd_fds)
    for keys, updates in values.items():
        # For field sql and params
        upd_values = [updates[fd.name] for fd in upd_fds if fd.set_function.needs_value]
        upd_sql_items, upd_params = _generate_fds_sql(conn, upd_fields, upd_format_bases, upd_values, first)
        key_sql_items, key_params = _generate_fds_sql(conn, key_fields, key_format_bases, keys, first)

        sql_items = key_sql_items + upd_sql_items

        values_items.append(sql_items)
        values_update_params.extend(chain(key_params, upd_params))
        first = False

    # NOTE. No extra brackets here or VALUES will return nothing
    values_sql = ', '.join(
        '(%s)' % ', '.join(item) for item in values_items
    )

    sel_sql = ', '.join(
        '"%s"' % fd.prefixed_name for fd in chain(key_fds, upd_fds) if fd.set_function.needs_value
    )

    return tpl % (sel_sql, values_sql, defaults_sql), values_update_params + list(defaults_params)


def _bulk_update_query_part(model, conn, key_fds, upd_fds, where):
    # type: (Type[Model], TDatabase, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Tuple[str, tuple]) -> Tuple[str, List[Any]]
    """
    Forms bulk update query part without values, counting that all keys and values are already in vals table
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :param key_fds: Field names, by which items would be selected (tuple)
    :param upd_fds: FieldDescriptor objects to update
    :param where: A sql, params tuple to filter query data before update
    :return: A tuple of sql and it's parameters
    """

    # Query template. We will form its substitutes in next sections
    query = """
        UPDATE %s AS t SET %s
        FROM "vals"
        WHERE %s
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
    where_params = []

    if where[0]:
        where_sql = '(%s) AND (%s)' % (where_sql, where[0])
        where_params.extend(where[1])

    # Form data for SET section
    set_items, set_params = [], []
    for fd in upd_fds:
        func_sql, params = fd.set_function.get_sql(fd.get_field(model),
                                                   '"vals"."%s"' % fd.prefixed_name, conn, val_as_param=False)
        set_items.append(func_sql)
        set_params.extend(params)
    set_sql = ', '.join(set_items)

    # Substitute query placeholders and concatenate with VALUES section
    quoted_table = conn.ops.quote_name(db_table)
    query = query % (quoted_table, set_sql, where_sql)
    return query, set_params + where_params


def _returning_query_part(model, conn, ret_fds):
    # type: (Type[Model], TDatabase, Optional[Tuple[FieldDescriptor]]) -> Tuple[str, List[Any]]
    """
    Forms returning query part
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :param ret_fds: FieldDescriptors to return
    :return: A tuple of sql and it's parameters
    """
    # No returning statement
    if ret_fds is None:
        return '', []

    return "RETURNING %s" % ', '.join('"%s"' % fd.get_field(model).column for fd in ret_fds), []


def _execute_update_query(model, conn, sql, params, ret_fds):
    # type: (Type[Model], TDatabase, str, List[Any], Optional[Tuple[FieldDescriptor]]) -> Union[int, 'ReturningQuerySet']  # noqa: F821
    """
    Does bulk update, skipping parameters validation.
    It is used for speed up in bulk_update_or_create, where parameters are already formatted.
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :param ret_fds: Optional fds to return as ReturningQuerySet
    :return: Number of records updated if ret_fds not given. ReturningQuerySet otherwise
    """
    # Execute query
    logger.debug('EXECUTING STATEMENT:\n        %sWITH PARAMETERS [%s]\n'
                 % (sql, ', '.join(str(v) for v in params)))
    if ret_fds is None:
        cursor = conn.cursor()
        cursor.execute(sql, params=params)
        return cursor.rowcount
    else:
        from django_pg_returning import ReturningQuerySet
        return ReturningQuerySet(sql, model=model, params=params, using=conn.alias,
                                 fields=[fd.get_field(model).attname for fd in ret_fds])


def _bulk_update_no_validation(model, values, conn, key_fds, upd_fds, ret_fds, where):
    # type: (Type[Model], TUpdateValuesValid, TDatabase, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[Tuple[FieldDescriptor]], Tuple[str, tuple]) -> Union[int, 'ReturningQuerySet']    # noqa: F821
    """
    Does bulk update, skipping parameters validation.
    It is used for speed up in bulk_update_or_create, where parameters are already formatted.
    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values_tuple: update_fields_dict
    :param conn: Database connection used
    :param key_fds: Field names, by which items would be selected (tuple)
    :param upd_fds: FieldDescriptor objects to update
    :param ret_fds: Optional fds to return as ReturningQuerySet
    :param where: A sql, params tuple to filter query data before update
    :return: Number of records updated if ret_fds not given. ReturningQuerySet otherwise
    """
    # No any values to update. Return that everything is done.
    if not upd_fds or not values:
        from django_pg_returning import ReturningQuerySet
        return len(values) if ret_fds is None else ReturningQuerySet(None)

    values_sql, values_params = _with_values_query_part(model, values, conn, key_fds, upd_fds)
    upd_sql, upd_params = _bulk_update_query_part(model, conn, key_fds, upd_fds, where)
    ret_sql, ret_params = _returning_query_part(model, conn, ret_fds)

    sql = "%s %s %s" % (values_sql, upd_sql, ret_sql)
    params = values_params + upd_params + ret_params

    return _execute_update_query(model, conn, sql, params, ret_fds)


def _concat_batched_result(batched_result, ret_fds):
    # type: (List[Any], Optional[Tuple[FieldDescriptor]]) -> Union[int, 'ReturningQuerySet']  # noqa: F821
    """
    Gets results of batched execution and format it to appropriate request answer
    :param batched_result: Batched result
    :param ret_fds: Descriptors of fields to return.
    :return: ReturningQuerySet if returning is not None or updated/inserted records count otherwise
    """
    if ret_fds is None:
        return sum(batched_result)
    elif len(batched_result) == 0:
        from django_pg_returning import ReturningQuerySet
        return ReturningQuerySet(None)
    else:
        # I can't use chain here, as it iterates over QuerySets, and I have to return ReturningQuerySet
        from django_pg_returning import ReturningQuerySet
        return sum(batched_result[1:], batched_result[0])


def bulk_update(model, values, key_fields='id', using=None, set_functions=None, key_fields_ops=(),
                where=None, returning=None, batch_size=None, batch_delay=0):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, TOperators, Optional[WhereNode], Optional[TFieldNames], Optional[int], float) -> Union[int, 'ReturningQuerySet']    # noqa: F821
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
    :param where: A WhereNode instance - filter condition for all query
    :param returning: Optional. If given, returns updated values of fields, listed in parameter.
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
    if using is not None and not isinstance(using, string_types):
        raise TypeError("using parameter must be None or string")
    if using and using not in connections:
        raise ValueError("using parameter must be existing database alias")

    key_fields = _validate_field_names(key_fields)
    upd_fds, values = _validate_update_values(model, key_fields, values)
    ret_fds = _validate_returning(model, returning)
    where = _validate_where(model, where, using)

    if len(values) == 0:
        return _concat_batched_result([], ret_fds)

    key_fields = _validate_operators(key_fields, key_fields_ops)
    upd_fds = _validate_set_functions(model, upd_fds, set_functions)
    conn = connection if using is None else connections[using]

    batched_result = batched_operation(_bulk_update_no_validation, values,
                                       args=(model, None, conn, key_fields, upd_fds, ret_fds, where),
                                       data_arg_index=1, batch_size=batch_size, batch_delay=batch_delay)

    return _concat_batched_result(batched_result, ret_fds)


def _insert_query_part(model, conn, insert_fds, default_fds):
    # type: (Type[Model], TDatabase, Tuple[FieldDescriptor], Tuple[FieldDescriptor]) -> Tuple[str, List[Any]]
    """
    Forms bulk update query part without values, counting that all keys and values are already in vals table
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :param insert_fds: FieldDescriptor objects to insert
    :param default_fds: FieldDescriptor objects to take as default values
    :return: A tuple of sql and it's parameters
    """
    query = """
        INSERT INTO %s (%s)
        SELECT %s FROM %s
    """

    # Table we save data to
    db_table = conn.ops.quote_name(model._meta.db_table)
    from_table = '"vals" CROSS JOIN "default_vals"' if default_fds else '"vals"'

    # Columns to insert to table
    columns = ', '.join(
        '"%s"' % fd.get_field(model).column
        for fd in chain(insert_fds, default_fds)
    )

    # Columns to select from values
    val_columns, val_columns_params = [], []
    for fd in insert_fds:
        val = '"vals"."%s"' % fd.prefixed_name
        func_sql, params = fd.set_function.get_sql_value(fd.get_field(model), val, conn, val_as_param=False,
                                                         for_update=False)
        val_columns.append(func_sql)
        val_columns_params.extend(params)

    for fd in default_fds:
        val = '"default_vals"."%s"' % fd.prefixed_name
        func_sql, params = fd.set_function.get_sql_value(fd.get_field(model), val, conn, val_as_param=False,
                                                         for_update=False)
        val_columns.append(func_sql)
        val_columns_params.extend(params)

    val_columns = ', '.join(val_columns)

    sql = query % (db_table, columns, val_columns, from_table)
    return sql, val_columns_params


def _insert_no_validation(model, values, default_fds, insert_fds, ret_fds, using):
    # type: (Type[Model], TUpdateValues, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[Tuple[FieldDescriptor]], Optional[str]) -> Union[int, 'ReturningQuerySet']  # noqa: F821
    """
    Creates a batch of records in database.
    Acts like native QuerySet.bulk_create() method, but uses library infrastructure and input formats
    Can be much more effective than native implementation on wide models.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param default_fds: FieldDescriptor objects to use as defaults
    :param insert_fds: FieldDescriptor objects to insert
    :param ret_fds: Optional fds to return as ReturningQuerySet
    :param using: Database alias to make query to.
    :return: A tuple (number of records created, number of records updated)
    """
    conn = connection if using is None else connections[using]
    val_sql, val_params = _with_values_query_part(model, values, conn, tuple(), insert_fds, default_fds)
    insert_sql, insert_params = _insert_query_part(model, conn, insert_fds, default_fds)
    ret_sql, ret_params = _returning_query_part(model, conn, ret_fds)

    sql = "%s %s %s" % (val_sql, insert_sql, ret_sql)
    params = val_params + insert_params + ret_params

    return _execute_update_query(model, conn, sql, params, ret_fds)


def bulk_create(model, values, using=None, set_functions=None, returning=None, batch_size=None, batch_delay=0):
    # type: (Type[Model], TUpdateValues, Optional[str], TSetFunctions, Optional[TFieldNames], Optional[int], float) -> Union[int, 'ReturningQuerySet']  # noqa: F821
    """
    Creates a batch of records in database.
    Acts like native QuerySet.bulk_create() method, but uses library infrastructure and input formats
    Can be much more effective than native implementation on wide models.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update.
        All items must update same fields!!!
        Iterable of dicts. Each dict is create data.
    :param using: Database alias to make query to.
    :param set_functions: Functions to set values.
        Should be a dict of field name as key, function as value.
        Default function is eq.
        Functions: [eq, =; incr, +; concat, ||]
        Example: {'name': 'eq', 'int_fields': 'incr'}
    :param returning: Optional. If given, returns updated values of fields, listed in parameter.
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
    if using is not None and not isinstance(using, string_types):
        raise TypeError("using parameter must be None or existing database alias")
    if using is not None and using not in connections:
        raise ValueError("using parameter must be None or existing database alias")

    insert_fds, values = _validate_update_values(model, tuple(), values)
    ret_fds = _validate_returning(model, returning)

    if len(values) == 0:
        return _concat_batched_result([], ret_fds)

    default_fds = _get_default_fds(model, tuple(insert_fds))
    insert_fds = _validate_set_functions(model, insert_fds, set_functions)

    batched_result = batched_operation(_insert_no_validation, values,
                                       args=(model, None, default_fds, insert_fds, ret_fds, using),
                                       data_arg_index=1, batch_size=batch_size, batch_delay=batch_delay)

    return _concat_batched_result(batched_result, ret_fds)


def _bulk_update_or_create_no_validation(model, values, key_fds, upd_fds, ret_fds, using, update):
    # type: (Type[Model], TUpdateValues, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[Tuple[FieldDescriptor]], Optional[str], bool) -> int
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
    :param upd_fds: FieldDescriptor objects to update
    :param ret_fds: Optional fds to return as ReturningQuerySet
    :param using: Database alias to make query to.
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
        update_result = _bulk_update_no_validation(model, update_items, conn, key_fds, upd_fds, ret_fds, ('', tuple()))

        # Create absent records
        # auto_now and auto_now_add don't work in bulk_create, as they are set up in pre_save

        created_items = model.objects.db_manager(using).bulk_create(create_items)

        if ret_fds is None:
            return len(created_items) + update_result
        else:
            # HACK There's no way to create ReturningQuerySet from already prefetched items
            res = update_result
            res._result_cache.extend(create_items)
            return res


def _insert_on_conflict_query_part(model, conn, key_fds, upd_fds, default_fds, update):
    # type: (Type[Model], TDatabase, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Tuple[FieldDescriptor], bool) -> Tuple[str, List[Any]]
    """
    Forms bulk update query part without values, counting that all keys and values are already in vals table
    :param model: Model to update, a subclass of django.db.models.Model
    :param conn: Database connection used
    :param key_fds: FieldDescriptor objects to use as key fields
    :param upd_fds: FieldDescriptor objects to update
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple of sql and it's parameters
    """
    query = "%s ON CONFLICT (%s) %s"

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
        conflict_action = 'DO UPDATE SET %s' % set_sql
        conflict_action_params = set_params
    else:
        conflict_action = 'DO NOTHING'
        conflict_action_params = []

    # Columns to insert to table
    key_fields = {fd.get_field(model) for fd in key_fds}
    insert_fds = tuple(chain(key_fds, (fd for fd in upd_fds if fd.get_field(model) not in key_fields)))
    insert_sql, insert_params = _insert_query_part(model, conn, insert_fds, default_fds)

    # Conflict columns
    key_columns = ', '.join('"%s"' % fd.get_field(model).column for fd in key_fds)

    sql = query % (insert_sql, key_columns, conflict_action)
    return sql, insert_params + conflict_action_params


def _insert_on_conflict_no_validation(model, values, key_fds, upd_fds, ret_fds, using, update):
    # type: (Type[Model], TUpdateValues, Tuple[FieldDescriptor], Tuple[FieldDescriptor], Optional[Tuple[FieldDescriptor]], Optional[str], bool) -> Union[int, 'ReturningQuerySet']  # noqa: F821
    """
    Searches for records, given in values by key_fields. If records are found, updates them from values.
    If not found - creates them from values. Note, that all fields without default value must be present in values.

    :param model: Model to update, a subclass of django.db.models.Model
    :param values: Data to update. All items must update same fields!!!
        Dict of key_values: update_fields_dict
            - key_values can be iterable or single object.
            - If iterable, key_values length must be equal to key_fields length.
            - If single object, key_fields is expected to have 1 element
    :param key_fds: FieldDescriptor objects to use as key fields
    :param upd_fds: FieldDescriptor objects to update
    :param ret_fds: Optional fds to return as ReturningQuerySet
    :param using: Database alias to make query to.
    :param update: If this flag is not set, existing records will not be updated
    :return: A tuple (number of records created, number of records updated)
    """
    conn = connection if using is None else connections[using]

    default_fds = _get_default_fds(model, tuple(chain(key_fds, upd_fds)))
    val_sql, val_params = _with_values_query_part(model, values, conn, key_fds, upd_fds, default_fds)
    upd_sql, upd_params = _insert_on_conflict_query_part(model, conn, key_fds, upd_fds, default_fds, update)
    ret_sql, ret_params = _returning_query_part(model, conn, ret_fds)

    sql = "%s %s %s" % (val_sql, upd_sql, ret_sql)
    params = val_params + upd_params + ret_params

    return _execute_update_query(model, conn, sql, params, ret_fds)


def bulk_update_or_create(model, values, key_fields='id', using=None, set_functions=None, update=True,
                          key_is_unique=True, returning=None, batch_size=None, batch_delay=0):
    # type: (Type[Model], TUpdateValues, TFieldNames, Optional[str], TSetFunctions, bool, bool, Optional[TFieldNames], Optional[int], float) -> Union[int, 'ReturningQuerySet']  # noqa: F821
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
    :param returning: Optional. If given, returns updated values of fields, listed in parameter.
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
    if using is not None and not isinstance(using, string_types):
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

    upd_fds, values = _validate_update_values(model, key_fds, values)
    ret_fds = _validate_returning(model, returning)

    if len(values) == 0:
        return _concat_batched_result([], ret_fds)

    upd_fds = _validate_set_functions(model, upd_fds, set_functions)

    # Insert on conflict is supported in PostgreSQL 9.5 and only with constraint
    if get_postgres_version(using=using) >= (9, 5) and key_is_unique:
        batch_func = _insert_on_conflict_no_validation
    else:
        batch_func = _bulk_update_or_create_no_validation

    batched_result = batched_operation(batch_func, values,
                                       args=(model, None, key_fds, upd_fds, ret_fds, using, update),
                                       data_arg_index=1, batch_size=batch_size, batch_delay=batch_delay)

    return _concat_batched_result(batched_result, ret_fds)
