"""
Contains some project unbind helpers
"""
from django.contrib.postgres.fields import HStoreField
from django.core.exceptions import FieldError
from django.db import DefaultConnectionProxy
from django.db.models import Field
from django.db.models.sql.subqueries import UpdateQuery
from typing import TypeVar, Set, Any, Tuple

# JSONField is available in django 1.9+ only
# I create fake class for previous version in order to just skip isinstance(item, JSONField) if branch
from .compatibility import hstore_serialize

try:
    from django.contrib.postgres.fields import JSONField
except ImportError:
    class JSONField(object):
        pass

T = TypeVar('T')


def get_subclasses(cls, recursive=False):  # type: (T, bool) -> Set[T]
    """
    Gets all subclasses of given class
    Attention!!! Classes would be found only if they were imported before using this function
    :param cls: Class to get subcalsses
    :param recursive: If flag is set, returns subclasses of subclasses and so on too
    :return: A list of subclasses
    """
    subclasses = set(cls.__subclasses__())

    if recursive:
        for subcls in subclasses.copy():
            subclasses.update(get_subclasses(subcls, recursive=True))

    return subclasses


def format_field_value(field, val, conn):
    # type: (Field, Any, DefaultConnectionProxy, **Any) -> Tuple[str, Tuple[Any]]
    """
    Formats value, according to field rules
    :param field: Django field to take format from
    :param val: Value to format
    :param conn: Connection used to update data
    :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
    """
    # This content is a part, taken from django.db.models.sql.compiler.SQLUpdateCompiler.as_sql()
    # And modified for our needs
    if hasattr(val, 'prepare_database_save'):
        if field.remote_field:
            val = field.get_db_prep_save(val.prepare_database_save(field), connection=conn)
        else:
            raise TypeError(
                "Tried to update field %s with a model instance, %r. "
                "Use a value compatible with %s."
                % (field, val, field.__class__.__name__)
            )
    else:
        val = field.get_db_prep_save(val, connection=conn)

    # Getting the placeholder for the field.
    query = UpdateQuery(field.model)
    compiler = query.get_compiler(connection=conn)

    if hasattr(val, 'resolve_expression'):
        val = val.resolve_expression(query, allow_joins=False, for_save=True)
        if val.contains_aggregate:
            raise FieldError("Aggregate functions are not allowed in this query")
        if val.contains_over_clause:
            raise FieldError('Window expressions are not allowed in this query.')
    elif hasattr(val, 'prepare_database_save'):
        if field.remote_field:
            val = field.get_db_prep_save(val.prepare_database_save(field), connection=conn)
        else:
            raise TypeError(
                "Tried to update field %s with a model instance, %r. "
                "Use a value compatible with %s."
                % (field, val, field.__class__.__name__)
            )
    elif isinstance(field, JSONField):
        # JSON field should be passed to execute() method as dict.
        # If get_db_prep_save is called, it wraps it in JSONAdapter object
        # When execute is done it tries wrapping it into JSONAdapter again and fails
        pass
    elif isinstance(field, HStoreField):
        # Django before 1.10 doesn't convert HStoreField values to string automatically
        # Which causes a bug in cursor.execute(). Let's do it here
        if isinstance(val, dict):
            val = hstore_serialize(val)
            val = field.get_db_prep_save(val, connection=conn)
    else:
        val = field.get_db_prep_save(val, connection=conn)

    # Getting the placeholder for the field.
    if hasattr(field, 'get_placeholder'):
        placeholder = field.get_placeholder(val, compiler, conn)
    else:
        placeholder = '%s'

    if hasattr(val, 'as_sql'):
        sql, update_params = compiler.compile(val)
        value = placeholder % sql
    elif val is not None:
        value, update_params = placeholder, [val]
    else:
        value, update_params = 'NULL', tuple()

    return value, update_params

