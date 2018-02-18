"""
Contains some project unbind helpers
"""
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import FieldError
from django.db import DefaultConnectionProxy, connection, connections
from django.db.models import Field
from django.db.models.sql.subqueries import UpdateQuery
from typing import TypeVar, Set, Any, Tuple, Union, Optional


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


def get_postgres_version(using=None, as_tuple=True):  # type: (Optional[str], bool) -> Union(Tuple[int], int)
    """
    Returns Postgres server verion used
    :param using: Connection alias to use
    :param as_tuple: If true, returns result as tuple, otherwize as concatenated integer
    :return: Database version as tuple (major, minor, revision) if as_tuple is true.
        A single number major*10000 + minor*100 + revision if false.
    """
    conn = connection if using is None else connections[using]
    num = conn.cursor().connection.server_version
    return (num / 10000, num % 10000 / 100, num % 100) if as_tuple else num
