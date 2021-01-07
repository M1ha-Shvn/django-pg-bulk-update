"""
This file contains number of functions to handle different software versions compatibility
"""
import importlib
import json
import sys
from typing import Dict, Any, Optional, Union, Tuple, List, Type, Callable

import django
from django.db import connection, connections, models, migrations
from django.db.models import Model, Field, BigIntegerField, IntegerField

from .types import TDatabase


# six.string_types replacement in order to remove dependency
string_types = (str,) if sys.version_info[0] == 3 else (str, unicode)  # noqa F821


# pytz.utc timezone in order to remove dependency
try:
    from datetime import timezone
    tz_utc = timezone.utc
except ImportError:
    # For python before 3.3
    import pytz
    tz_utc = pytz.utc


def zip_longest(*args, **kwargs):
    """
    https://docs.python.org/3.5/library/itertools.html#itertools.zip_longest
    """
    try:
        # python 3
        from itertools import zip_longest
        return zip_longest(*args, **kwargs)
    except ImportError:
        # python 2.7
        from itertools import izip_longest
        return izip_longest(*args, **kwargs)


def jsonb_available():  # type: () -> bool
    """
    Checks if we can use JSONField.
    It is available since django 1.9 and doesn't support Postgres < 9.4
    :return: Bool
    """
    return get_postgres_version() >= (9, 4) and django.VERSION >= (1, 9)


def hstore_available():  # type: () -> bool
    """
    Checks if we can use HStoreField.
    It is available since django 1.8
    :return: Bool
    """
    return django.VERSION >= (1, 8)


def array_available():  # type: () -> bool
    """
    Checks if we can use ArrayField.
    It is available since django 1.8
    :return: Bool
    """
    return django.VERSION >= (1, 8)


def import_pg_field_or_dummy(field_name, available_func):  # type: (str, Callable) -> Any
    """
    Imports PostgreSQL specific field, if it is avaialbe. Otherwise returns dummy class
    This is used to simplify isinstance(f, PGField) checks
    :param field_name: Field name. It should have same case as class name
    :param available_func: Function to check if field is available. Should return boolean
    :return: Field class or dummy class
    """
    if sys.version_info < (3,):
        field_name = field_name.encode()

    dummy_class = type(field_name, (), {})

    if available_func():
        # Since django 3.1 JSONField is moved to django.db.models
        module_basic = importlib.import_module('django.db.models')
        if hasattr(module_basic, field_name):
            return getattr(module_basic, field_name, dummy_class)

        module_pg = importlib.import_module('django.contrib.postgres.fields')
        return getattr(module_pg, field_name, dummy_class)
    else:
        return dummy_class


def returning_available(raise_exception=False):
    # type: (bool) -> bool
    """
    Tests if returning query is available
    :return: boolean
    """
    try:
        from django_pg_returning import ReturningQuerySet  # noqa: F401
        return True
    except ImportError:
        if raise_exception:
            raise ImportError('Returning feature requires django-pg-returning library installed. '
                              'Use pip install django-pg-returning')
        else:
            return False


def hstore_serialize(value):  # type: (Dict[Any, Any]) -> Dict[str, str]
    """
    Django before 1.10 doesn't convert HStoreField values to string automatically
    Which causes a bug in cursor.execute(). This function converts all key/values to string
    :param value: A dict
    :return:
    """
    val = {
        str(k): json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        for k, v in value.items()
    }
    return val


def get_postgres_version(using=None, as_tuple=True):
    # type: (Optional[str], bool) -> Union[Tuple[int], int]
    """
    Returns Postgres server version used
    :param using: Connection alias to use
    :param as_tuple: If true, returns result as tuple, otherwise as concatenated integer
    :return: Database version as tuple (major, minor, revision) if as_tuple is true.
        A single number major*10000 + minor*100 + revision if false.
    """
    conn = connection if using is None else connections[using]
    num = conn.cursor().connection.server_version
    return (int(num / 10000), int(num % 10000 / 100), num % 100) if as_tuple else num


def get_field_db_type(field, conn):
    # type: (models.Field, TDatabase) -> str
    """
    Get database field type used for this field.
    :param field: django.db.models.Field instance
    :param conn: Database connection used
    :return: Database type name (str)
    """
    # db_type() as id field returned 'serial' instead of 'integer' here
    # rel_db_type() return integer, but it is not available before django 1.10
    # rel_db_type() returns serial[] for arrays.
    # cast_db_type() is not well documented and returns serial too.
    db_type = field.db_type(conn)

    # Some external libraries may add column fields here
    # Let's cut them
    cut_phrases = [
        'CONSTRAINT', 'NOT NULL', 'NULL', 'CHECK', 'DEFAULT', 'UNIQUE', 'PRIMARY KEY', 'REFERENCES', 'COLLATE'
    ]
    for ph in cut_phrases:
        db_type = db_type.split(ph, 1)[0]

    db_type = db_type.strip()

    if 'bigserial' in db_type:
        db_type = db_type.replace('bigserial', BigIntegerField().db_type(conn))
    elif 'serial' in db_type:
        db_type = db_type.replace('serial', IntegerField().db_type(conn))

    return db_type


def get_model_fields(model, concrete=False):  # type: (Type[Model], Optional[bool]) -> List[Field]
    """
    Gets model field
    :param model: Model to get fields for
    :param concrete: If set, returns only fields with column in model's table
    :return: A list of fields
    """
    if not hasattr(model._meta, 'get_fields'):
        # Django 1.8+
        if concrete:
            res = model._meta.concrete_fields
        else:
            res = model._meta.fields + model._meta.many_to_many
    else:
        res = model._meta.get_fields()

        if concrete:
            # Many to many fields have concrete flag set to True. Strange.
            res = [f for f in res if getattr(f, 'concrete', True) and not getattr(f, 'many_to_many', False)]

    return res


# Postgres 9.4 has JSONB support, but doesn't support concat operator (||)
# So I've taken function to solve the problem from
# https://stackoverflow.com/questions/30101603/merging-concatenating-jsonb-columns-in-query
class Postgres94MergeJSONBMigration(migrations.RunSQL):
    FUNCTION_NAME = "django_pg_bulk_update_jsonb_merge"

    SQL_TEMPLATE = """
    CREATE OR REPLACE FUNCTION %s(jsonb1 JSONB, jsonb2 JSONB)
        RETURNS JSONB AS $$
        DECLARE
          result JSONB;
          v RECORD;
        BEGIN
           result = (
             SELECT json_object_agg(KEY,value)
             FROM (
               SELECT jsonb_object_keys(jsonb1) AS KEY,
                 1::INT AS jsb,
                 jsonb1 -> jsonb_object_keys(jsonb1) AS value
               UNION SELECT jsonb_object_keys(jsonb2) AS KEY,
                 2::INT AS jsb,
                 jsonb2 -> jsonb_object_keys(jsonb2) AS value ) AS t1
           );
           RETURN COALESCE(result, '{}'::JSONB);
        END;
        $$ LANGUAGE plpgsql;
    """

    REVERSE_SQL_TEMPLATE = """
    DROP FUNCTION IF EXISTS %s(jsonb1 JSONB, jsonb2 JSONB);
    """

    def __init__(self, **kwargs):
        sql = self.SQL_TEMPLATE % self.FUNCTION_NAME
        kwargs['reverse_sql'] = self.REVERSE_SQL_TEMPLATE % self.FUNCTION_NAME
        super(Postgres94MergeJSONBMigration, self).__init__(sql, **kwargs)
