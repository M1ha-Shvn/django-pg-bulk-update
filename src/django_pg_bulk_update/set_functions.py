"""
This file contains classes, describing functions which set values to fields.
"""
import datetime
from typing import Type, Optional, Any, Tuple, Dict

from django.db.models import Field, Model

from .compatibility import get_postgres_version, jsonb_available, Postgres94MergeJSONBMigration, hstore_serialize, \
    hstore_available, import_pg_field_or_dummy, tz_utc
from .types import TDatabase, AbstractFieldFormatter
from .utils import get_subclasses, format_field_value

# When doing increment operations, we need to replace NULL values with something
# This dictionary contains field defaults by it's class name.
# I don't use classes as keys not to import them here
base_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tz_utc)
NULL_DEFAULTS = {
    # Standard django types
    'IntegerField': 0,
    'BigIntegerField': 0,
    'SmallIntegerField': 0,
    'PositiveIntegerField': 0,
    'PositiveSmallIntegerField': 0,
    'FloatField': 0,
    'DecimalField': 0,

    'CharField': '',
    'TextField': '',
    'EmailField': '',
    'FilePathField': '',
    'SlugField': '',
    'CommaSeparatedIntegerField': '0,0',
    'URLField': '',
    'BinaryField': b'',
    'UUIDField': '',

    'DateField': base_datetime.date(),
    'DateTimeField': base_datetime,
    'DurationField': datetime.timedelta(seconds=0),
    'TimeField': base_datetime.time(),

    # This may be incorrect, but there is no any chance to understand, what is correct here
    'BooleanField': True,
    'NullBooleanField': True,

    # These fields can't be null, but I add them for compatibility
    'AutoField': 0,
    'BigAutoField': 0,

    # Postgres specific types
    'ArrayField': [],
    'CICharField': '',
    'CIEmailField': '',
    'CITextField': '',
    'HStoreField': {},
    'JSONField': {},

    'IntegerRangeField': (0, 0),
    'BigIntegerRangeField': (0, 0),
    'FloatRangeField': (0, 0),
    'DateTimeRangeField': (base_datetime, base_datetime),
    'DateRangeField': (base_datetime.date(), base_datetime.date())
}


class AbstractSetFunction(AbstractFieldFormatter):
    names = set()

    # If set function supports any field class, this should be None.
    # Otherwise a set of class names supported
    supported_field_classes = None

    # If set functions doesn't need value from input, set this to False.
    needs_value = True

    def modify_create_params(self, model, key, kwargs):
        # type: (Type[Model], str, Dict[str, Any]) -> Dict[str, Any]
        """
        This method modifies parameters before passing them to model(**kwargs)
        :param model: Model to get field from
        :param key: Field key, for which SetFunction is adopted
        :param kwargs: Function parameters
        :return: Modified params
        """
        if hstore_available():
            # Django before 1.10 doesn't convert HStoreField values to string automatically
            # Which causes a bug in cursor.execute(). Let's do it here
            from django.contrib.postgres.fields import HStoreField
            if isinstance(model._meta.get_field(key), HStoreField):
                kwargs[key] = hstore_serialize(kwargs[key])

        return kwargs

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        # type: (Field, Any, TDatabase, bool, bool, bool, **Any) -> Tuple[str, Tuple[Any]]
        """
        Returns value sql to set into field and parameters for query execution
        This method is called from get_sql() by default.
        :param field: Django field to take format from
        :param val: Value to format
        :param connection: Connection used to update data
        :param val_as_param: If flag is not set, value should be converted to string and inserted into query directly.
            Otherwise a placeholder and query parameter will be used
        :param with_table: If flag is set, column name in sql is prefixed by table name
        :param for_update: If flag is set, returns update sql. Otherwise - insert SQL
        :param kwargs: Additional arguments, if needed
        :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
        """
        raise NotImplementedError("'%s' must define get_sql method" % self.__class__.__name__)

    def get_sql(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        # type: (Field, Any, TDatabase, bool, bool, bool, **Any) -> Tuple[str, Tuple[Any]]
        """
        Returns function sql and parameters for query execution
        :param field: Django field to take format from
        :param val: Value to format
        :param connection: Connection used to update data
        :param val_as_param: If flag is not set, value should be converted to string and inserted into query directly.
            Otherwise a placeholder and query parameter will be used
        :param with_table: If flag is set, column name in sql is prefixed by table name
        :param for_update: If flag is set, returns update sql. Otherwise - insert SQL
        :param kwargs: Additional arguments, if needed
        :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
        """
        val, params = self.get_sql_value(field, val, connection, val_as_param=val_as_param, with_table=with_table,
                                         for_update=for_update, **kwargs)
        return '"%s" = %s' % (field.column, val), params

    @classmethod
    def get_function_by_name(cls, name):  # type: (str) -> Optional[Type[AbstractSetFunction]]
        """
        Finds subclass of AbstractOperation applicable to given operation name
        :param name: String name to search
        :return: AbstractOperation subclass if found, None instead
        """
        try:
            return next(sub_cls for sub_cls in get_subclasses(cls, recursive=True) if name in sub_cls.names)
        except StopIteration:
            raise ValueError("Operation with name '%s' doesn't exist" % name)

    def field_is_supported(self, field):  # type: (Field) -> bool
        """
        Tests if this set function supports given field
        :param field: django.db.models.Field instance
        :return: Boolean
        """
        if self.supported_field_classes is None:
            return True
        else:
            return field.__class__.__name__ in self.supported_field_classes

    def _parse_null_default(self, field, connection, **kwargs):
        """
        Part of get_function_sql() method.
        When operation is done on the bases of field's previous value, we need a default to set instead of NULL.
        This function gets this default value.
        :param field: Field to set default for
        :param connection: Connection used to update data
        :param kwargs: kwargs of get_function_sql
        :return: null_default value
        """
        if 'null_default' in kwargs:
            null_default = kwargs['null_default']
        elif field.__class__.__name__ in NULL_DEFAULTS:
            null_default = NULL_DEFAULTS[field.__class__.__name__]
        else:
            raise Exception("Operation '%s' doesn't support field '%s'"
                            % (self.__class__.__name__, field.__class__.__name__))

        return self.format_field_value(field, null_default, connection, cast_type=True)

    def _get_field_column(self, field, with_table=False):
        # type: (Field, bool) -> str
        """
        Returns quoted field column, prefixed with table name if needed
        :param field: Field instance
        :param with_table: Boolean flag - add table or not
        :return: String name
        """
        table = '"%s".' % field.model._meta.db_table if with_table else ''
        return '%s"%s"' % (table, field.column)


class EqualSetFunction(AbstractSetFunction):
    names = {'eq', '='}

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        if val_as_param:
            return self.format_field_value(field, val, connection)
        else:
            return '%s' % str(val), []


class EqualNotNullSetFunction(AbstractSetFunction):
    names = {'eq_not_null'}

    def modify_create_params(self, model, key, kwargs):
        if kwargs[key] is None:
            del kwargs[key]

        return kwargs

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        tpl = 'COALESCE(%s, %s)'
        if for_update:
            default_value, default_params = self._get_field_column(field, with_table=with_table), []
        else:
            default_value, default_params = self.format_field_value(field, field.get_default(), connection)
        if val_as_param:
            sql, params = self.format_field_value(field, val, connection)
            return tpl % (sql, default_value), params + default_params
        else:
            return tpl % (str(val), default_value), default_params


class PlusSetFunction(AbstractSetFunction):
    names = {'+', 'incr'}

    supported_field_classes = {'IntegerField', 'FloatField', 'AutoField', 'BigAutoField', 'BigIntegerField',
                               'SmallIntegerField', 'PositiveIntegerField', 'PositiveSmallIntegerField', 'DecimalField',
                               'IntegerRangeField', 'BigIntegerRangeField', 'FloatRangeField', 'DateTimeRangeField',
                               'DateRangeField'}

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        null_default, null_default_params = self._parse_null_default(field, connection, **kwargs)

        if val_as_param:
            sql, params = self.format_field_value(field, val, connection)
        else:
            sql, params = str(val), tuple()

        if for_update:
            tpl = 'COALESCE(%s, %s) + %s'
            return tpl % (self._get_field_column(field, with_table=with_table), null_default, sql),\
                null_default_params + params
        else:
            return sql, params


class ConcatSetFunction(AbstractSetFunction):
    names = {'||', 'concat'}

    supported_field_classes = {'CharField', 'TextField', 'EmailField', 'FilePathField', 'SlugField', 'HStoreField',
                               'URLField', 'BinaryField', 'JSONField', 'ArrayField', 'CITextField', 'CICharField',
                               'CIEmailField'}

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        null_default, null_default_params = self._parse_null_default(field, connection, **kwargs)
        JSONField = import_pg_field_or_dummy('JSONField', jsonb_available)

        # Postgres 9.4 has JSONB support, but doesn't support concat operator (||)
        # So I've taken function to solve the problem from
        # Note, that function should be created before using this operator
        if not for_update:
            tpl = '%s'
        elif get_postgres_version() < (9, 5) and isinstance(field, JSONField):
            tpl = '{0}(COALESCE(%s, %s), %s)'.format(Postgres94MergeJSONBMigration.FUNCTION_NAME)
        else:
            tpl = 'COALESCE(%s, %s) || %s'

        if val_as_param:
            val_sql, params = self.format_field_value(field, val, connection)
        else:
            val_sql, params = str(val), tuple()

        if not for_update:
            return tpl % val_sql, params
        else:
            return tpl % (self._get_field_column(field, with_table=with_table), null_default, val_sql),\
                   null_default_params + params


class UnionSetFunction(AbstractSetFunction):
    names = {'union'}

    supported_field_classes = {'ArrayField'}

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        if for_update:
            sub_func = ConcatSetFunction()
            sql, params = sub_func.get_sql_value(field, val, connection, val_as_param=val_as_param,
                                                 with_table=with_table, **kwargs)
            sql = 'ARRAY(SELECT DISTINCT UNNEST(%s))' % sql
        else:
            sql, params = val, []

        return sql, params


class ArrayRemoveSetFunction(AbstractSetFunction):
    names = {'array_remove'}

    supported_field_classes = {'ArrayField'}

    def format_field_value(self, field, val, connection, cast_type=False, **kwargs):
        # Support for django 1.8
        if not hasattr(field.base_field, 'model'):
            field.base_field.model = field.model

        return format_field_value(field.base_field, val, connection, cast_type=cast_type)

    def modify_create_params(self, model, key, kwargs):
        if kwargs.get(key):
            kwargs[key] = model._meta.get_field(key).get_default()

        return kwargs

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        if val_as_param:
            val_sql, params = self.format_field_value(field, val, connection)
        else:
            val_sql, params = str(val), tuple()

        if for_update:
            sql = 'array_remove({0}, {1})'.format(self._get_field_column(field, with_table=with_table), val_sql)
        else:
            sql, params = self.format_field_value(field, field.get_default(), connection)

        return sql, params


class NowSetFunction(AbstractSetFunction):
    names = {'now', 'NOW'}
    supported_field_classes = {'DateField', 'DateTimeField'}
    needs_value = False

    def __init__(self, if_null=False):  # type: (bool) -> None
        self._if_null = if_null
        super(NowSetFunction, self).__init__()

    def get_sql_value(self, field, val, connection, val_as_param=True, with_table=False, for_update=True, **kwargs):
        if for_update and self._if_null:
            default_value, default_params = self._get_field_column(field, with_table=with_table), tuple()
            return "COALESCE(%s, NOW())" % default_value, default_params
        else:
            return 'NOW()', tuple()
