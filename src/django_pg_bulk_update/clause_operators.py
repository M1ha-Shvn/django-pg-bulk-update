"""
This function contains operators used in WHERE query part
"""
from django.db import DefaultConnectionProxy
from django.db.models import Field, Model
from typing import Type, Optional, Any, Tuple, Iterable

from django_pg_bulk_update.compatibility import array_available, get_field_db_type
from .utils import get_subclasses, format_field_value


class AbstractClauseOperator(object):
    inverse = False
    names = set()

    def get_django_filter(self, name):  # type: (str) -> str
        """
        This method should return parameter name to use in django QuerySet.fillter() kwargs
        :param name: Name of parameter
        :return: String with filter
        """
        raise NotImplementedError("%s must implement get_django_filter method" % self.__class__.__name__)

    @classmethod
    def get_operation_by_name(cls, name):  # type: (str) -> Optional[Type[AbstractClauseOperator]]
        """
        Finds subclass of AbstractOperation applicable to given operation name
        :param name: String name to search
        :return: AbstractOperation subclass if found, None instead
        """
        try:
            return next(sub_cls for sub_cls in get_subclasses(cls, recursive=True) if name in sub_cls.names)
        except StopIteration:
            raise AssertionError("Operator with name '%s' doesn't exist" % name)

    def get_sql_operator(self):  # type: () -> str
        """
        If get_sql operator is simple binary operator like "field <op> val", this functions returns operator
        :return: str
        """
        raise NotImplementedError("%s must implement get_sql_operator method" % self.__class__.__name__)

    def get_sql(self, table_field, value):  # type: (str, str) -> str
        """
        This method returns SQL of this operator.
        :param table_field: Table field string, already quoted and formatted on which clause is used
        :param value: Value string, already quoted and formatted. Can contain function calls.
        :return: String
        """
        return "%s %s %s" % (table_field, self.get_sql_operator(), value)

    def get_null_fix_sql(self, model, field_name, connection):
        # type: (Type[Model], str, DefaultConnectionProxy) -> str
        """
        Bug fix. Postgres wants to know exact type of field to save it
        This fake update value is used for each saved column in order to get it's type
        :param model: Django model subclass
        :param field_name: Name of field fix is got for
        :param connection: Database connection used
        :return: SQL string
        """
        db_table = model._meta.db_table
        field = model._meta.get_field(field_name)
        return '(SELECT "{key}" FROM "{table}" LIMIT 0)'.format(key=field.column, table=db_table)

    def format_field_value(self, field, val, connection, **kwargs):
        # type: (Field, Any, DefaultConnectionProxy, **Any) -> Tuple[str, Tuple[Any]]
        """
        Formats value, according to field rules
        :param field: Django field to take format from
        :param val: Value to format
        :param connection: Connection used to update data
        :param kwargs: Additional arguments, if needed
        :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
        """
        return format_field_value(field, val, connection)


class EqualClauseOperator(AbstractClauseOperator):
    names = {'eq', '=', '=='}

    def get_django_filter(self, name):
        return name

    def get_sql_operator(self):
        return '='


class NotEqualClauseOperator(EqualClauseOperator):
    names = {'!eq', '!=', '<>'}
    inverse = True

    def get_sql_operator(self):
        return '!='


class InClauseOperator(AbstractClauseOperator):
    names = {'in'}
    django_operation = 'in'

    def get_django_filter(self, name):
        return '%s__in' % name

    def get_sql_operator(self):
        return super(InClauseOperator, self).get_sql_operator()

    def get_sql(self, table_field, value):
        # We can't simply use in as format_field_value will return ARRAY, not set of records
        return '%s = ANY(%s)' % (table_field, value)

    def get_null_fix_sql(self, model, field_name, connection):
        field = model._meta.get_field(field_name)
        db_type = get_field_db_type(field, connection)
        return '(SELECT ARRAY[]::%s[] LIMIT 0)' % db_type

    def format_field_value(self, field, val, connection, **kwargs):
        assert isinstance(val, Iterable), "'%s' value must be iterable" % self.__class__.__name__

        # With in operator we should pass array of values instead of single field value.
        # So let's validate it as Array of this field
        if array_available():
            from django.contrib.postgres.fields import ArrayField
            arr_field = ArrayField(field)
            arr_field.model = field.model
            return super(InClauseOperator, self).format_field_value(arr_field, val, connection, **kwargs)
        else:
            # This means, we use django < 1.8. Try converting it manually
            db_type = get_field_db_type(field, connection)
            tpl = "ARRAY[%s]::%s[]"
            val = list(val)

            placeholders, values = [], []
            for item in val:
                p, v = super(InClauseOperator, self).format_field_value(field, item, connection, **kwargs)
                placeholders.append(p)
                values.append(v)

            query = tpl % (', '.join(placeholders), db_type)
            return query, values


class NotInClauseOperation(InClauseOperator):
    names = {'!in'}
    inverse = True

    def get_sql_operator(self):
        return super(NotInClauseOperation, self).get_sql_operator()

    def get_sql(self, table_field, value):
        return 'NOT %s' % super(NotInClauseOperation, self).get_sql(table_field, value)


class LTClauseOperator(AbstractClauseOperator):
    names = {'lt', '<'}

    def get_django_filter(self, name):
        return '%s__lt' % name

    def get_sql_operator(self):
        return '<'


class GTClauseOperator(AbstractClauseOperator):
    names = {'gt', '>'}

    def get_django_filter(self, name):
        return '%s__gt' % name

    def get_sql_operator(self):
        return '>'


class GTEClauseOperator(AbstractClauseOperator):
    names = {'gte', '>='}

    def get_django_filter(self, name):
        return '%s__gte' % name

    def get_sql_operator(self):
        return '>='


class LTEClauseOperator(AbstractClauseOperator):
    names = {'lte', '<='}

    def get_django_filter(self, name):
        return '%s__lte' % name

    def get_sql_operator(self):
        return '<='
