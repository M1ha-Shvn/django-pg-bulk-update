"""
This function contains operators used in WHERE query part
"""
from typing import Type, Optional, Any, Iterable, Dict

from .compatibility import array_available, get_field_db_type
from .types import AbstractFieldFormatter
from .utils import get_subclasses


class AbstractClauseOperator(AbstractFieldFormatter):
    inverse = False
    names = set()
    requires_value = True

    def get_django_filters(self, name, value):
        # type: (str, Any) -> Dict[str, Any]
        """
        This method should return parameter name to use in django QuerySet.filter() kwargs
        :param name: Name of the parameter
        :param value: Value of the parameter
        :return: kwargs to pass to Q() object constructor
        """
        raise NotImplementedError("%s must implement get_django_filter method" % self.__class__.__name__)

    @classmethod
    def get_operator_by_name(cls, name):  # type: (str) -> Optional[Type[AbstractClauseOperator]]
        """
        Finds subclass of AbstractOperation applicable to given operation name
        :param name: String name to search
        :return: AbstractOperation subclass if found, None instead
        """
        try:
            return next(sub_cls for sub_cls in get_subclasses(cls, recursive=True) if name in sub_cls.names)
        except StopIteration:
            raise ValueError("Operator with name '%s' doesn't exist" % name)

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


class AbstractArrayValueOperator(AbstractClauseOperator):
    """
    Abstract class partial, that handles an array of field values as input
    """
    def format_field_value(self, field, val, connection, **kwargs):
        assert isinstance(val, Iterable), "'%s' value must be iterable" % self.__class__.__name__

        # With in operator we should pass array of values instead of single field value.
        # So let's validate it as Array of this field
        if array_available():
            from django.contrib.postgres.fields import ArrayField
            arr_field = ArrayField(field)
            arr_field.model = field.model
            return super(AbstractArrayValueOperator, self).format_field_value(arr_field, val, connection, **kwargs)
        else:
            # This means, we use django < 1.8. Try converting it manually
            db_type = get_field_db_type(field, connection)
            tpl = "ARRAY[%s]::%s[]"
            val = list(val)

            placeholders, values = [], []
            for item in val:
                p, v = super(AbstractArrayValueOperator, self).format_field_value(field, item, connection, **kwargs)
                placeholders.append(p)
                values.extend(v)

            query = tpl % (', '.join(placeholders), db_type)
            return query, values


class EqualClauseOperator(AbstractClauseOperator):
    names = {'eq', '=', '=='}

    def get_django_filters(self, name, value):
        return {name: value}

    def get_sql_operator(self):
        return '='


class NotEqualClauseOperator(EqualClauseOperator):
    names = {'!eq', '!=', '<>'}
    inverse = True

    def get_sql_operator(self):
        return '!='


class IsNullClauseOperator(AbstractClauseOperator):
    names = {'is_null', 'isnull'}
    requires_value = False

    def format_field_value(self, field, val, connection, cast_type=False, **kwargs):
        tpl = 'CAST(%s AS bool)' if cast_type else '%s'
        return tpl, [bool(val)]

    def get_django_filters(self, name, value):
        return {'%s__isnull' % name: value}

    def get_sql(self, table_field, value):
        return '%s IS NULL AND %s OR %s IS NOT NULL AND NOT %s' % (table_field, value, table_field, value)

    def get_sql_operator(self):
        raise NotImplementedError("%s implements get_sql method, this method shouldn't be called"
                                  % self.__class__.__name__)


class InClauseOperator(AbstractArrayValueOperator):
    names = {'in'}
    django_operation = 'in'

    def get_django_filters(self, name, value):
        return {'%s__in' % name: value}

    def get_sql_operator(self):  # type: () -> str
        raise NotImplementedError("%s implements get_sql method, this method shouldn't be called"
                                  % self.__class__.__name__)

    def get_sql(self, table_field, value):
        # We can't simply use in as format_field_value will return ARRAY, not set of records
        return '%s = ANY(%s)' % (table_field, value)


class NotInClauseOperation(InClauseOperator):
    names = {'!in'}
    inverse = True

    def get_sql_operator(self):
        return super(NotInClauseOperation, self).get_sql_operator()

    def get_sql(self, table_field, value):
        return 'NOT %s' % super(NotInClauseOperation, self).get_sql(table_field, value)


class LTClauseOperator(AbstractClauseOperator):
    names = {'lt', '<'}

    def get_django_filters(self, name, value):
        return {'%s__lt' % name: value}

    def get_sql_operator(self):
        return '<'


class GTClauseOperator(AbstractClauseOperator):
    names = {'gt', '>'}

    def get_django_filters(self, name, value):
        return {'%s__gt' % name: value}

    def get_sql_operator(self):
        return '>'


class GTEClauseOperator(AbstractClauseOperator):
    names = {'gte', '>='}

    def get_django_filters(self, name, value):
        return {'%s__gte' % name: value}

    def get_sql_operator(self):
        return '>='


class LTEClauseOperator(AbstractClauseOperator):
    names = {'lte', '<='}

    def get_django_filters(self, name, value):
        return {'%s__lte' % name: value}

    def get_sql_operator(self):
        return '<='


class BetweenClauseOperator(AbstractArrayValueOperator):
    names = {'between'}

    def get_django_filters(self, name, value):
        assert isinstance(value, Iterable) and len(value) == 2, "value must be iterable of size 2"
        return {'%s__gte' % name: value[0], '%s__lte' % name: value[1]}

    def get_sql_operator(self):  # type: () -> str
        raise NotImplementedError("%s implements get_sql method, this method shouldn't be called"
                                  % self.__class__.__name__)

    def get_sql(self, table_field, value):  # type: (str, str) -> str
        # Postgres enumerates arrays from 1
        return "%s BETWEEN %s[1] AND %s[2]" % (table_field, value, value)
