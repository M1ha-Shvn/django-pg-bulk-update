from typing import Iterable, Union, Dict, Tuple, Any, Optional, Type

from django.db import DefaultConnectionProxy
from django.db.models import Model, Field

TFieldNames = Union[str, Iterable[str]]

TOperator = Union[str, 'AbstractClauseOperator']  # noqa: F821
TOperatorsValid = Tuple['FieldDescriptor']

TOperators = Union[Dict[str, TOperator], Iterable[TOperator]]
TUpdateValuesValid = Dict[Tuple[Any], Dict[str, Any]]
TUpdateValues = Union[Union[TUpdateValuesValid, Dict[Any, Dict[str, Any]]], Iterable[Dict[str, Any]]]
TSetFunction = Union[str, 'AbstractSetFunction']  # noqa: F821
TSetFunctions = Optional[Dict[str, TSetFunction]]
TSetFunctionsValid = Tuple['FieldDescriptor']
TDatabase = Union[DefaultConnectionProxy]


class FieldDescriptor(object):
    """
    This class is added in order to make passing parameters in queries easier
    """
    __slots__ = ['name', 'auto_set', '_set_function', '_key_operator', '_prefix']

    def __init__(self, name, set_function=None, key_operator=None):
        # type: (str, TSetFunction, TOperator) -> None
        self.name = name
        self.set_function = set_function
        self.key_operator = key_operator
        self.auto_set = False
        self._prefix = ''

    def get_field(self, model):
        # type: (Type[Model]) -> Field
        """
        Returns model field, described by this descriptor
        :param model: django.db.models.Model subclass
        :return: django.db.fields.Field instance
        """
        return model._meta.get_field(self.name)

    @property
    def set_function(self):
        # type: () -> 'AbstractSetFunction'  # noqa: F821
        """
        Returns set_function for this field descriptor.
        :return: AbstractSetFunction instance
        """
        return self._set_function

    @set_function.setter
    def set_function(self, val):
        # type: (Union[None, str, 'AbstractSetFunction']) -> None
        """
        Changes set_function for this field_descriptor.
        :param val: Set function name or instance. Defaults to EqualSetFunction() if None is passed
        :return:
        """
        from .compatibility import string_types
        from .set_functions import EqualSetFunction, AbstractSetFunction

        if val is None:
            self._set_function = EqualSetFunction()
        elif isinstance(val, string_types):
            self._set_function = AbstractSetFunction.get_function_by_name(val)()
        elif isinstance(val, AbstractSetFunction):
            self._set_function = val
        else:
            raise TypeError("Invalid set function type: %s" % str(type(val)))

    @property
    def key_operator(self):
        # type: () -> 'AbstractClauseOperator'  # noqa: F821
        """
        Returns operator to use in comparison
        :return: AbstractKeyOperator instance
        """
        return self._key_operator

    @key_operator.setter
    def key_operator(self, val):
        # type: (Union[None, str, 'AbstractClauseOperator']) -> None
        """
        Sets comparison operator for this field descriptor
        :param val: String name of operator or AbstractClauseOperator instance.
            Defaults to EqualClauseOperator if None is passed
        :return: None
        """
        from .clause_operators import EqualClauseOperator, AbstractClauseOperator
        from .compatibility import string_types

        if val is None:
            self._key_operator = EqualClauseOperator()
        elif isinstance(val, string_types):
            self._key_operator = AbstractClauseOperator.get_operator_by_name(val)()
        elif isinstance(val, AbstractClauseOperator):
            self._key_operator = val
        else:
            raise TypeError("Invalid key operator type: %s" % str(type(val)))

    def set_prefix(self, prefix, index=None):  # type: (str, Optional[int]) -> None
        """
        Sets prefix to use in values query part. It is used to divide key fields from update and default fields
        :param prefix: Prefix to use
        :param index: field can be used more than once in conditions. Set this index to prevent duplicates.
        :return:
        """
        self._prefix = prefix
        if index is not None:
            self._prefix += '_%d' % index

    @property
    def prefixed_name(self):  # type: () -> str
        """
        Returns prefixed name of the field
        :return:
        """
        if self._prefix is None:
            raise ValueError('prefix has not been set yet')
        return "%s__%s" % (self._prefix, self.name)


class AbstractFieldFormatter(object):
    def format_field_value(self, field, val, connection, cast_type=False, **kwargs):
        # type: (Field, Any, TDatabase, bool, **Any) -> Tuple[str, Tuple[Any]]
        """
        Formats value, according to field rules
        :param field: Django field to take format from
        :param val: Value to format
        :param connection: Connection used to update data
        :param cast_type: Adds type casting to sql if flag is True
        :param kwargs: Additional arguments, if needed
        :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
        """
        from .utils import format_field_value
        return format_field_value(field, val, connection, cast_type=cast_type)
