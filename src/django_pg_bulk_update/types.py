from typing import Iterable, Union, Dict, Tuple, Any, Optional

from .clause_operators import AbstractClauseOperator
from .set_functions import AbstractSetFunction

TFieldNames = Union[str, Iterable[str]]

# TODO It's better to use more strict OrderedDict here
TOperatorsValid = Dict[str, AbstractClauseOperator]

TOperators = Union[Dict[str, Union[str, AbstractClauseOperator]], Iterable[Union[str, AbstractClauseOperator]]]
TUpdateValuesValid = Dict[Tuple[Any], Dict[str, Any]]
TUpdateValues = Union[Union[TUpdateValuesValid, Dict[Any, Dict[str, Any]]], Iterable[Dict[str, Any]]]
TSetFunctions = Optional[Dict[str, Union[str, AbstractSetFunction]]]
TSetFunctionsValid = Dict[str, AbstractSetFunction]
