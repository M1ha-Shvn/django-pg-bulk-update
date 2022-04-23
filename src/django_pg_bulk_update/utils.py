"""
Contains some project unbind helpers
"""
import logging
from itertools import islice
from time import sleep
from typing import TypeVar, Set, Any, Tuple, Iterable, Callable, Optional, List

from django.core.exceptions import FieldError
from django.db.models import Field
from django.db.models.sql.subqueries import UpdateQuery
from importlib import import_module

from .compatibility import hstore_serialize, hstore_available, get_field_db_type, import_pg_field_or_dummy
from .types import TDatabase

logger = logging.getLogger('django-pg-bulk-update')

T = TypeVar('T')


def get_subclasses(cls, recursive=False):  # type: (T, bool) -> Set[T]
    """
    Gets all subclasses of given class
    Attention!!! Classes would be found only if they were imported before using this function
    :param cls: Class to get subclasses
    :param recursive: If flag is set, returns subclasses of subclasses and so on too
    :return: A list of subclasses
    """
    subclasses = set(cls.__subclasses__())

    if recursive:
        for sub_cls in subclasses.copy():
            subclasses.update(get_subclasses(sub_cls, recursive=True))

    return subclasses


def format_field_value(field, val, conn, cast_type=False):
    # type: (Field, Any, TDatabase, bool) -> Tuple[str, Tuple[Any]]
    """
    Formats value, according to field rules
    :param field: Django field to take format from
    :param val: Value to format
    :param conn: Connection used to update data
    :param cast_type: Adds type casting to sql if flag is True
    :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
    """
    # This content is a part, taken from django.db.models.sql.compiler.SQLUpdateCompiler.as_sql()
    # And modified for our needs
    query = UpdateQuery(field.model)
    compiler = query.get_compiler(connection=conn)
    HStoreField = import_pg_field_or_dummy('HStoreField', hstore_available)  # noqa

    if hasattr(val, 'resolve_expression'):
        val = val.resolve_expression(query, allow_joins=False, for_save=True)
        if val.contains_aggregate:
            raise FieldError(
                'Aggregate functions are not allowed in this query '
                '(%s=%r).' % (field.name, val)
            )
        if val.contains_over_clause:
            raise FieldError(
                'Window expressions are not allowed in this query '
                '(%s=%r).' % (field.name, val)
            )
    elif hasattr(val, 'prepare_database_save'):
        if field.remote_field:
            val = field.get_db_prep_save(val.prepare_database_save(field), connection=conn)
        else:
            raise TypeError(
                "Tried to update field %s with a model instance, %r. "
                "Use a value compatible with %s."
                % (field, val, field.__class__.__name__)
            )
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

        # django 2.2 adds ::serial[] to placeholders for arrays...
        placeholder = placeholder.split('::')[0]
    else:
        placeholder = '%s'

    if hasattr(val, 'as_sql'):
        sql, update_params = compiler.compile(val)
        value = placeholder % sql
    elif val is not None:
        value, update_params = placeholder, (val,)
    else:
        value, update_params = 'NULL', tuple()

    if cast_type:
        value = 'CAST(%s AS %s)' % (value, get_field_db_type(field, conn))

    return value, tuple(update_params)


def batch(data, batch_size):  # type: (Iterable[T], int) -> Iterable[Tuple[T, ...]]
    """
    Splits Iterable data (can be a generator) into tuples with length less or equal to batch_size
    In fact, this function is reverse for itertools.chain(...)
    :param data: Iterable of any type
    :param batch_size: Expected maximum tuple size
    :return: Tuples with length <= batch_size
    """
    if type(batch_size) is not int:
        raise TypeError("batch_size must be positive integer")

    elif batch_size <= 0:
        raise ValueError("batch_size must be positive integer")

    it = iter(data)

    next_batch = tuple(islice(it, 0, batch_size))
    while len(next_batch) == batch_size:
        yield next_batch
        next_batch = tuple(islice(it, 0, batch_size))

    if next_batch:
        yield next_batch


def batched_operation(handler, data, batch_size=None, batch_delay=0, args=(), kwargs=None, data_arg_index=0):
    # type: (Callable, Iterable, Optional[int], float, Iterable, Optional[dict], int) -> List[Any]
    """
    Splits data to batches, configured by batch_size parameter and executes handler on each of them
    Makes a delay between every batch.
    Batch is passed as first handler parameter.
    :param handler: A callable to apply to a batch
    :param data: Data to process. Must be iterable. If dict, will be split to dicts by keys.
    :param batch_size: Size of parts to split data to.
    :param batch_delay: Delay between batches handling in seconds
    :param args: Additional arguments to pass to handler
    :param kwargs: Additional arguments to pass to handler
    :param data_arg_index: If data is not first argument (by default), you can pass its index in args here.
        Note, that args must contain any placeholder value, which will be replaced by batch data
    :return: A list of results for each batch
    """
    if type(batch_delay) not in {int, float}:
        raise TypeError("batch_delay must be non negative float")
    elif batch_delay < 0:
        raise ValueError("batch_delay must be non negative float")

    if type(data_arg_index) is not int:
        raise TypeError("data_arg_num must be integer between 0 and len(args)")
    elif not 0 <= data_arg_index < len(tuple(args)):
        raise ValueError("data_arg_num must be integer between 0 and len(args)")

    def _batches_iterator():
        if batch_size is None:
            yield data
        elif isinstance(data, dict):
            for b in batch(data.items(), batch_size):
                yield dict(b)
        else:
            for b in batch(data, batch_size):
                yield b

    results = []
    args = list(args)
    kwargs = kwargs or {}
    for j, batch_items in enumerate(_batches_iterator()):
        logger.debug('Processing batch %d with size %d' % (j + 1, len(batch_items)))
        args[data_arg_index] = batch_items
        r = handler(*args, **kwargs)
        results.append(r)
        sleep(batch_delay)

    return results


def is_auto_set_field(field):  # type: (Field) -> bool
    """
    Checks if model fields should be set automatically if absent in values
    :param field: Model field instance
    :return: Boolean
    """
    return getattr(field, 'auto_now', False) or getattr(field, 'auto_now_add', False)


def lazy_import(class_obj):  # (str) -> Optional[Any]
    """
    Import field class by dot separated import path
    :param class_obj: Already imported class or dot separated import path
    :return: Class or None if class has not been imported
    """
    if not isinstance(class_obj, str):
        return class_obj

    module_path, cls = class_obj.rsplit('.', 1)
    try:
        module = import_module(module_path)
    except ImportError:
        return None

    return getattr(module, cls, None)
