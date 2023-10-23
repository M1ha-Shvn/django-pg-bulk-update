from datetime import date

from django.db.models import Value, Q, BooleanField
from django.template.backends import django
from django.utils.timezone import now
from django.utils.tree import Node

from django_pg_bulk_update.compatibility import get_postgres_version


def get_auto_now_date(key_is_unique=True):  # type: (bool) -> date
    """
    Django generates auto_now for DateField as datetime.date.today(),
       not looking at django selected timezone.
     That can cause curious locale bugs as NOW() database function uses django's locale
     P. s. DateTimeField uses django.utils.timezone.now(), and behaves correctly
     Django ticket: https://code.djangoproject.com/ticket/32320#ticket
    :param key_is_unique: if key_is_unique flag is used while calling bulk_update
    :return: Date object
    """
    return date.today() if not key_is_unique or get_postgres_version() < (9, 5) else now().date()


class EmptyQ(Q):
    """
    Empty condition should return empty result
    See https://stackoverflow.com/questions/35893867/always-false-q-object
    """
    def __init__(self):
        Node.__init__(self, children=[
            ("pk", Value(False, output_field=BooleanField()))
        ], connector=None, negated=False)

    def __str__(self):
        # Django before 3.0 raises 'TypeError: cannot unpack non-iterable Value object'
        #   when trying to insert Q(Value(False, output_field=BooleanField())) to
        return 'FALSE'


def get_empty_q_object() -> Q:
    """
    Generates Q-Object, which leads to empty QuerySet.
      See https://stackoverflow.com/questions/35893867/always-false-q-object
    """
    import django
    if django.VERSION >= (3,):
        return Q(Value(False, output_field=BooleanField()))

    # Django before 3.0 doesn't work with not binary conditions and expects field name to be always present.
    #   It raises TypeError: cannot unpack non-iterable Value object.
    #   This condition raises EmptyResultSet while forming query and doesn't even execute it
    return Q(pk__in=[])
