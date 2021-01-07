from datetime import date

from django.utils.timezone import now

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
