"""
This file contains django settings to run tests with runtests.py
"""
from os import environ

SECRET_KEY = 'fake-key'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test',
        'USER': environ.get('PGUSER', 'test'),
        'PASSWORD': environ.get('PGPASS', 'test'),
        'HOST': environ.get('PGHOST', '127.0.0.1'),
        'PORT': environ.get('PGPORT', 5432)
    },
    'secondary': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test2',
        'USER': environ.get('PGUSER', 'test'),
        'PASSWORD': environ.get('PGPASS', 'test'),
        'HOST': environ.get('PGHOST', '127.0.0.1'),
        'PORT': environ.get('PGPORT', 5432)
    }
}

LOGGING = {
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django-pg-bulk-update': {
            'handlers': ['console'],
            'level': 'DEBUG'
        }
    }
}

# DATABASES should be defined before this call
from django_pg_bulk_update.compatibility import jsonb_available, array_available, hstore_available  # noqa: W292, E402

INSTALLED_APPS = []
USE_TZ = True

if hstore_available() or jsonb_available() or array_available():
    INSTALLED_APPS.append("django.contrib.postgres")

INSTALLED_APPS.extend([
    "src",
    "tests"
])

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
