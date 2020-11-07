"""
This file contains django settings to run tests with runtests.py
"""
SECRET_KEY = 'fake-key'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test',
        'USER': 'test',
        'PASSWORD': 'test',
        'HOST': '127.0.0.1',
        'PORT': '5432'
    },
    'secondary': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test2',
        'USER': 'test',
        'PASSWORD': 'test',
        'HOST': '127.0.0.1',
        'PORT': '5432'
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
from django_pg_bulk_update.compatibility import jsonb_available, array_available, hstore_available

INSTALLED_APPS = []
USE_TZ = True

if hstore_available() or jsonb_available() or array_available():
    INSTALLED_APPS.append("django.contrib.postgres")

INSTALLED_APPS.extend([
    "src",
    "tests"
])
