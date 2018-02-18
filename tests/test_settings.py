"""
This file contains django settings to run tests with runtests.py
"""
SECRET_KEY = 'fake-key'

INSTALLED_APPS = [
    "django.contrib.postgres",
    "src",
    "tests"
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'test',
        'USER': 'test',
        'PASSWORD': 'test',
        'HOST': '127.0.0.1',
        'PORT': '5432'
    }
}