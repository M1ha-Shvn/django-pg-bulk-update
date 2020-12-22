#!/usr/bin/env python

"""
This suite runs tests in django environment. See:
https://docs.djangoproject.com/en/1.11/topics/testing/advanced/#using-the-django-test-runner-to-test-reusable-applications
"""

import os
import sys

import django
from django.conf import settings
from django.test.utils import get_runner

if __name__ == "__main__":
    print('Django: ', django.VERSION)
    print('Python: ', sys.version)

    # Add the src directory to sys.path
    curdir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(curdir + "/src")

    print('sys.path: ', sys.path)

    # Setup test labels
    if len(sys.argv) > 1:
        test_labels = sys.argv[1:]
    else:
        test_labels = ["tests"]

    os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'
    django.setup()
    TestRunner = get_runner(settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests(test_labels)
    sys.exit(bool(failures))
