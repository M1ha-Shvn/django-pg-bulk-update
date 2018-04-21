#!/usr/bin/env python

"""
This bunch of code is run separately from unittests.
It is written in order to test performance of library on big number of records.
"""
import datetime
import os
import sys

import django
from django.db import connection


class AbstractPerformanceTest(object):
    @classmethod
    def init_data(cls, count=1000):
        """
        Creates sample data
        :return: None
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS tests_testmodel (
          id INTEGER PRIMARY KEY,
          name VARCHAR(255),
          int_field INTEGER,
          array_field INTEGER[],
          json_field jsonb,
          hstore_field hstore
        )"""
        cursor = connection.cursor()
        cursor.execute(create_sql)

        data = [
            TestModel(name=str(i), id=i + 1, int_field=i)
            for i in range(count)
        ]
        TestModel.objects.bulk_create(data)

    @classmethod
    def drop_data(cls):
        """
        Cleans test table between tests
        :return: None
        """
        cursor = connection.cursor()
        cursor.execute('DROP TABLE "%s"' % TestModel._meta.db_table)

    @classmethod
    def get_time(cls):
        # type: () -> float
        """
        Returns current time in seconds
        :return:
        """
        return datetime.datetime.now().timestamp()

    @classmethod
    def test(self):  # type: () -> None
        """
        This method will define test content
        :return:
        """
        raise NotImplementedError("test is not implemented")

    @classmethod
    def run_test(cls):  # type: () -> float
        """
        This is test wrapper, which creates and drops data between tests and measures execution time
        :return: Execution time in seconds
        """
        cls.init_data()
        start = cls.get_time()
        try:
            cls.test()
        finally:
            end = cls.get_time()
            cls.drop_data()
        return end - start


class BulkUpdateTest(AbstractPerformanceTest):
    upd_data = [{'int_field': i + 2, 'id': i + 1} for i in range(1000)]

    @classmethod
    def test(cls):
        from tests.models import TestModel
        bulk_update(TestModel, cls.upd_data)


class SingleUpdateTest(AbstractPerformanceTest):
    upd_data = [{'int_field': i + 2, 'id': i + 1} for i in range(1000)]

    @classmethod
    def test(cls):
        from tests.models import TestModel
        for item in cls.upd_data:
            TestModel.objects.filter(id=item['id']).update(int_field=item['int_field'])


if __name__ == "__main__":
    print('Django: ', django.VERSION)
    print('Python: ', sys.version)
    os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.test_settings'
    django.setup()

    # Django imports must be done after init
    from tests.models import TestModel
    from django_pg_bulk_update.utils import get_subclasses
    from django_pg_bulk_update import bulk_update

    tests = get_subclasses(AbstractPerformanceTest)
    for test_cls in tests:
        res = test_cls.run_test()
        print("Test `%s` executed in %.2f seconds" % (test_cls.__name__, res))
