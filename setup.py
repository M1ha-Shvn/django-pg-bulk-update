from setuptools import setup

requires = []
with open('requirements.txt') as f:
    for line in f.readlines():
        line = line.strip()  # Remove spaces
        line = line.split('#')[0]  # Remove comments
        if line:  # Remove empty lines
            requires.append(line)

setup(
    name='django-pg-bulk-update',
    version='1.1.0',
    packages=['django_pg_bulk_update'],
    package_dir={'': 'src'},
    url='https://github.com/M1hacka/django-pg-bulk-update',
    license='BSD 3-clause "New" or "Revised" License',
    author='Mikhail Shvein',
    author_email='work_shvein_mihail@mail.ru',
    description='Django extension, executing bulk update operations for PostgreSQL',
    requires=requires
)
