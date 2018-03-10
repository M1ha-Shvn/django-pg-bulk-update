# django-pg-bulk-update
Django extension to update multiple table records with similar (but not equal) conditions in efficient way on PostgreSQL

## Requirements
* Python 2.7 or Python 3.3+
* django >= 1.7  
  Previous versions may also work, but haven't been tested.  
  django.postgres.contrib fields are also supported (available since django 1.8)
  django.postgres.contrib.JSONField is supported since django 1.9  
* pytz
* six
* typing
* psycopg2
* PostgreSQL 9.2+   
  Previous versions may also work, but haven't been tested.  
  JSONB operations are available for PostgreSQL 9.4+.

## Installation
Install via pip (not working, TODO):  
`pip install django-pg-bulk-update`    
or via setup.py:  
`python setup.py install`

## Usage
You can make queries in 2 ways:
* Declaring a custom manager for your model
* Calling query functions directly

### Query functions
There are 3 query helpers in this library. There parameters are unified and described in the section below.  

* `bulk_update(model, values, key_fields='id', using=None, set_functions=None, key_fields_ops=(), batch_size=None, batch_delay=0)`  
    This function updates multiple records of given model in single database query.  
    Functions forms raw sql query for PostgreSQL. It's work is not guaranteed on other databases.  
    Function returns number of updated records.
    
* `bulk_update_or_create(model, values, key_fields='id', using=None, set_functions=None, update=True, batch_size=None, batch_delay=0)`  
    This function finds records by key_fields. It creates not existing records with data, given in values.   
    If `update` flag is set, it updates existing records with data, given in values.  
    Update is performed with bulk_udpate function above, so function work is not guaranteed on PostgreSQL only.  
    
    Function is done in transaction in 3 queries:  
    + Search for existing records  
    + Create not existing records (if values have any)  
    + Update existing records (if values have any and `update` flag is set)
      
    Function returns a tuple, containing number of records inserted and records updated.
    
* `pdnf_clause(key_fields, field_values, key_fields_ops=())`  
  Pure django implementation of principal disjunctive normal form. It is base on combining Q() objects.  
  Condition will look like:
  ```sql
    SELECT ... WHERE (a = x AND b = y AND ...) OR (a = x1 AND b = y1  AND ...) OR ...
  ```
  Function returns a [django.db.models.Q](https://docs.djangoproject.com/en/2.0/topics/db/queries/#complex-lookups-with-q-objects) instance  


### Function parameters
* `model: Type[Model]`
    A subclass of django.db.models.Model to update
    
* `values: Union[Union[TUpdateValuesValid, Dict[Any, Dict[str, Any]]], Iterable[Dict[str, Any]]]`    
    Data to update. All items must update same fields!!!    
    Parameter can have one of 2 forms:    
    + Iterable of dicts. Each dict contains both key and update data. Each dict must contain all key_fields as keys.
        You can't update key_fields with this format.
    + Dict of key_values: update_fields_dict    
        You can use this format to update key_fields
        - key_values can be tuple or single object. If tuple, key_values length must be equal to key_fields length.
         If single object, key_fields is expected to have 1 element
        - update_fields_dict is a dictionary {field_name: update_value} to update
        
* `key_fields: Union[str, Iterable[str]]`
  Optional. Field names, which are used as update conditions.
  Parameter can have one of 2 forms:
  + String for single key field. Primary key is used by default.
  + Iterable of strings for multiple key fields.
  
* `using: Optional[str]`  
  Optional. Database alias to query. If not set, 'default' database is used.
  
* `set_functions: Optional[Dict[str, Union[str, AbstractSetFunction]]]`  
  Optional. Functions which will be used to set values.  
  If given, it should be a dictionary:
  + Key is a field name, function is applied to
  + Value is a function alias name or AbstractSetFunction instance.  
    Available function aliases:
    - 'eq', '='  
      Simple assign operator. It used by default for fields that are not mentioned in the dict.  
    - 'incr', '+'  
      Adds field value to previous one. It can be used for all numeric database types.   
    - 'concat', '||'  
      Concatenates field value to previous one. It can be used for string types, JSONField, HStoreField, ArrayField.
    - 'eq_not_null'  
      This function can be used, if you want to update value only if it is not None.
    - You can define your own set function. See section below.
  
    Increment and concatenate functions concern NULL as default value. You can see default values in sections below.
    
* `key_field_ops: Union[Dict[str, Union[str, AbstractClauseOperator]], Iterable[Union[str, AbstractClauseOperator]]]`
    Optional. Operators, which are used to fined records for update. Operators are applied to `key_fields`.  
    If some fields are not given, equality operator is used.
    `bulk_update_or_create` function always uses equality operator
    Parameter can have one of 2 forms:  
    - Iterable of operator alias names or AbstractClauseOperator instances.
      Order of iterable must be the same as key_fields.
    - Dictionary:
      + Key is a field name, function is applied to
      + Value is a function alias name of set function or AbstractSetFunction instance.  
    Available name aliases:
    - 'eq', '=', '=='
      Simple equality condition. It is used by default.
    - '!eq', '!=', '<>'
      Not equal operator
    - 'in'
      Searches for records, which have field from values list. Value should be an iterable of correct field values.
    - '!in'
      Searches for records, which have field not from values list. Value should be an iterable of correct field values.
    - 'lt', '<'
    - 'lte', '<='
    - 'gt', '>'
    - 'gte', '>='
    - You can define your own clause operation. See section below.
    
* `batch_size: Optional[int]`  
    If this parameter is set, values are split into batches of given size. Each batch is processed separately.
    Note that batch_size != number of records processed if you use key_field_ops other than 'eq'
    
* `batch_delay: float`  
   If batch_size is set, this parameter sets time to sleep in seconds between batches execution
    
* `update: bool`  
    If flag is not set, bulk_update_or_create function will not update existing records, only creating not existing. 
    
* `field_values: Iterable[Union[Iterable[Any], dict]]`  
    Field values to use in `pdnf_clause` function. They have simpler format than update functions.
    It can come in 2 formats:  
    + An iterable of tuples in key_fields order `( (x, y), (x1, y1), ...)`
    + An iterable of dicts with field name as key `({'a': x, 'b': y}, ...)`
    

### Examples
```python
from django.db import models
from djngo_pg_bulk_udpate import bulk_update, bulk_update_or_create, pdnf_clause

# Test model
class TestModel(models.Model):
    name = models.CharField(max_length=50)
    int_field = models.IntegerField()

# Create test data
TestModel.objects.bulk_create([TestModel(pk=i, name="item%d" % i, int_field=1) for i in range(1, 4)])

# Update by id field
updated = bulk_update(TestModel, [{
    "id": 1,
    "name": "updated1",
}, {
    "id": 2,
    "name": "updated2"
}])

print(updated)
# Outputs: 2

# Call update by name field
updated = bulk_update(TestModel, {
    "updated1": {
        "int_field": 2
    },
    "updated2": {
        "int_field": 3
    }
}, key_fields="name")

print(updated)
# Outputs: 2

print(list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))
# Outputs: [
#     {"id": 1, "name": "updated1", "int_field": 2},
#     {"id": 2, "name": "updated2", "int_field": 3},
#     {"id": 3, "name": "item3", "int_field": 1}
# ]

# Increment int_field by 3 and set name to 'incr' for records where id >= 2 and int_field < 3
updated = bulk_update(TestModel, {
    (2, 3): {
        "int_field": 3,
        "name": "incr"
    }
}, key_fields=['id', 'int_field'], key_fields_ops={'int_field': '<', 'id': 'gte'}, set_functions={'int_field': '+'})

print(updated)
# Outputs: 1

print(list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))
# Outputs: [
#     {"id": 1, "name": "updated1", "int_field": 2},
#     {"id": 2, "name": "updated2", "int_field": 3},
#     {"id": 3, "name": "incr", "int_field": 4}
# ]
 
 
inserted, updated = bulk_update_or_create(TestModel, [{
    "id": 3,
    "name": "_concat1",
    "int_field": 4
}, {
    "id": 4,
    "name": "concat2",
    "int_field": 5
}], set_functions={'name': '||'})

print(inserted, updated)
# Outputs: 1, 1

print(list(TestModel.objects.all().order_by("id").values("id", "name", "int_field")))
# Outputs: [
#     {"id": 1, "name": "updated1", "int_field": 2},
#     {"id": 2, "name": "updated2", "int_field": 3},
#     {"id": 3, "name": "incr_concat1", "int_field": 4},
#     {"id": 4, "name": "concat2", "int_field": 5},
# ]

# Find records where 
# id IN [1, 2, 3] AND name = 'updated2' OR id IN [3, 4, 5] AND name = 'concat2' OR id IN [2, 3, 4] AND name = 'updated1'
        cond = pdnf_clause(['id', 'name'], [([1, 2, 3], 'updated2'),
                                            ([3, 4, 5], 'concat2'),
                                            ([2, 3, 4], 'updated1')], key_fields_ops={'id': 'in'})
data = TestModel.objects.filter(cond).order_by('int_field').values_list('int_field', flat=True)
print(list(data))
# Outputs: [3, 5]
```

### Using custom manager
In order to simplify using `bulk_update` and `bulk_update_or_create` functions, you can use a custom manager.  
It automatically fills `model` and `using` parameters. `using` extracts managers' write database. 
You can change database to use with [Manager.db_manager()](https://docs.djangoproject.com/en/2.0/topics/db/multi-db/#using-managers-with-multiple-databases) method.  
The rest parameters are the same as above.  
Example:
```python
from django.db import models
from djngo_pg_bulk_udpate.manager import BulkUpdateManager, BulkUpdateManagerMixin

# Test model
class TestModel(models.Model):
    objects = BulkUpdateManager()
    
    name = models.CharField(max_length=50)
    int_field = models.IntegerField()
    
# Now you can use functions like:

TestModel.objects.bulk_update([
    # Any data here
], key_fields='id', set_functions=None, key_fields_ops=())

TestModel.objects.bulk_update_or_create([
    # Any data here
], key_fields='id', set_functions=None, update=True)           
```

If you already have a manager, you can inherit it from BulkUpdateManagerMixin:
```python
from django.db import models
from djngo_pg_bulk_udpate.manager import BulkUpdateManagerMixin


class CustomManager(models.Manager, BulkUpdateManagerMixin):
    pass
    
    
# Test model
class TestModel(models.Model):
    objects = CustomManager()
    
    name = models.CharField(max_length=50)
    int_field = models.IntegerField()
```

### Custom clause operation
You can define your own clause operator, creating `AbstractClauseOperator` subclass and implementing:
* `names` attribute
* `def get_django_filter(self, name)` method
* One of `def get_sql_operator(self)` or `def get_sql(self, table_field, value)`
  When clause is formed, it calls `get_sql()` method.
  In order to simplify method usage of simple `field <op> value` operators,
  by default `get_sql()` forms this condition, calling  `get_sql_operator()` method, which returns <op>.
  
Optionally, you can change `def format_field_value(self, field, val, connection, **kwargs)` method,
which formats value according to field rules

Example:
```python
from django_pg_bulk_update import bulk_update
from django_pg_bulk_update.clause_operators import AbstractClauseOperator

class LTClauseOperator(AbstractClauseOperator):
    names = {'lt', '<'}

    def get_django_filter(self, name):  # type: (str) -> str
        """
        This method should return parameter name to use in django QuerySet.fillter() kwargs
        :param name: Name of parameter
        :return: String with filter
        """
        return '%s__lt' % name

    def get_sql_operator(self):  # type: () -> str
        """
        If get_sql operator is simple binary operator like "field <op> val", this functions returns operator
        :return: str
        """
        return '<'
        

# Usage examples
# import you function here before calling an update
bulk_update(TestModel, [], key_field_ops={'int_field': 'lt'})
bulk_update(TestModel, [], key_field_ops={'int_field': LTClauseOperator()})
```

You can use class instance directly in `key_field_ops` parameter or use its aliases from `names` attribute.  
When update function is called, it searches for all imported AbstractClauseOperator subclasses and takes first class
which contains alias in `names` attribute.

### Custom set function
You can define your own set function, creating `AbstractSetFunction` subclass and implementing:
* `names` attribute
* `supported_field_classes` attribute
* `def get_sql(self, field, val, connection, val_as_param=True, **kwargs)` method

Optionally, you can change `def format_field_value(self, field, val, connection, **kwargs)`
method, if input data needs special formatting.  

Example:  

```python
from django_pg_bulk_update import bulk_update
from django_pg_bulk_update.set_functions import AbstractSetFunction

class CustomSetFunction(AbstractSetFunction):
    # Set function alias names
    names = {'func_alias_name'}

    # Names of django field classes, this function supports. You can set None (default) to support any field.
    supported_field_classes = {'IntegerField', 'FloatField', 'AutoField', 'BigAutoField'}

    def get_sql(self, field, val, connection, val_as_param=True, **kwargs):
        # type: (Field, Any, DefaultConnectionProxy, bool, **Any) -> Tuple[str, Tuple[Any]]
        """
        Returns function sql and parameters for query execution
        :param field: Django field to take format from
        :param val: Value to format
        :param connection: Connection used to update data
        :param val_as_param: If flag is not set, value should be converted to string and inserted into query directly.
            Otherwise a placeholder and query parameter will be used
        :param kwargs: Additional arguments, if needed
        :return: A tuple: sql, replacing value in update and a tuple of parameters to pass to cursor
        """
        # If operation is incremental, it should be ready to get NULL in database
        null_default, null_default_params = self._parse_null_default(field, connection, **kwargs)
        
        # Your function/operator should be defined here
        tpl = '"%s" = COALESCE("%s", %s) + %s'

        if val_as_param:
            sql, params = self.format_field_value(field, val, connection)
            return tpl % (field.column, field.column, null_default, sql), null_default_params + params
        else:
            return tpl % (field.column, field.column, null_default, str(val)), null_default_params
            
            
# Usage examples
# import you function here before calling an update
bulk_update(TestModel, [], set_functions={'int_field': 'func_alias_name'})
bulk_update(TestModel, [], set_functions={'int_field': CustomSetFunction()})
```

You can use class instance directly in `set_functions` parameter or use its aliases from `names` attribute.  
When update function is called, it searches for all imported AbstractSetFunction subclasses and takes first class
which contains alias in `names` attribute.


## Compatibility
Library supports django.contrib.postgres.fields:  
+ ArrayField  
+ JSONField  
+ HStoreField  

Note that ArrayField and HStoreField are available since django 1.8, JSONField - since django 1.9.  
Also PostgreSQL before 9.4 doesn't support jsonb, and so - JSONField.  
PostgreSQL 9.4 supports JSONB, but doesn't support concatenation operator (||).
In order to support this set function a special function for postgres 9.4 was written. Add a migration to create it:

```python
from django.db import migrations,
from django_pg_bulk_update.compatibility import Postgres94MergeJSONBMigration

class Migration(migrations.Migration):
    dependencies = []

    operations = [
        Postgres94MergeJSONBMigration()
    ]
```

## [django-bulk-update](https://github.com/aykut/django-bulk-update) difference
Pros:
* bulk_update_or_create() method
* Ability to use complex set functions
* Ability to use complex conditions
* pdnf_clause helper
* Django 1.7 support
* Ability to make delay between batches

Corns:
* PostgreSQL only

