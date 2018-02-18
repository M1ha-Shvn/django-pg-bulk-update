# django-pg-bulk-update
Django extension to update multiple table records with similar (but not equal) conditions in efficient way

## Requirements
* Python 2.7 or Python 3.3+
* django >= 1.7
* pytz
* six
* typing
* psycopg2

## Installation
Install via pip:  
`pip install django-pg-bulk-update`    
or via setup.py:  
`python setup.py install`

## Usage
You can make queries in 2 ways:
* Declaring a custom manager for your model
* Calling query functions directly

### Query functions
There are 3 query helpers in this library:
* `bulk_update(model, values, key_fields='id', using=None, set_functions=None, key_fields_ops=())`    
    This function updates multiple records of given model in single database query.    
    ##### Parameters:
    + `model: Type[Model]`
        A subclass of django.db.models.Model to update
    + `values: Union[Union[TUpdateValuesValid, Dict[Any, Dict[str, Any]]], Iterable[Dict[str, Any]]]`    
        Data to update. All items must update same fields!!!    
        Parameter can have one of 2 forms:    
        + Iterable of dicts. Each dict contains both key and update data. Each dict must contain all key_fields as keys.
            You can't update key_fields with this format.
        + Dict of key_values: update_fields_dict    
            You can use this format to update key_fields
            - key_values can be tuple or single object. If tuple, key_values length must be equal to key_fields length.
             If single object, key_fields is expected to have 1 element
            - update_fields_dict is a dictionary {field_name: update_value} to update
    + `key_fields: Union[str, Iterable[str]]`
    + `using: Optional[str]`   
    + `set_functions: Optional[Dict[str, Union[str, AbstractSetFunction]]]`
    + `key_field_ops: Union[Dict[str, Union[str, AbstractClauseOperator]], Iterable[Union[str, AbstractClauseOperator]]]`
    ##### Examples:
    ```Python
    ```
* `bulk_update_or_create(model, values, key_fields='id', using=None, set_functions=None, update=True)`
* `pdnf_clause(field_names, field_values, operators=())`


### Using custom manager
TODO
Example:
```python
# TODO
            
```


### Performance
TODO

### django-bulk-update difference
TODO

