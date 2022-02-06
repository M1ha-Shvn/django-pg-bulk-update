# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-12-26 11:00
from __future__ import unicode_literals

from uuid import uuid4

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('tests', '0003_add_test_model_with_schema')
    ]

    operations = [
        migrations.CreateModel(
            name='UUIDFieldPrimaryModel',
            fields=[
                ('id', models.UUIDField(auto_created=False, primary_key=True, serialize=False, verbose_name='ID',
                                        default=uuid4)),
                ('key_field', models.IntegerField(unique=True)),
                ('char_field', models.CharField(default=str, max_length=10, blank=True)),
            ],
            options={
                'abstract': False
            }
        )
    ]
