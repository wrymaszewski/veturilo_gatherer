# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2018-04-23 08:25
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scraper', '0010_auto_20180419_1253'),
    ]

    operations = [
        migrations.AlterField(
            model_name='snapshot',
            name='timestamp',
            field=models.DateTimeField(),
        ),
    ]