# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2018-04-17 09:30
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scraper', '0002_auto_20180417_0918'),
    ]

    operations = [
        migrations.AddField(
            model_name='location',
            name='name',
            field=models.CharField(default='loc', max_length=255),
            preserve_default=False,
        ),
    ]
