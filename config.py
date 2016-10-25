# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import importlib


def get_config():
    settings = None
    try:
        if 'CASSANDRA_SETTINGS' in os.environ:
            settings = importlib.import_module(os.environ['CASSANDRA_SETTINGS'])
    except Exception:
        click.secho('Unable to load settings module!', fg='red')
        sys.exit()

    # Check configuration
    config = {}
    if 'CASSANDRA_SEEDS' in os.environ:
        config['seeds'] = os.environ['CASSANDRA_SEEDS']
    else:
        if hasattr(settings, 'CASSANDRA_SEEDS'):
            config['seeds'] = settings.CASSANDRA_SEEDS
        else:
            click.secho('settings.CASSANDRA_SEEDS is missing!', fg='red')
            sys.exit()
    if 'CASSANDRA_KEYSPACE' in os.environ:
        config['keyspace'] = os.environ['CASSANDRA_KEYSPACE']
    else:
        if hasattr(settings, 'CASSANDRA_KEYSPACE'):
            config['keyspace'] = settings.CASSANDRA_KEYSPACE
        else:
            click.secho('settings.CASSANDRA_KEYSPACE is missing!', fg='red')
            sys.exit()
    return config
