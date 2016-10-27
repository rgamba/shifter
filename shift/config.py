# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import importlib


def get_config(env_override=None):
    env = os.environ
    if env_override is not None:
        for key, value in env_override.iteritems():
            env[key] = value

    settings = None
    try:
        if 'CASSANDRA_SETTINGS' in os.environ:
            settings = importlib.import_module(env['CASSANDRA_SETTINGS'])
    except Exception:
        click.secho('Unable to load settings module!', fg='red')
        sys.exit()

    # Check configuration
    config = {}
    if 'CASSANDRA_SEEDS' in env:
        config['seeds'] = env['CASSANDRA_SEEDS']
    else:
        if hasattr(settings, 'CASSANDRA_SEEDS'):
            config['seeds'] = settings.CASSANDRA_SEEDS
        else:
            click.secho('CASSANDRA_SEEDS is missing is settings file!', fg='red')
            sys.exit()
    if 'CASSANDRA_KEYSPACE' in env:
        config['keyspace'] = env['CASSANDRA_KEYSPACE']
    else:
        if hasattr(settings, 'CASSANDRA_KEYSPACE'):
            config['keyspace'] = settings.CASSANDRA_KEYSPACE
        else:
            click.secho('settings.CASSANDRA_KEYSPACE is missing!', fg='red')
            sys.exit()
    return config
