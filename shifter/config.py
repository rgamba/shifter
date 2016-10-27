# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import importlib

REQUIRED = [
    'CASSANDRA_SEEDS',
    'CASSANDRA_KEYSPACE'
]
OPTIONAL = [
    'CASSANDRA_PORT',
    'CASSANDRA_USER',
    'CASSANDRA_PASSWORD',
    'CASSANDRA_CQLVERSION'
]


def get_config(env_override=None):
    """ Get the configuration dict. """
    env = os.environ
    if env_override is not None:
        for key, value in env_override.iteritems():
            env[key] = value

    settings = None
    try:
        if 'CASSANDRA_SETTINGS' in os.environ:
            settings = importlib.import_module(env['CASSANDRA_SETTINGS'])
    except Exception:
        click.secho('Unable to load settings module {}!'.format(env.get('CASSANDRA_SETTINGS')), fg='red')
        sys.exit()

    # Check configuration
    config = {}

    for c in REQUIRED:
        if hasattr(settings, c):
            config[c.lower().split('_', 1).pop()] = getattr(settings, c)
        else:
            click.secho('{} is missing is settings file!'.format(c), fg='red')
            sys.exit()
    for c in OPTIONAL:
        if hasattr(settings, c):
            config[c.lower().split('_', 1).pop()] = getattr(settings, c)

    return config
