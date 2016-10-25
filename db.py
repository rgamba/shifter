# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import click
import sys
from cassandra.cluster import Cluster
from invoke import run

DEMO_KEYSPACE = 'cm_tmp'

session = None


def connect(config):
    global session
    # Connect to cassandra
    cluster = Cluster(config['seeds'])
    try:
        session = cluster.connect()
        click.echo("Connected to Cassandra, keyspace {}".format(config['keyspace']))
    except:
        click.secho("Unable to connect to Cassandra", fg='red')
        sys.exit()
    return session


def get_current_schema(config):
    try:
        out = run('cqlsh -e "DESCRIBE {c[keyspace]}" {c[seeds][0]}'.format(c=config), hide='stdout')
    except Exception as e:
        click.secho("Unable to get the current DB schema: {}".format(e), fg='red')
        sys.exit()
    return out.stdout


def create_demo_keyspace(schema, schema_name):
    schema = schema.replace("CREATE KEYSPACE {}".format(schema), "CREATE KEYSPACE {}".format(DEMO_KEYSPACE), 1)
    try:
        click.echo("Creating tmp keyspace... ", nl=False)
        session.execute("DROP KEYSPACE {} IF EXISTS".format(DEMO_KEYSPACE))
        session.execute(schema)
    except Exception as e:
        click.secho("ERROR", fg='red', bold=True)
        sys.exit()
