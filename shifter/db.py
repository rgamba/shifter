# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import sys
import time
import hashlib

import click
from invoke import run
from cassandra.cluster import Cluster
from cassandra.util import max_uuid_from_time
from cassandra.auth import PlainTextAuthProvider


DEMO_KEYSPACE = 'cm_tmp'
SHIFT_TABLE = 'shift_migrations'

session = None


def connect(config):
    global session
    # Connect to cassandra
    auth_provider = None
    if config.get('user'):
        auth_provider = PlainTextAuthProvider(username=config.get('user'), password=config.get('password'))
    cluster = Cluster(
        contact_points=config.get('seeds'),
        port=(int(config.get('port')) if config.get('port') else 9042),
        auth_provider=auth_provider
    )
    try:
        session = cluster.connect()
    except:
        click.secho("Unable to connect to Cassandra", fg='red')
        sys.exit()
    return session


def get_session():
    global session
    if session is None:
        connect()
    return session


def run_cqlsh(config, command, keyspace=None):
    q = ['cqlsh', '-e', '"{}"'.format(command)]
    if config.get('user'):
        q.append('-u')
        q.append(config.get('user'))
    if config.get('password'):
        q.append('-p')
        q.append(config.get('password'))
    if keyspace:
        q.append('-k')
        q.append(keyspace)
    if config.get('cqlversion'):
        q.append('--cqlversion={}'.format(config.get('cqlversion')))
    q.append(config['seeds'][0])
    if config.get('port'):
        q.append(config['port'])
    return ' '.join(q)


def get_current_schema(config):
    try:
        cqlsh = run_cqlsh(config, command="DESCRIBE " + config['keyspace'])
        out = run(cqlsh, hide='stdout')
    except Exception as e:
        click.secho("Unable to get the current DB schema: {}".format(e), fg='red')
        sys.exit()
    return out.stdout


def create_demo_keyspace(schema, schema_name):
    schema = schema.replace("CREATE KEYSPACE {}".format(schema_name), "CREATE KEYSPACE {}".format(DEMO_KEYSPACE), 1)
    schema = schema.replace("{}.".format(schema_name), "{}.".format(DEMO_KEYSPACE))
    try:
        click.echo("Creating tmp keyspace... ", nl=False)
        session.execute("DROP KEYSPACE IF EXISTS {}".format(DEMO_KEYSPACE))
        for q in schema.replace('\n', '').split(';'):
            if q.strip() == "":
                continue
            session.execute(q)
    except Exception:
        click.secho("ERROR", fg='red', bold=True)
        sys.exit()
    click.secho("OK", fg='green', bold=True)


def delete_demo_keyspace():
    try:
        click.echo("Deleting tmp keyspace... ", nl=False)
        session.execute("DROP KEYSPACE IF EXISTS {}".format(DEMO_KEYSPACE))
        click.secho("OK", fg='green', bold=True)
    except Exception:
        click.secho("ERROR", fg='red', bold=True)


def record_migration(name, schema, up=True):
    if name.endswith('.cql'):
        name = name[:-4]
    if not up:
        # Delete
        rows = session.execute(
            """
            SELECT time FROM shift_migrations 
            WHERE type = 'MIGRATION' 
                AND migration = %s 
            ALLOW FILTERING
            """, (name,)
        )
        if not rows:
            click.secho("Unable to select last migration from DB", fg="red")
            return False
        id = rows[0].time
        delete = session.execute("DELETE FROM shift_migrations WHERE type = 'MIGRATION' AND time = %s", (id,))
        return

    m = hashlib.md5()
    m.update(schema)
    session.execute(
        """
        INSERT INTO shift_migrations(type, time, migration, hash)
        VALUES (%s, %s, %s, %s)
        """,
        ('MIGRATION', max_uuid_from_time(time.time()), name, m.hexdigest())
    )


def create_migration_table(keyspace):
    session.set_keyspace(keyspace)
    click.echo("Creating shift_migrations table... ", nl=False)
    try:
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS shift_migrations(
                type text,
                time timeuuid,
                migration text,
                hash text,
                PRIMARY KEY (type, time)
            )
            WITH CLUSTERING ORDER BY(time DESC)
            """
        )
        click.secho('OK', fg='green', bold=True)
        return (True, None)
    except Exception as e:
        click.secho('ERROR', fg='red', bold=True)
        return (False, e)


def keyspace_exists(name):
    session.set_keyspace('system_schema')
    ks = session.execute('SELECT keyspace_name FROM keyspaces')
    if not ks:
        return False
    for row in ks:
        if row.keyspace_name == name:
            return True
    return False


def get_keyspace_tables(keyspace):
    session.set_keyspace('system_schema')
    ks = session.execute('SELECT table_name FROM tables WHERE keyspace_name = %s', [keyspace])
    if not ks:
        return []
    tables = []
    for row in ks:
        tables.append(row.table_name)
    return tables


def get_table_columns(keyspace, table):
    """ Return a list of tuples (col_name, col_type). """
    session.set_keyspace('system_schema')
    ks = session.execute('SELECT * FROM columns WHERE keyspace_name = %s AND table_name = %s', [keyspace, table])
    if not ks:
        return []
    cols = []
    for row in ks:
        cols.append({
            'name': row.column_name,
            'type': row.type,
            'kind': row.kind,
            'order': row.clustering_order,
            'position': row.position
        })
    return cols


class Column(object):
    def __init__(self, name, type, kind='regular', order=None, position=-1):
        self.name = name
        self.type = type
        self.kind = kind
        self.order = order
        self.position = position

    def is_pk(self):
        return self.kind == 'partition_key'

    def is_clustering(self):
        return self.kind == 'clustering'

    def dump_cql(self):
        return '{} {}'.format(self.type, self.name)


class Table(object):
    def __init__(self, name, columns=[]):
        self.name = name
        self.columns = columns

    def primary_keys(self):
        pk = {}
        for c in self.columns:
            if c.is_pk():
                pk[c.position] = c.name
        r = []
        for i in sorted(pk):
            r.append(pk[i])
        return r

    def clustering_columns(self):
        cl = {}
        for c in self.columns:
            if c.is_clustering():
                cl[c.position] = c.name
        r = []
        for i in sorted(cl):
            r.append(cl[i])
        return r

    def dump_cql(self):
        cql = ['CREATE TABLE {} ('.format(self.name)]
        cql.append('\t' + ',\n'.join([x.dump_cql() for x in self.columns]))
        pk = self.primary_keys()
        pks = ('({})' if len(pk) > 1 else '{}').format(', '.join(pk))
        cc = self.clustering_columns()
        clustering = ',' + ', '.join(cc) if cc else ''
        cql.append('\tPRIMARY KEY ({}{})'.format(pks, clustering))
        cql.append(')')
        return '\n'.join(cql)
