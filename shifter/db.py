# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import sys
import time
import hashlib
import copy

import click
from invoke import run
from cassandra.cluster import Cluster
from cassandra.util import max_uuid_from_time
from cassandra.auth import PlainTextAuthProvider

from .map import Column, Table, Keyspace


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


def get_snapshot():
    try:
        file = open('migrations/.snapshot', 'r')
        content = file.read()
        file.close()
    except Exception:
        return None
    return content


def update_snapshot(schema):
    try:
        file = open('migrations/.snapshot', 'w+')
        file.write(schema)
        file.close()
    except Exception:
        pass


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
    except Exception as e:
        click.secho("ERROR {}".format(e), fg='red', bold=True)
        sys.exit()
    click.secho("OK", fg='green', bold=True)


def delete_demo_keyspace():
    try:
        click.echo("Deleting tmp keyspace... ", nl=False)
        session.execute("DROP KEYSPACE IF EXISTS {}".format(DEMO_KEYSPACE))
        click.secho("OK", fg='green', bold=True)
    except Exception:
        click.secho("ERROR", fg='red', bold=True)


def record_migration(name, schema, config, up=True):
    session.set_keyspace(config['keyspace'])
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
    update_snapshot(schema)


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


def auto_migrate_keyspace(source_name, target_name):
    """
    Compare the 2 keyspaces and return a list of
    queries to be performed in order to sync source to target.
    """
    source = Keyspace(name=source_name, tables=[])
    stables = get_keyspace_tables(source_name)
    for table in stables:
        source.tables.append(Table(
            name=table,
            columns=get_table_columns(source_name, table)
        ))
    target = Keyspace(name=target_name, tables=[])
    ttables = get_keyspace_tables(target_name)
    table = []
    for table in ttables:
        target.tables.append(Table(
            name=table,
            columns=get_table_columns(target_name, table)
        ))
    return get_keyspace_diff(source, target)


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
        cols.append(Column(
            name=row.column_name,
            type=row.type,
            kind=row.kind,
            order=row.clustering_order,
            position=row.position))
    return cols


def get_columns_diff(source, target):
    if not isinstance(source, Column) or not isinstance(target, Column):
        raise ValueError("parameters must be Column instances")
    if source.kind != target.kind:
        return "kind"
    if source.position != target.position:
        return "position"
    if source.type != target.type:
        return "type"
    return None

def get_tables_diff(source, target):
    if not isinstance(source, Table) or not isinstance(target, Table):
        raise ValueError("parameters must be Table instances")
    if source == target:
        return None
    actions = []
    # Compare table name
    if source.name != target.name:
        raise ValueError("tables does not share the same name")
    # Columns in source table
    for col in source.columns:
        target_col = target.get_column(col.name)
        if not target_col:
            actions.append(source.drop_column_cql(col))
        else:
            if target_col != col:
                dif = get_columns_diff(col, target_col)
                if dif == "type":
                    actions.append(source.alter_column_type_cql(col.name, target_col.type))
    # Columns in destination table
    for col in target.columns:
        source_col = source.get_column(col.name)
        if not source_col:
            actions.append(source.add_column_cql(col))
    return actions


def get_keyspace_diff(source, target):
    if not isinstance(source, Keyspace) or not isinstance(target, Keyspace):
        raise ValueError("parameters must be Table instances")
    actions = []
    # Source tables
    for table in source.tables:
        target_table = target.get_table(table.name)
        if not target_table:
            actions.append(table.drop_cql())
        else:
            a = get_tables_diff(table, target_table)
            if a:
                actions += a
    # Target tables
    for table in target.tables:
        source_table = source.get_table(table.name)
        if not source_table:
            actions.append(table.dump_cql())
    return actions

