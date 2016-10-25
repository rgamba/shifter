# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import time

from db import connect, session, get_current_schema, create_demo_keyspace, delete_demo_keyspace
from config import get_config


# Get configuration
config = get_config()

# Connection to Cassandra
connect(config)


def get_last_migration():
    """ Get the last migration stored on cassandra. """
    migrations = session.execute("SELECT migration FROM cm_migrations ORDER BY id DESC LIMIT 1")
    if migrations is None:
        click.echo("cm_migrations table not found, creating... ", nl=False)
        session.execute(
            """
            CREATE TABLE cm_migrations(
                id timeuuid PRIMARY KEY,
                migration text,
                hash text
            )
            WITH CLUSTERING ORDER BY(id DESC)
            """
        )
        click.secho('OK', fg='green', bold=True)
        last_migration = None
    else:
        last_migration = migrations[0].migration
    return last_migration


def get_migrations_on_file():
    """ Get the stored migrations on file. """
    try:
        files = os.listdir('migrations')
    except Exception:
        click.secho('Unable to open the migrations directory!', fg='red')
        sys.exit()
    files.sort()
    return files


def get_pending_migrations(last_migration, migrations):
    """ Return a list of migration files that should be applied. """
    if last_migration not in migrations:
        click.secho('Unable to migrate because migrations DB is ahead of migrations on file.', fg='red')
        sys.exit()
    pending = []
    for m in migrations:
        try:
            f = m.split('_')[0]
            mint = int(f)
        except Exception:
            continue
        if mint <= int(last_migration):
            continue
        pending.append(m)
    return pending


def apply_migration(file, up, keyspace):
    click.echo("Applying migration {} {} ".format(file, ('UP' if up else 'DOWN')), nl=False)
    try:
        file = open('migrations/{}'.format(file), 'r')
        content = file.read()
        file.close()
    except Exception:
        click.secho('ERROR (unable to upen file)', fg='red', bold=True)
    parts = content.split('--DOWN--')
    if len(parts) > 1:
        qryup, qrydown = parts
    else:
        qryup = parts[0]
        qrydown = None

    session.set_keyspace(keyspace)
    qry = qryup if up else qrydown
    try:
        for q in qry.replace('\n', '').split(';'):
            session.execute(q)
    except Exception:
        click.secho('ERROR', fg='red', bold=True)
        return False
    click.secho('OK', fg='green', bold=True)


def create_migration_file(name, up, down=None, title='', description=''):
    if not os.path.isdir('migrations'):
        os.mkdir('migrations')
    migrations = get_migrations_on_file()
    i = 1
    while True:
        file_name = [str(len(migrations) + i).zfill(5)]
        file_name.append(name.strip().lower().replace(' ', '_'))
        file_name = '_'.join(file_name) + '.cql'
        if os.path.isfile('migrations/' + file_name):
            i++1
            continue
        file = open('migrations/' + file_name, 'w')
        file.write('/*\n')
        if title:
            file.write(title)
        else:
            file.write(name)
        file.write('\n\n')
        if description:
            file.write(description + '\n')
        file.write('Created: ' + time.strftime("%d-%m-%Y") + '\n')
        file.write('*/\n')
        file.write('--UP--\n')
        file.write(up + '\n\n')
        if down:
            file.write('--DOWN--\n')
            file.write(down)
        return file_name


def create_init_migration(config):
    click.echo("Creating initial migration... ", nl=False)
    current = get_current_schema(config)
    new_file = create_migration_file(name='initial', title='Initial migration', up=current)
    click.secho('OK', fg='green', bold=True)
    return new_file


create_init_migration(config)
#current = get_current_schema(config)
#create_demo_keyspace(str(current), config['keyspace'])
#delete_demo_keyspace()


