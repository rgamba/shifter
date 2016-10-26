# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import time
import warnings

from db import connect, get_current_schema
import db
from config import get_config

warnings.filterwarnings("ignore")

# Get configuration
config = get_config()


def get_last_migration(config):
    """ Get the last migration stored on cassandra. """
    db.session.set_keyspace(config['keyspace'])
    try:
        migrations = db.session.execute("SELECT migration FROM cm_migrations LIMIT 1")
        if not migrations:
            return None
        last_migration = migrations[0].migration
        if last_migration.endswith('.cql'):
            last_migration = last_migration[:-4]
    except Exception as e:
        click.echo("cm_migrations table not found, creating... ", nl=False)
        db.session.execute(
            """
            CREATE TABLE cm_migrations(
                id uuid,
                time timeuuid,
                migration text,
                hash text,
                PRIMARY KEY (id, time)
            )
            WITH CLUSTERING ORDER BY(time DESC)
            """
        )
        click.secho('OK', fg='green', bold=True)
        last_migration = None
    return last_migration


def get_migrations_on_file():
    """ Get the stored migrations on file. """
    try:
        files = []
        for f in os.listdir('migrations'):
            if f[-3:] == 'cql':
                files.append(f)
    except Exception:
        click.secho('Unable to open the migrations directory!', fg='red')
        sys.exit()
    files.sort()
    return files


def get_head_migration_on_file(migrations):
    """ Get the highest migration on file. """
    mig = []
    for m in migrations:
        try:
            m = m.split('_')[0]
            m = int(m)
        except Exception:
            continue
        mig.append(m)
    if not mig:
        return 0
    return mig.pop()


def get_pending_migrations(last_migration, migrations, head=None):
    """
    Return a list of migration files that should be applied. 

    If head is not None, it must be a positive integer pointing at the tarjet migration we
    are headed. If the current DB migration is ahead of the head, then the delta migrations
    will be applied DOWN. Otherwise the delta migrations will be applied UP.

    In case DOWN migrations are not found in the file, the migration wont be able to continue.
    Pending migrations will be returned IN ORDER in which they must be executed.
    """
    if last_migration is not None and (last_migration + '.cql') not in migrations:
        click.secho('Unable to migrate because migrations DB is ahead of migrations on file.', fg='red')
        sys.exit()
    if last_migration is None:
        last_migration = 0
    else:
        last_migration = last_migration.split('_')[0]

    pointer = int(last_migration)
    files_head = get_head_migration_on_file(migrations)
    # If no head is specified, then the target head will be the
    # largest migration on file.
    target = files_head if head is None else int(head)
    if target > files_head:
        click.secho('The target migration provided does not exist in the migrations dir.', fg='red')
        sys.exit()
    # Are we migrating up or down?
    up = True if pointer <= target else False

    if not up:
        migrations.sort(reverse=True)

    pending = []
    for m in migrations:
        try:
            f = m.split('_')[0]
            cur_mig = int(f)
        except Exception:
            continue

        if up:
            if cur_mig <= pointer or cur_mig > target:
                continue
        else:
            if cur_mig <= target or cur_mig > pointer:
                continue

        pending.append(m)
    return (pending, up)


def apply_migration(file, up, keyspace):
    click.echo("Applying migration {} {} ".format(file, ('UP' if up else 'DOWN')), nl=False)
    try:
        file = open('migrations/{}'.format(file), 'r')
        content = file.read()
        file.close()
    except Exception:
        click.secho('ERROR (unable to upen file)', fg='red', bold=True)
    content = content.replace('--UP--', '', 1)
    parts = content.split('--DOWN--')
    if len(parts) > 1:
        qryup, qrydown = parts
    else:
        qryup = parts[0]
        qrydown = None

    db.session.set_keyspace(keyspace)
    qry = qryup if up else qrydown
    try:
        for q in qry.replace('\n', '').split(';'):
            q = q.strip()
            if q != '':
                db.session.execute(q.strip())
    except Exception as e:
        click.secho('ERROR', fg='red', bold=True)
        return (False, e)
    click.secho('OK', fg='green', bold=True)
    return (True, None)


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
            i += 1
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
    db.record_migration(name=new_file.replace('.cql', ''), schema=current)
    return new_file


# Cmd commands

@click.group()
def cli():
    pass


@cli.command('create', short_help='Create a new migration file')
@click.argument('name', required=True)
@click.option('--title', help='Migration title', default=None)
@click.option('--description', help='Migration description', default=None)
def create(name, title, description):
    connect(config)
    file = create_migration_file(name=name, up='/* YOUR CQL GOES HERE */', title=title, description=description)
    click.echo('Create migration file ', nl=False)
    click.secho(file, bold=True, fg='green')


@cli.command('migrate', short_help='Migrate the current database')
@click.argument('head', required=False)
@click.option('--simulate', is_flag=True, help='Just print the migrations that will be performed')
def migrate(head, simulate):
    global config
    try:
        head = int(head) if head else None
    except Exception:
        click.secho('Head argument must be an integer.', fg='red')
    connect(config)
    schema = get_current_schema(config)
    last = get_last_migration(config)
    migrations = get_migrations_on_file()
    if len(migrations) <= 0:
        create_init_migration(config)
    pending, up = get_pending_migrations(last, migrations, head)
    if len(pending) <= 0:
        click.echo("Already up to date.")
        return

    if simulate:
        for p in pending:
            click.echo('{} will be applied {}'.format(p, 'UP' if up else 'DOWN'))
        return

    # First in demo
    error = False
    db.create_demo_keyspace(schema, config['keyspace'])
    for f in pending:
        res, err = apply_migration(file=f, up=up, keyspace=db.DEMO_KEYSPACE)
        if not res:
            error = True
            click.secho('---\nUnable to continue due to an error in {}:\n\n{}\n---\n'.format(f, err.message), fg='red')
            break
    db.delete_demo_keyspace()
    if error:
        return
    # Now in real keyspace
    for f in pending:
        res, err = apply_migration(file=f, up=True, keyspace=config['keyspace'])
        if not res:
            error = True
            click.secho('---\nUnable to continue due to an error in {}:\n\n{}\n---\n'.format(f, err.message), fg='red')
            break
        db.record_migration(name=f, schema=get_current_schema(config))
    if error:
        return
    click.echo("Migration completed successfully.")


if __name__ == "__main__":
    cli()

