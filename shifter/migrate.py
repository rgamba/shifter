# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import click
import time
import warnings

from .db import connect, get_current_schema, get_session, create_demo_keyspace, keyspace_exists
from .db import record_migration, delete_demo_keyspace, create_migration_table, DEMO_KEYSPACE
from .db import auto_migrate_keyspace, get_snapshot, update_snapshot
from .config import get_config

warnings.filterwarnings("ignore")

# Get configuration
config = get_config()


def get_last_migration(config, get_schema=False):
    """
    Get the last migration stored on cassandra.
    If there is no first migration, it will return 0
    If there is no table shift_migrations, it will return None
    """
    get_session().set_keyspace(config['keyspace'])
    try:
        migrations = get_session().execute("SELECT migration, snapshot FROM shift_migrations LIMIT 1")
        if not migrations:
            return 0
        last_migration = migrations[0].migration if not get_schema else migrations[0].snapshot
        if last_migration.endswith('.cql'):
            last_migration = last_migration[:-4]
    except Exception as e:
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
    if last_migration and '{}.cql'.format(last_migration) not in migrations:
        click.secho('Unable to migrate because migrations DB is ahead of migrations on file.', fg='red')
        sys.exit()
    if not last_migration:
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
            if f == '00000':
                continue
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
    """
    Apply the migration given the raw file name.
    If up is True, then it will execute the up statement, else it will execute the down statement.
    Valid CQL format in files is as follows:

    --UP--
    /* Your CQL statements here, separated by ; */
    --DOWN--
    /* Your CQL statements here. They MUST revert what the UP statements do */

    """
    fname = file
    click.echo("Applying migration {} {} ".format(file, ('UP' if up else 'DOWN')), nl=False)
    try:
        file = open('migrations/{}'.format(file), 'r')
        content = file.read()
        file.close()
    except Exception:
        click.secho('ERROR', fg='red', bold=True)
        return (False, 'Unable to open file {}.'.format(file))
    content = content.replace('--UP--', '', 1)
    parts = content.split('--DOWN--')
    if len(parts) > 1:
        qryup, qrydown = parts
    else:
        qryup = parts[0]
        qrydown = None

    if not qrydown:
        click.secho('ERROR', fg='red', bold=True)
        return (False, 'File {} does not include a --DOWN-- statement.'.format(fname))

    if keyspace is not None:
        get_session().set_keyspace(keyspace)
    qry = qryup if up else qrydown
    try:
        for q in qry.replace('\n', '').split(';'):
            q = q.strip()
            if q != '':
                get_session().execute(q.strip())
    except Exception as e:
        click.secho('ERROR', fg='red', bold=True)
        return (False, e)
    click.secho('OK', fg='green', bold=True)
    return (True, None)


def create_migration_file(name, up, down=None, title='', description='',
                          genesis=False):
    """
    Create a migration file in the migrations folder
    and return its filename.
    """
    if not os.path.isdir('migrations'):
        os.mkdir('migrations')
    migrations = get_migrations_on_file()
    i = 1
    while True:
        count = len(migrations) if not genesis else -1
        file_name = [str(count + i).zfill(5)]
        if not genesis:
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
    """
    Create the genesis configuration file and return it's filename.
    This function will also write the current keyspace schema (the one
    defined in the current settings) as the content of the file.
    """
    click.echo("Creating migration genesis... ", nl=False)
    if os.path.isfile('migrations/00000.cql'):
        click.secho('ERROR (already exists)', fg='red', bold=True)
        return False
    current = get_current_schema(config)
    down = 'DROP KEYSPACE {};'.format(config.get('keyspace'))
    new_file = create_migration_file(name='', title='MIGRATION GENESIS', up=current, down=down, genesis=True)
    click.secho('OK', fg='green', bold=True)
    update_snapshot(current)
    return new_file


# Cmd commands

@click.group()
def cli():
    pass


@cli.command('create', short_help='Create a new migration file.')
@click.argument('name', required=True)
@click.option('--title', help='Migration title', default=None)
@click.option('--description', help='Migration description', default=None)
def create(name, title, description):
    """ Create a new migration file. """
    file = create_migration_file(name=name, up='/* YOUR CQL GOES HERE */', 
                                 title=title, description=description)
    click.echo('Create migration file ', nl=False)
    click.secho(file, bold=True, fg='green')


@cli.command('init', short_help='Create the migration genesis based on the current keyspace.')
def init():
    """ Initiate the migration project in the current directory. """
    global config
    # Cassandra connection.
    connect(config)
    create_init_migration(config)


@cli.command('status', short_help='Show the current migration status.')
@click.option('--settings', default=None, help='Settings module (not file). Must contain CASSANDRA_SEEDS and CASSANDRA_KEYSPACE defined')
def status(settings):
    """ Get the current migration status. """
    global config
    if settings is not None:
        config = get_config({'CASSANDRA_SETTINGS': settings})
    # Cassandra connection.
    connect(config)
    # Check migrations on file.
    migrations = get_migrations_on_file()
    if not migrations:
        click.secho('No migrations found on the current directory.')
        return
    if '00000.cql' not in migrations:
        click.secho('There is no genesis migration found.')
        return
    if not keyspace_exists(config.get('keyspace')):
        click.echo('Shift hasn\'t been initialized on this keyspace.\nRun \'shift migrate\' to initiate or user the --help flag.')
        return
    last = get_last_migration(config)
    if last is None:
        click.secho('Shift hasn\'t been initialized in this keyspace.')
        return
    pending, up = get_pending_migrations(last, migrations)
    if len(pending) <= 0:
        click.echo("Already up to date.\nCurrent head is {}".format(last))
        return
    click.echo("Cassandra is {} movements behind the current file head ({}).\nCurrent Cassandra head is {}".format(len(pending), migrations[-1], last))


@cli.command('auto-update', short_help='Auto generate the next migration targeting the current Cassandra structure.')
def auto_update():
    global config
    # Cassandra connection.
    connect(config)
    # Check migrations on file.
    migrations = get_migrations_on_file()
    if not keyspace_exists(config.get('keyspace')):
        click.echo('Shift hasn\'t been initialized on this keyspace.\nRun \'shift migrate\' to initiate or user the --help flag.')
        return
    last = get_last_migration(config, get_schema=True)
    if last is None:
        click.secho('Shift hasn\'t been initialized in this keyspace.')
        return
    pending, up = get_pending_migrations(last, migrations)
    if len(pending) > 0:
        click.secho('There are pending migrations to be done, please migrate before auto-updating.')
        return
    # Create demo keyspace to compare
    snap = get_snapshot()
    if not snap:
        click.secho('Unable to locate the last snapshot.', fg='red')
        return
    create_demo_keyspace(snap, config.get('keyspace'))
    actions = auto_migrate_keyspace(DEMO_KEYSPACE, config.get('keyspace'))
    click.echo(';\n'.join(actions))
    delete_demo_keyspace()


@cli.command('migrate', short_help='Migrate the current database.')
@click.argument('head', required=False)
@click.option('--simulate', is_flag=True, help='Just print the migrations that will be performed')
@click.option('--just-demo', is_flag=True, help='Just perform the migrations in demo DB')
@click.option('--settings', default=None, help='Settings module (not file). Must contain CASSANDRA_SEEDS and CASSANDRA_KEYSPACE defined')
def migrate(head, simulate, just_demo, settings):
    """ Migrate now. """
    global config
    if settings is not None:
        config = get_config({'CASSANDRA_SETTINGS': settings})
    # Input validation.
    try:
        head = int(head) if head else None
    except Exception:
        click.secho('Head argument must be an integer.', fg='red')
        return
    # Cassandra connection.
    connect(config)
    # Check migrations on file.
    migrations = get_migrations_on_file()
    # Check if there is the migration genesis is present
    # if it's not present we cannot continue.
    if '00000.cql' not in migrations:
        click.secho('Migration genesis (00000.cql) is missing! Forgot to run init command first?', fg='red')
        return
    # Check if the keyspace exists and if we have a migrations
    # table configured.
    if not keyspace_exists(config.get('keyspace')):
        # Keyspace does not exist, we need to create it based on the genesis file.
        click.echo('Keyspace not found, creating from the genesis file.')
        result, err = apply_migration('00000.cql', True, None)
        if not result:
            click.secho('---\nUnable to continue due to an error genesis migration:\n\n{}\n---\n'.format(err.message), fg='red')
            return
        # Override head, it needs to go all the way from the bottom...
        head = None

    schema = get_current_schema(config)
    last = get_last_migration(config)
    if last is None:
        result, err = create_migration_table(config.get('keyspace'))
        if not result:
            click.secho('---\nUnable to continue due to an error:\n\n{}\n---\n'.format(err.message), fg='red')
            return
        update_snapshot(get_current_schema(config))

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
    create_demo_keyspace(schema, config['keyspace'])
    for f in pending:
        res, err = apply_migration(file=f, up=up, keyspace=DEMO_KEYSPACE)
        if not res:
            error = True
            click.secho('---\nUnable to continue due to an error in {}:\n\n{}\n---\n'.format(f, err), fg='red')
            break
    delete_demo_keyspace()
    if error:
        return
    if just_demo:
        return
    # Now in real keyspace
    for f in pending:
        res, err = apply_migration(file=f, up=up, keyspace=config['keyspace'])
        if not res:
            error = True
            click.secho('---\nUnable to continue due to an error in {}:\n\n{}\n---\n'.format(f, err.message), fg='red')
            break
        record_migration(name=f, schema=get_current_schema(config), up=up)
    if error:
        return
    click.echo("Migration completed successfully.")


if __name__ == "__main__":
    cli()

