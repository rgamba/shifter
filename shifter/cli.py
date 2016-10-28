# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import click
import warnings

from .migrate import create_migration_file, create_init_migration, get_migrations_on_file
from .migrate import get_last_migration, get_pending_migrations, apply_migration
from .db import connect, get_current_schema, create_demo_keyspace, keyspace_exists
from .db import record_migration, delete_demo_keyspace, create_migration_table, DEMO_KEYSPACE
from .db import auto_migrate_keyspace, get_snapshot, update_snapshot
from .config import get_config

warnings.filterwarnings("ignore")

# Get configuration
config = get_config()

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
    click.echo('Created migration file ', nl=False)
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
@click.option('--print', is_flag=True, help='Just print the migrations')
@click.option('--name', required=True, help='Name of the update')
def auto_update(print, name):
    global config
    # Cassandra connection.
    connect(config)
    # Check migrations on file.
    migrations = get_migrations_on_file()
    if not keyspace_exists(config.get('keyspace')):
        click.echo('Shift hasn\'t been initialized on this keyspace.\nRun \'shift migrate\' to initiate or user the --help flag.')
        return
    last = get_last_migration(config)
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
    actions_up = auto_migrate_keyspace(DEMO_KEYSPACE, config.get('keyspace'))
    if len(actions_up) <= 0:
        click.secho('Cassandra is up to date with migrations on file.')
        delete_demo_keyspace()
        return
    actions_down = auto_migrate_keyspace(config.get('keyspace'), DEMO_KEYSPACE)
    delete_demo_keyspace()
    upquery = ';\n'.join(actions_up) + ";"
    downquery = ';\n'.join(actions_down)  + ";"
    if print:
        click.echo('---\n' + upquery + '\n---\n')
        return
    file = create_migration_file(name, upquery, downquery)
    click.echo('Created migration file ', nl=False)
    record_migration(file, get_current_schema(config), config)
    click.secho(file, bold=True, fg='green')


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
        record_migration(name=f, schema=get_current_schema(config), up=up, config=config)
    if error:
        return
    click.echo("Migration completed successfully.")


if __name__ == "__main__":
    cli()
