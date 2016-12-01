import logging
import os
import re
import subprocess

LOGGER = logging.getLogger(__package__)


def _make_cmd(*args):
    """Normalize args list that may contain empty values or tuples.

    Flatten tuples and lists in the args list and remove any values that are
    None or the empty string.

    :param args:
    :return: processed list of command arguments
    """
    cmd = []
    for a in args:
        if type(a) == tuple or type(a) == list:
            for sub_a in a:
                cmd.append(sub_a)
        else:
            cmd.append(a)
    return [a for a in cmd if a != '' and a is not None]


def _optional(*args):
    """Return *args if no element is the empty string or None.

    Otherwise, return an empty string.

    :param args:
    :return: *args or ''
    """
    for a in args:
        if a == '' or a is None:
            return ''
    return args


def run_command(cmd):
    """Wrapper around subprocess.checkout_output to report errors properly.

    Raises an exception if the command fails.

    :return: output of cmd as a str
    """
    LOGGER.info('Will run: {}'.format(' '.join(cmd)))
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        raise
        # msg = ('Subprocess failed, cmd was `{cmd}`, '
        #        'result was `{code}`, output was `{output}`')
        # sys.exit(msg.format(output=e.output, code=e.returncode,
        #                     cmd=' '.join(e.cmd)))
    return result.decode('utf-8')


def psql(stmt, database=None, conn_info={}):
    """Execute a statement against a PostgreSQL database; return output.

    The `psql` command can be overridden via the `PSQL_CMD` environment
    variable (but take a look at the switches that are used, below). N.B.: the
    overriding command is split on whitespace before use.

    If the statement terminates with an error, an exception is raised.

    :param stmt: SQL statement
    :param database: database to connect to (else same as user)
    :param conn_info: dict of 'hostname', 'port', and 'username'
    :return: raw stdout from psql as a unicode str
    """
    if '"' in stmt:
        raise ValueError('Double-quote not allowed ')

    cmd = _make_cmd(os.environ.get('PSQL_CMD', 'psql').split(),
                    '-t',  # tuples only
                    '-v', 'ON_ERROR_STOP=1',
                    _optional('-U', conn_info.get('username', None)),
                    _optional('-h', conn_info.get('hostname', None)),
                    _optional('-p', conn_info.get('port', None)),
                    _optional('-d', database or conn_info.get('username',
                                                              None)),
                    '-c', '{}'.format(stmt))
    return run_command(cmd)


def pg_dump(database, filename, schemas=None, conn_info={}, empty=False):
    """Execute pg_dump command.

    If the `schemas` collection is present and non-empty, then for all
    non-None schemas in the collection, the database dump is
    restricted to those schemas; otherwise, there is no restriction by
    schema.

    If the `empty` parameter is True, the dump will be almost entirely
    empty, because all schemas will be excluded. I.e. the backup will be
    usable only for reconstituting a database with nothing in it, the idea
    being that schema-containing backups can be subsequently loaded into it.

    The `pg_dump` command can be overridden via the `PG_DUMP_CMD` environment
    variable (but take a look at the switches that are used, below). N.B.: the
    overriding command is split on whitespace before use.

    :param database:
    :param filename:
    :param schemas:
    :param empty: just backup the database proper, no schemas
    :param conn_info: dict of 'username', 'hostname', and 'port'
    :return: None
    """
    cmd = _make_cmd(os.environ.get('PG_DUMP_CMD', 'pg_dump').split(),
                    '--format=c',  # compressed format
                    _optional('-U', conn_info.get('username', None)),
                    _optional('-h', conn_info.get('hostname', None)),
                    _optional('-p', conn_info.get('port', None)),
                    '-d', database,
                    '-f', filename)

    if empty:
        if schemas:
            raise ValueError('non-None schemas incompatible with `empty`')
        for schema in get_schemas(database, conn_info):
            cmd.append('-N')
            cmd.append(schema)

    if schemas:
        for schema in schemas:
            cmd.append('-n')
            cmd.append(schema)

    run_command(cmd)


def pg_dumpall_globals(filename, conn_info):
    """Execute pg_dumpall command to dump globals.

    The `pg_dumpall` command can be overridden via the `PG_DUMPALL_CMD`
    environment variable (but take a look at the switches that are used, below).
    N.B.: the overriding command is split on whitespace before use.

    :param filename:
    :param conn_info: dict of 'hostname', 'port', and 'username'
    :return: None
    """
    cmd = _make_cmd(os.environ.get('PG_DUMPALL_CMD', 'pg_dumpall').split(),
                    _optional('-U', conn_info.get('username', None)),
                    _optional('-h', conn_info.get('hostname', None)),
                    _optional('-p', conn_info.get('port', None)),
                    '--globals-only',
                    '--database=template1',
                    '-f', filename)
    run_command(cmd)


def pg_restore(filename, database=None, conn_info={}):
    """Execute pg_restore command.

    This is really only to facilitate tests.

    The `pg_restore` command can be overridden via the `PG_RESTORE_CMD`
    environment variable (but take a look at the switches that are used, below).
    N.B.: the overriding command is split on whitespace before use.

    If `database` is None, `pg_restore` will connect to `template1` and
    create whatever database is specified in the dump file. Otherwise,
    `pg_restore` will connect to the specified database and restore into it.

    To clarify the previous paragraph, `database` should NOT be None if the
    backup file was created with one or more `-n/--namespace` switches to
    specify schemas. Backups created in this way do not reference a particular
    database, and the restore needs to be performed into a specified database.

    :param filename: input dump file path
    :param database: database name to create and restore into (or None)
    :param conn_info: dict of 'hostname', 'port', and 'username'
    :return:
    """
    if not database:
        database = 'template1'
    cmd = _make_cmd(os.environ.get('PG_RESTORE_CMD', 'pg_restore').split(),
                    _optional('-U', conn_info.get('username', None)),
                    _optional('-h', conn_info.get('hostname', None)),
                    _optional('-p', conn_info.get('port', None)),
                    '--create',
                    '-d', database,
                    filename)

    results = run_command(cmd)
    return results


def get_schemas(database, conn_info):
    """List all regular schemas in the specified database.

    Return schemas normally returned by `\dn` in psql, i.e. all schemas
    except 'pg_temp_1', 'pg_toast', 'pg_toast_temp_1', 'pg_catalog', and
    'information_schema'.

    :param database: name of database
    :param conn_info: dict of 'hostname', 'port', and 'username'
    :return: list of schemas
    """
    cmd = ("select schema_name from information_schema.schemata"
           " where schema_name !~ '^(pg_temp.*)|(pg_toast*)|(pg_catalog)"
           "|(information_schema)$'")
    psql_output = psql(cmd, database=database, conn_info=conn_info)
    return [x.strip() for x in psql_output.split('\n') if x.strip()]


def get_databases(conn_info):
    """Return all regular databases in the PostgreSQL database.

    The `template0`, `template1`, and `postgres` are excluded.

    Arguably, `template1` should be included, because theoretically
    the user could customize it. However, we don't do that (yet).

    Also, although it is conventional that the founding superuser
    is named `postgres` and owns a similarly named database, this
    is not carved in stone.

    :param conn_info: dict of 'hostname', 'port', and 'username'
    """
    sql = ("select datname from pg_database "
           "where datname not in ('template0', 'template1', 'postgres')")
    psql_output = psql(sql, conn_info=conn_info)
    return [x.strip() for x in psql_output.split('\n') if x]


def filter_collection(collection, include=None, exclude=None):
    """Filter collection based on include and exclude regexps.

    The regexp patterns are not implicitly anchored at the beginning of
    strings (i.e. `search` is used, rather than `match`). E.g.
    'abc' will match '_abc_' as well as 'abcdef'.

    :return: set of matching collection elements
    """
    matches = set()
    if include:
        include_pat = re.compile(include)
    else:
        include_pat = None
    if exclude:
        exclude_pat = re.compile(exclude)
    else:
        exclude_pat = None

    for element in collection:
        if include_pat and not re.search(include_pat, element):
            continue  # Do not include this database; failed to match include
        if exclude_pat and re.search(exclude_pat, element):
            continue  # Do not include this database; matched exclude
        matches.add(element)

    return matches


def matching_databases(conn_info, include=None, exclude=None):
    """Return databases matching `include` and not matching `exclude`."""
    return filter_collection(get_databases(conn_info), include, exclude)


class SchemaCriteria(object):
    """Encapsulate schema selection criteria"""
    def __init__(self, database, conn_info, include=None, exclude=None):
        self.database = database
        self.conn_info = conn_info
        self.include = include
        self.exclude = exclude


def matching_schemas(schema_criteria):
    """Return schemas in database matching `include`/not matching `exclude`

    :param SchemaCriteria schema_criteria: database, conn_info, and inclusion
    and exclusion patterns
    :return: set of matching schema names in the database
    """
    return filter_collection(get_schemas(schema_criteria.database,
                                         schema_criteria.conn_info),
                             schema_criteria.include,
                             schema_criteria.exclude)
