#!/usr/bin/env python
"""pgbackup

Back up a PostgreSQL database. Similar functionality to the
pg_backup_rotated.py script on the PostgreSQL Wiki, but written in Python, with
additional functionality:

* Database regular expression to specify candidate databases to back up.
Example: `pedsnet_dcc_v.*`

* Database wildcard exclusion regular expression to specify databases to
exclude.

* Schema (namespace) regular expression to specify candidate schemas to back
up.

* Schema exclusion regular expression to specify schemas to exclude.

* Schemas can be written to separate files.

Usage:
  pgbackup (-h | --help)
  pgbackup --version
  pgbackup -c <cfgfile> [-d <database>...]

Options:
  -h --help     Show this screen.
  --version     Show version.
  -c <cfgfile>  Config file.
  -d <database> Database to backup (default: all)
"""

try:
    # Python 3
    import configparser
except ImportError:
    import ConfigParser as configparser

import glob
import logging
import os
import re

from docopt import docopt

from pgbackup.backup import Backup, get_today
from pgbackup.pg_utils import matching_databases, matching_schemas, SchemaCriteria

LOGGER = logging.getLogger(__package__)


class MyConfigParser(configparser.ConfigParser):
    """Subclass of ConfigParser with safe_get"""
    def safe_get(self, section, option, default):
        try:
            v = self.get(section, option)
        except Exception:
            v = default
        return v


def read_config(filename, extra_defaults=None):
    """Read the config file and return config object.

    Settings can be read from the returned config object like this:

    config.get('backup', 'username')

    :param filename: config file path
    :param extra_defaults: optional additional defaults to merge in
    :return: config object
    """
    if not os.path.isfile(filename):
        raise ValueError('Config file `{}` does not exist'.format(filename))

    defaults = {
        'hostname': '',
        'port': '',
        'username': '',
        'include_databases': '',
        'exclude_databases': '',
        'include_schemas': '',
        'exclude_schemas': '',
        'pg_dumpall_cmd': 'pg_dumpall',
        'pg_dump_cmd': 'pg_dump',
        'psql_cmd': 'psql',
    }

    if extra_defaults:
        defaults.update(extra_defaults)

    config = MyConfigParser(defaults)
    with open(filename) as config_file:
        config.readfp(config_file)

    # Enforce required options
    config.get('backup', 'backup_dir')

    return config


def sort_files(file_collection):
    """ Return list of files sorted by date

    File names are sorted by name, which, if the pattern is suitable, will
    cause the file names to be sorted by creation date.

    Another option might be to sort on mtime followed by name as a tie-breaker.
    ctime is not an option because it changes when a fellow hard link is
    removed.
    """
    def keyfunc(fname):
        return fname
    return sorted(file_collection, key=keyfunc)


def sorted_files(pattern):
    """Return files matching glob pattern, *effectively* sorted by date
    """
    return sort_files(glob.glob(pattern))


def _boolify(value):
    """
    `value` should be of type bool or else a string having the value
    /yes|no|true|false/i.

    In the latter case, return a corresponding bool value.

    If already of type bool, return as is.

    If None, return None.

    If none of the above, raise a ValueError exception.

    :param str|bool value:
    :return: boolean or None
    :rtype: boolean or None
    """
    if value is not None:
        if isinstance(value, bool):
            pass
        elif re.match(r'^true|yes$', value, re.IGNORECASE):
            value = True
        elif re.match(r'^false|no$', value, re.IGNORECASE):
            value = False
        else:
            raise ValueError("Not a string-format boolean value: " + value)
    return value


def _retention(config):
    """Extract retention-related settings"""
    return {'days_to_keep': config.safe_get('backup', 'days_to_keep', 0),
            'weeks_to_keep': config.safe_get('backup', 'weeks_to_keep', 0),
            'months_to_keep': config.safe_get('backup', 'months_to_keep', 0)}


def _conn_info(config):
    """Extract connection-related settings into format used by pg_utils"""
    return {'hostname': config.safe_get('backup', 'hostname', None),
            'port': config.safe_get('backup', 'port', None),
            'username': config.safe_get('backup', 'username', None)}


def do_backups(config, things_to_back_up, backup_type, today):
    """Make a separate backup on each database.

    `things_to_back_up` is a dict keyed on database, where each value is a
    collection of schemas to backup within the database (the collection may
    be empty).

    `backup_type` may be 'daily', 'weekly', or 'monthly'.

    If `config.separate_schema_dumps` is `yes`, then a separate backup is made
    for each schema in a database and an "empty" top-level database backup is
    also made. The reason for this is that a PostgreSQL `pg_dump` backup that
    specifies schemas cannot be used to recreate the database that originally
    included those schemas.

    If `config.separate_schema_dumps` is `no`, then what is done depends on
    whether there are schema inclusions or exclusions. If there are no
    inclusions or exclusions, then a single dump is made of the database.
    If there *are* inclusions or exclusions, then two dumps are made: one
    including the matching schemas, and one being an "empty" top-level
    database backup. As before, the two files in this case would be required
    to reconstitute the original database and schemas.

    The above logic determines the minimum number of backup files that will be
    produced.

    The other thing that determines what files actually get written is the
    `backup_type` (daily, weekly, monthly, or globals). If there is a more
    granular corresponding backup file (daily in the case of weekly, etc)
    then the backup is created as a hard-link to the more granular
    corresponding backup.

    For example, if we are doing a weekly backup, and if daily backups are also
    enabled, then if a weekly backup does not already exist for this week, the
    daily backup is hard-linked to be the weekly backup. If daily backups are
    *not* enabled, then if a weekly backup does not already exist for this
    week, an actual weekly backup needs to be performed.  The logic is similar
    for monthly backups.

    The purpose of the `today` parameter is to ensure that all of the backups
    created in the same run will have the same date stamp embedded in the file
    name (the actual file modification timestamp will be accurate, of course).
    This may or may not improve sanity.

    :param config:
    :param things_to_back_up: dict[str][SchemaCriteria]
    :param backup_type: daily, weekly, monthly
    :param today: date object to be used for backup file names
    :return: collection of file backup objects (e.g. obj.filename)
    :rtype: list[Backup]
    """
    backup_objects_for_return = []

    separate = _boolify(config.get('backup', 'separate_schema_dumps'))

    retention = _retention(config)

    conn_info = _conn_info(config)

    for database, schema_criteria in things_to_back_up.items():
        if separate:
            # Separate dump for each schema ...
            for schema in matching_schemas(schema_criteria):
                backup = Backup(config.get('backup', 'backup_dir'),
                                backup_type, database, schema, conn_info,
                                today=today)
                backup.backup(**retention)
                backup_objects_for_return.append(backup)
            # ... and one separate dump for the database
            backup = Backup(config.get('backup', 'backup_dir'), backup_type,
                            database, schema_spec=None, conn_info=conn_info,
                            today=today, empty=True)
            backup.backup(**retention)
            backup_objects_for_return.append(backup)
        else:
            # Try for single dump, but if there were any inclusions,
            # then we will also have to do an 'empty' dump of the
            # database, because of the way `pg_dump` works.
            backup = Backup(config.get('backup', 'backup_dir'),
                            backup_type, database, schema_criteria, conn_info,
                            today=today)
            backup.backup(**retention)
            backup_objects_for_return.append(backup)
            if config.safe_get('backup', 'include_schemas', None):
                backup = Backup(config.get('backup', 'backup_dir'), backup_type,
                                database, schema_spec=None, conn_info=conn_info,
                                today=today, empty=True)
                backup.backup(**retention)
                backup_objects_for_return.append(backup)

    return backup_objects_for_return


def main_backup(config):
    """Back up a PostgreSQL database.

    :param config: ConfigParser config object with "backup" section
    :return: list of file backup objects (files written)
    :rtype: list[Backup]
    """
    backup_objects_for_return = []

    # All files for this run will have the same date stamp embedded in the
    # filename.
    today = get_today()

    retention = _retention(config)

    conn_info = _conn_info(config)

    databases = matching_databases(
        conn_info,
        include=config.get('backup', 'include_databases'),
        exclude=config.get('backup', 'exclude_databases'))

    things_to_back_up = {}
    # Make a dict of databases with associated schema patterns
    for database in databases:
        things_to_back_up[database] = SchemaCriteria(
            database,
            conn_info,
            include=config.get('backup', 'include_schemas'),
            exclude=config.get('backup', 'exclude_schemas'))

    globals_backup = Backup(backup_dir=config.get('backup', 'backup_dir'),
                            backup_type='globals',
                            conn_info=conn_info,
                            today=today)
    globals_backup.backup(**retention)
    backup_objects_for_return.append(globals_backup)

    # If daily backups are enabled, delete any expired daily backup files, then
    # perform the daily backup.
    if config.safe_get('backup', 'days_to_keep', 0):
        backups = do_backups(config, things_to_back_up, 'daily', today)
        backup_objects_for_return += backups

    # If weekly backups are enabled, delete any expired weekly backup files,
    # then create the weekly backup file by the following procedure.  If we did
    # not do a daily, then we create a new weekly from scratch. If we *did* do
    # a daily, check to see if it should be hard-linked to also be the weekly.
    # The logic to do this is that we derive the ISO "week number" (WN) from
    # the current day and from the last weekly backup. If the WNs are
    # different, then promote the daily to weekly also.
    if config.safe_get('backup', 'weeks_to_keep', 0):
        backups = do_backups(config, things_to_back_up, 'weekly', today)
        backup_objects_for_return += backups

    # If monthly backups are enabled, delete any expired monthly backups, then
    # created the monthly backup file by the following procedure. If we did not
    # do a weekly or daily backup, then just create a new monthly. If we *did*
    # do another backup, check to see if it should be promoted to a monthly via
    # a hard link. The month is identified by the combination of year and month
    # number embedded in the filename.
    if config.safe_get('backup', 'months_to_keep', 0):
        backups = do_backups(config, things_to_back_up, 'monthly', today)
        backup_objects_for_return += backups

    return backup_objects_for_return


def main():
    from pgbackup import __version__
    arguments = docopt(__doc__, version='pgbackup {}'.format(__version__))
    config = read_config(arguments['-c'])
    main_backup(config)


if __name__ == '__main__':
    main()
