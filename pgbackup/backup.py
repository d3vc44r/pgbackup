import datetime
import glob
import logging
import os
import socket

from pgbackup.pg_utils import pg_dump, pg_dumpall_globals, SchemaCriteria, \
    matching_schemas

try:
    basestring = basestring
except NameError:
    # In Python 3
    basestring = str

LOGGER = logging.getLogger(__package__)


def hostname_label(hostname):
    """Return normalized hostname for consistency in file names.

    If empty, assume localhost, and furthermore, if localhost, then try
    to get the real hostname (not FQDN, though); otherwise 'localhost'.

    If the hostname is fully or partially qualified, strip away all but the
    leading hostname component (i.e. if there is a period in the hostname,
    strip it and everything following it).

    :param hostname: hostname from the config file (may be empty or None)
    """
    if not hostname:
        hostname = 'localhost'
    if hostname == '127.0.0.1' or hostname == 'localhost':
        hostname = socket.gethostname() or 'localhost'

    if '.' in hostname:
        hostname = hostname.partition('.')[0]

    return hostname


def port_label(port):
    """Return normalized port for consistency in file names.

    If empty, assume 5432.
    :param port: port from the config file (may be empty or None)
    :rtype: str
    """
    if not port:
        return '5432'
    else:
        return str(port)


def schema_label(schema_spec, empty):
    """Return normalized schema label for consistency in file names.

    `schema_spec` is a schema name, a collection of schema names,
    or a SchemaCriteria object

    Possible labels are:
        'no_schemas'
        {single schema name}
        'selected_schemas'
        'all_schemas'

    :param schema_spec: string, collection, or SchemaCriteria object
    :param bool empty: whether to use schema at all
    :return: label for schema for use in filename
    """
    if empty:
        return 'no_schemas'
    elif schema_spec is None:
        return 'no_schemas'
    elif isinstance(schema_spec, SchemaCriteria) and (
                schema_spec.include or schema_spec.exclude):
        return 'selected_schemas'
    elif isinstance(schema_spec, SchemaCriteria) and not (
                schema_spec.include or schema_spec.exclude):
        return 'all_schemas'
    elif isinstance(schema_spec, basestring):
        return schema_spec      # single schema name
    elif len(schema_spec) == 1:
        return schema_spec[0]   # collection only contains one schema
    elif len(schema_spec) > 1:
        return 'selected_schemas'
    else:
        raise ValueError('Programmer brain overflow')


def database_label(database):
    """Return normalized database label for consistency in file names.
    """
    if database:
        return database
    else:
        return 'database_na'


def get_today():
    """Return today's date object.

    Makes mocking much easier"""
    return datetime.date.today()


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


class Backup(object):
    """Encapsulate features of a backup

    TODO: get rid of date_obj - too confusing

    Can either be used to prepare to create a new backup, or to describe an
    existing backup file.

    In the case of preparing to create a new backup, the actual backup file is
    not created until the Backup.backup() method is called. For this use case,
    the constructor needs to be called with `backup_dir`, `backup_type`,
    `database`, `schema_spec`, `empty`, and, optionally, `conn_info` (a dict
    of 'hostname', 'port', and 'username').

    `schema_spec` can be None (no schemas), a string (single schema),
    collection of schema names, or a SchemaCriteria object (potentially
    multiple selected schemas, or all schemas).

        - One specified schema; dump one schema; schema label is `{name}`.
        - Multiple specified schemas; dump selected schemas; schema label is
        `selected_schemas`.
        - SchemaCriteria with no inclusions/exclusions, and `empty` is False:
        make a complete dump including all schemas; schema label is
        `all_schemas`.
        - `empty` is True: make an "empty" dump containing
        no schemas; schema label is `no_schemas`.

    In the case of describing an existing backup file, the constructor
    should be called with `filename`.

    The `today` parameter for the initializer allows the caller to control
    the date stamp used in file names in case multiple backups should share the
    same date stamp even if they might start on different days. If `today`
    is None, the actual current date object will be used.

    Usable properties:
        backup_dir
        conn_info
        hostname_label
        port_label
        database
        schema_spec
        self.schema_names (derived from schema_spec)
        date (string in %Y-%m-%d format)
        backup_type
        filename
    """
    NUM_FILENAME_PARTS = 7  # For sanity-checking our parsing:
    # host, port, database, schema, date, type, suffix
    GLOBALS_SUFFIX = 'sql'
    REGULAR_SUFFIX = 'pg_dump_Fc'
    BACKUP_TYPES = ('globals', 'daily', 'weekly', 'monthly')
    DATE_FORMAT = '%Y-%m-%d'

    @staticmethod
    def validate_backup_type(backup_type):
        if backup_type not in Backup.BACKUP_TYPES:
            raise ValueError(
                'backup_type `{}` not in `{}`'.format(backup_type,
                                                      Backup.BACKUP_TYPES))

    def _parse_filename(self, filename):
        """Parse a backup filename and initialize object attributes"""
        (self.backup_dir, basename) = os.path.split(filename)
        parts = basename.split('.')
        if len(parts) != Backup.NUM_FILENAME_PARTS:
            raise ValueError('Invalid backup filename: {}'.format(basename))

        (self.hostname_label, self.port_label, self.database_label,
         self.schema_label, self.date, self.backup_type, suffix) = parts

        self.conn_info = {
            'hostname': self.hostname_label,
            'port': self.port_label,
            'username': None}

        # Do very light validation
        try:
            datetime.datetime.strptime(self.date, Backup.DATE_FORMAT)
        except ValueError:
            raise ValueError('{} contains invalid date {}'.format(filename,
                                                                  self.date))
        Backup.validate_backup_type(self.backup_type)

        if suffix not in (Backup.GLOBALS_SUFFIX, Backup.REGULAR_SUFFIX):
            raise ValueError('Invalid backup suffix: {}'.format(suffix))

    @staticmethod
    def _validate_conn_info(conn_info):
        if not isinstance(conn_info, dict):
            raise ValueError('Invalid conn_info (not a dict)')
        for key in conn_info.keys():
            if key not in ('hostname', 'port', 'username'):
                raise ValueError('Invalid key `{}` found in conn_info'.format(key))

    def __init__(self, backup_dir=None, backup_type=None, database=None,
                 schema_spec=None, conn_info={}, filename=None,
                 today=None, empty=False):
        if filename:
            # Initialize from the name of an existing backup file.
            self._parse_filename(filename)
        else:
            # Initialize for a prospective backup.
            if backup_type == 'globals':
                # Initialize for a prospective globals backup.
                if not backup_dir:
                    raise ValueError('Required argument: backup_dir')
            else:
                # Initialize for a prospective "normal" backup.
                if not (backup_dir and backup_type and database):
                    raise ValueError('Required arguments: backup_dir, '
                                     'backup_type, database')

            self.backup_dir = backup_dir
            self._validate_conn_info(conn_info)
            self.conn_info = conn_info
            self.hostname_label = hostname_label(conn_info.get('hostname', None))
            self.port_label = port_label(conn_info.get('port', None))

            self.database_label = database_label(database)
            self.empty = empty

            self.schema_label = schema_label(schema_spec, empty)
            self.schema_spec = schema_spec
            if isinstance(schema_spec, SchemaCriteria):
                self.schema_names = matching_schemas(schema_spec)
            elif schema_spec is None:
                self.schema_names = []
            elif isinstance(schema_spec, basestring):
                self.schema_names = [schema_spec]
            else:
                self.schema_names = schema_spec

            # Sanity check to ensure that file names are consistently parsable
            # by having components separated by periods:
            for val in [self.port_label, self.database_label] + list(
                    self.schema_names):
                if '.' in val:
                    raise ValueError('Port, database, or schema may not '
                                     'contain `.`')

            if not today:
                today = get_today()
            self.date = today.strftime(Backup.DATE_FORMAT)

            self.validate_backup_type(backup_type)
            self.backup_type = backup_type

    @property
    def filename(self):
        """Return filename for backup.

        Filename pattern: hostname.port.database.schema.date.backup_type.suffix

        where suffix is 'globals.sql' or 'pg_dump.Fc'.

        Note: the hostname is not allowed to contain periods, so the components
        can later be deterministically parsed by splitting on the period
        character.
        """
        if self.backup_type == 'globals':
            suffix = Backup.GLOBALS_SUFFIX
        else:
            suffix = Backup.REGULAR_SUFFIX

        base_filename = '.'.join([self.hostname_label, self.port_label,
                                  self.database_label, self.schema_label,
                                  self.date,
                                  self.backup_type,
                                  suffix])

        # The number of separating dots should be one less than the number
        # of filename components. This is just a sanity check
        # to make sure that `filename` and `_parse` are in agreement.
        expected_dots = Backup.NUM_FILENAME_PARTS - 1
        if base_filename.count('.') != expected_dots:
            raise RuntimeError(
                "Constructed filename doesn't have {} periods: {}".format(
                    expected_dots, base_filename
                ))

        return os.path.join(self.backup_dir, base_filename)

    def filename_with_date_masked(self, override_backup_type=None):
        """Return backup filename with asterisk replacing the date.

        If `override_backup_type` is not None, then use that backup type
        in the returned file name.

        :param override_backup_type: alternate backup_type
        :rtype: str
        """
        mask_obj = Backup(filename=self.filename)
        if override_backup_type:
            # noinspection PyProtectedMember
            Backup.validate_backup_type(override_backup_type)
            mask_obj.backup_type = override_backup_type
        mask_obj.date = '*'
        return mask_obj.filename

    @property
    def date_obj(self):
        """Return date object representation of the self.date string."""
        return datetime.datetime.strptime(self.date, Backup.DATE_FORMAT).date()

    def week_number(self):
        """Return the ISO (month, week) of the backup."""
        return datetime.date.isocalendar(self.date_obj)[:2]

    def month_number(self):
        """Return the ISO (year, month) of the backup."""
        date_obj = self.date_obj
        return date_obj.year, date_obj.month

    def do_promotion(self):
        """Hard-link a more granular backup if needed.

        If a more granular backup has not already been hard-linked to become the
        backup for this week or this month, do it.

        Details: for a weekly backup, compute the week number of what would be
        today's backup (the week number being the tuple (year, iso_week_num) as
        returned by datetime.isocalendar). Then find the existing weekly
        backups, and obtain the week number for the most recent one. If the week
        numbers match, we are done. If the week numbers don't match or if
        another weekly backup doesn't exist, then find the the most recent
        corresponding daily backup and hard-link that to become the weekly
        backup. If such a daily backup doesn't exist, raise an exception.

        For a monthly backup, compute the month number of what would be today's
        backup as (year, month). Then find the month number for the most recent
        monthly backup. If the month numbers match, we are done.  If the month
        numbers don't match or if another monthly backup doesn't exist, then we
        need to make a new monthly backup; find the most recent corresponding
        weekly backup and hard-link that to become the monthly backup. If such a
        backup doesn't exist, raise an exception.

        Helpful library function:
        a_datetime.isocalendar() - return (year, weekno, dayno)
        date(y,m,d) - constructor for a date

        TODO: the code here is not very DRY, but an attempt to make it DRY might
        be very obfuscatory.

        :param Backup self: Backup object
        :return:
        """
        if self.backup_type == 'weekly':
            week_number = self.week_number()
            promote_daily_to_weekly = False
            weekly_backups = sorted_files(
                self.filename_with_date_masked())
            if not weekly_backups:
                promote_daily_to_weekly = True
            else:
                last_weekly_backup = Backup(filename=weekly_backups[-1])
                if last_weekly_backup.week_number() != week_number:
                    promote_daily_to_weekly = True
            if promote_daily_to_weekly:
                daily_mask = self.filename_with_date_masked('daily')
                daily_backups = sorted_files(daily_mask)
                if not daily_backups:
                    raise RuntimeError("Expected daily backup(s) matching {}"
                                       .format(daily_mask))
                daily_to_promote = daily_backups[-1]
                LOGGER.info('Hard-linking {} to {}'.format(daily_to_promote,
                                                           self.filename))
                os.link(daily_to_promote, self.filename)

        elif self.backup_type == 'monthly':
            month_number = self.month_number()
            promote_weekly_to_monthly = False
            monthly_backups = sorted_files(
                self.filename_with_date_masked())
            if not monthly_backups:
                promote_weekly_to_monthly = True
            else:
                last_monthly_backup = Backup(filename=monthly_backups[-1])
                if last_monthly_backup.month_number() != month_number:
                    promote_weekly_to_monthly = True
            if promote_weekly_to_monthly:
                weekly_mask = self.filename_with_date_masked('weekly')
                weekly_backups = sorted_files(weekly_mask)
                if not weekly_backups:
                    raise RuntimeError("Expected weekly backup(s) matching {}"
                                       .format(weekly_mask))
                weekly_to_promote = weekly_backups[-1]
                LOGGER.info('Hard-linking {} to {}'.format(weekly_to_promote,
                                                           self.filename))
                os.link(weekly_to_promote, self.filename)

    def has_promotable_more_granular_backups(self, days_to_keep, weeks_to_keep):
        """Return whether this backup can happen via hard-linking another.

        :param days_to_keep:
        :param weeks_to_keep:
        :return: bool
        """
        if self.backup_type == 'weekly':
            if days_to_keep:
                return True
        elif self.backup_type == 'monthly':
            if weeks_to_keep:
                return True
        return False

    def expire(self, days_to_keep, weeks_to_keep, months_to_keep):
        """Expire (delete) old backups.

        Use the mask hostname.port.database.schema.*.backup_type.suffix.

        If `today` is None (falsy, actually), the actual current day will be
        used.

        :param days_to_keep:
        :param weeks_to_keep:
        :param months_to_keep:
        :return:
        """
        # Create a backup object just to build a filename mask
        mask_obj = Backup(backup_dir=self.backup_dir,
                          backup_type=self.backup_type,
                          database=self.database_label,
                          schema_spec=self.schema_spec,
                          conn_info=self.conn_info,
                          today=self.date_obj, empty=self.empty)
        pattern = mask_obj.filename_with_date_masked()

        if self.backup_type == 'globals':
            # Keep only one per host/port combination
            num_to_keep = 1
        else:
            if self.backup_type == 'daily':
                num_to_keep = int(days_to_keep)
            elif self.backup_type == 'weekly':
                num_to_keep = int(weeks_to_keep)
            elif self.backup_type == 'monthly':
                num_to_keep = int(months_to_keep)
            else:
                raise ValueError(
                    'Invalid self.backup_type {}'.format(self.backup_type))

        for f in sorted_files(pattern)[:-num_to_keep]:
            LOGGER.info('Deleting expired backup file {}'.format(f))
            os.unlink(f)

    def backup(self, days_to_keep, weeks_to_keep, months_to_keep):
        """Perform a backup operation, including expiration.

        :return:
        """
        if self.has_promotable_more_granular_backups(days_to_keep,
                                                     weeks_to_keep):
            self.do_promotion()
        else:
            if self.backup_type == 'globals':
                pg_dumpall_globals(self.filename, self.conn_info)
            else:
                unselective = (isinstance(self.schema_spec, SchemaCriteria)
                               and not (self.schema_spec.include or
                                        self.schema_spec.exclude))
                if not self.schema_spec or unselective:
                    schema_list = []
                else:
                    schema_list = self.schema_names
                pg_dump(self.database_label, self.filename, schema_list,
                        conn_info=self.conn_info, empty=self.empty)
        self.expire(days_to_keep, weeks_to_keep, months_to_keep)
