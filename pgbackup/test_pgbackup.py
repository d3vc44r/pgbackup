import collections
import datetime
import os
import shutil
import subprocess
import tempfile
import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import testing.postgresql

from pgbackup import backup
from pgbackup import main
from pgbackup import pg_utils


DAYS_PER_WEEK = 7
DAYS_PER_MONTH = 30.42

# Generate PostgreSQL class which shares the generated database
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


def tearDownModule(self):
    # clear cached database at end of tests
    Postgresql.clear_cache()


class TestBackup(unittest.TestCase):
    def setUp(self):
        self.postgresql = Postgresql()
        dsn = self.postgresql.dsn()
        self.dburi = self.postgresql.url()
        self.conn_info = {
            'username': dsn['user'],
            'hostname': dsn['host'],
            'port': str(dsn['port']),
        }
        self.tmpdir = tempfile.mkdtemp()
        defaults = {
            'username': dsn['user'],
            'hostname': dsn['host'],
            'port': str(dsn['port']),
            'backup_dir': self.tmpdir
        }

        self.config = main.read_config('sample.config', defaults)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        self.postgresql.stop()

    def test_run_command_failure(self):
        with self.assertRaises(subprocess.CalledProcessError):
            pg_utils.psql('badcommand', 'test', self.conn_info)

    def test_list_schemas(self):
        pg_utils.psql('create schema newschema', 'test', self.conn_info)
        schemas = pg_utils.get_schemas('test', self.conn_info)
        self.assertSetEqual({'public', 'newschema'}, set(schemas))
        pg_utils.psql('drop schema newschema', 'test', self.conn_info)

    def test_list_databases(self):
        databases = pg_utils.get_databases(self.conn_info)
        self.assertSetEqual(
            {'test'},
            set(databases))

    def test_filter(self):
        things = {'apple', 'banana', 'cherry', 'durian', 'eggplant'}

        self.assertSetEqual(pg_utils.filter_collection(things, include='a'),
                            {'apple', 'banana', 'durian', 'eggplant'})
        self.assertSetEqual(pg_utils.filter_collection(things, include='a',
                                                       exclude='n$'),
                            {'apple', 'banana', 'eggplant'})
        self.assertSetEqual(pg_utils.filter_collection(things),
                            things)
        self.assertSetEqual(pg_utils.filter_collection(things, exclude='a'),
                            {'cherry'})

    def check_files(self, no_count_checks=False, globals_count=1,
                    daily_count=1, weekly_count=1,
                    monthly_count=1, schemas=None):
        """Assert expected counts and return day, week, and month numbers.

        Returns {'daily': {'schemalabel1': [day numbers], ...},
                 'weekly': {'schemalabel1': [week numbers], ...},
                 'monthly': {'schemalabel1': [week numbers], ...},

        If `no_count_checks` is True, then the expected count values are not
        required or checked; the only action is to return the "numbers".
        returned.

        If `schemas` is not None or empty, it is expected that there will be one file of each type
        per schema, plus one for the empty database; the `schemalabel` of the latter is
        'no_schemas'.

        If `schemas` is None or empty, it is expected that there will be one file of each type,
        and the `schemalabel` will be 'all_schemas'.

        :param globals_count:
        :param daily_count:
        :param weekly_count:
        :param monthly_count:
        :param schemas:
        :return: see above
        """
        files = os.listdir(self.tmpdir)

        hostport = '{}.{}'.format(backup.hostname_label(self.config.get(
            'backup', 'hostname')), self.config.get('backup', 'port'))

        if not no_count_checks:
            tpl = r'{}\.database_na\.no_schemas\..*\.globals.sql'
            globals_pat = tpl.format(hostport)
            global_files = pg_utils.filter_collection(files,
                                                      include=globals_pat)
            self.assertEquals(len(global_files), globals_count)

        if schemas:
            num_schemas = len(schemas)
        else:
            num_schemas = 0

        tpl = r'{hostport}\.test\..*\..*\.{type}.pg_dump_Fc'

        daily_pat = tpl.format(hostport=hostport, type='daily')
        daily_files = pg_utils.filter_collection(files, include=daily_pat)
        day_numbers = collections.defaultdict(list)
        for f in backup.sort_files(daily_files):
            this_backup = backup.Backup(filename=f)
            day_number = this_backup.date
            schema = this_backup.schema_label
            # Make sure we don't have redundant daily backups (no-brainer)
            self.assertNotIn(day_number, day_numbers[schema])
            day_numbers[schema].append(day_number)

        if not no_count_checks:
            self.assertEquals(len(daily_files), daily_count * (num_schemas + 1))

        weekly_pat = tpl.format(hostport=hostport, type='weekly')
        weekly_files = pg_utils.filter_collection(files, include=weekly_pat)
        week_numbers = collections.defaultdict(list)
        for f in backup.sort_files(weekly_files):
            this_backup = backup.Backup(filename=f)
            week_number = this_backup.week_number()
            schema = this_backup.schema_label
            # Make sure we don't have redundant weekly backups
            self.assertNotIn(week_number, week_numbers[schema])
            week_numbers[schema].append(week_number)

        if not no_count_checks:
            self.assertEquals(len(weekly_files), weekly_count * (num_schemas
                                                                 + 1))

        monthly_pat = tpl.format(hostport=hostport, type='monthly')
        monthly_files = pg_utils.filter_collection(files, include=monthly_pat)
        month_numbers = collections.defaultdict(list)
        for f in backup.sort_files(monthly_files):
            this_backup = backup.Backup(filename=f)
            month_number = this_backup.month_number()
            schema = this_backup.schema_label
            # Make sure we don't have redundant monthly backups
            self.assertNotIn(month_number, month_numbers[schema])
            month_numbers[schema].append(month_number)

        if not no_count_checks:
            self.assertEquals(len(monthly_files), monthly_count * (
                num_schemas + 1))

        return {'daily': day_numbers,
                'weekly': week_numbers,
                'monthly': month_numbers}

    # noinspection PyMethodMayBeStatic
    def starting_date(self):
        """Fake a starting date at the beginning of an ISO week.

        The date returned is 2016-01-04, which is the start of ISO week 1.

        An illuminating snippet:
        ```
        first_day = date(2016, 1, 1)
        for delta in range(0, 365):
            this_day = first_day + timedelta(days=delta)
            print delta, this_day, this_day.isocalendar()
        """
        return datetime.date(2016, 1, 4)

    @patch('main.get_today')
    def test_basic_backups(self, mock_today):
        mock_today.return_value = self.starting_date()
        main.main_backup(self.config)
        self.check_files()

    @patch('main.get_today')
    def test_multiple_schemas(self, mock_today):
        mock_today.return_value = self.starting_date()
        pg_utils.psql('create schema newschema', 'test', self.conn_info)
        main.main_backup(self.config)
        # Without include or exclude settings, just one file is created
        # per category.
        self.check_files(daily_count=1, weekly_count=1, monthly_count=1)
        # How about with exclude?
        self.config.set('backup', 'exclude_schemas', 'nomatch')
        main.main_backup(self.config)

        pg_utils.psql('drop schema newschema', 'test', self.conn_info)

    def test_restore(self):
        # Create full backup, then destroy the database and restore
        pg_utils.psql('create schema newschema', 'test', self.conn_info)
        backups = main.main_backup(self.config)
        self.check_files()
        for b in backups:
            if 'daily' in b.filename:
                filename = b.filename
                break  # This is guaranteed to happen by check_files()

        pg_utils.psql('drop database test', 'template1', self.conn_info)
        databases = pg_utils.get_databases(self.conn_info)
        self.assertNotIn('test', databases)

        pg_utils.pg_restore(filename, conn_info=self.conn_info)
        databases = pg_utils.get_databases(self.conn_info)
        self.assertIn('test', databases)

        schemas = pg_utils.get_schemas('test', self.conn_info)
        self.assertSetEqual({'newschema', 'public'}, set(schemas))

    def test_multi_part_restore(self):
        # Create separate schema and database backups, then destroy the
        # database and restore
        pg_utils.psql('create schema newschema', 'test', self.conn_info)

        self.config.set('backup', 'separate_schema_dumps', 'yes')

        backups = main.main_backup(self.config)
        self.check_files(schemas=['newschema', 'public'])

        pg_utils.psql('drop database test', 'template1', self.conn_info)
        databases = pg_utils.get_databases(self.conn_info)
        self.assertNotIn('test', databases)

        files = [b.filename for b in backups]
        database_backup_filename = pg_utils.filter_collection(
            files,
            include='no_schemas.*daily').pop()
        newschema_backup_filename = pg_utils.filter_collection(
            files,
            include='newschema.*daily').pop()
        public_backup_filename = pg_utils.filter_collection(
            files,
            include='public.*daily').pop()

        pg_utils.pg_restore(database_backup_filename, conn_info=self.conn_info)
        pg_utils.pg_restore(newschema_backup_filename,
                            database='test', conn_info=self.conn_info)
        pg_utils.pg_restore(public_backup_filename,
                            database='test', conn_info=self.conn_info)

        databases = pg_utils.get_databases(self.conn_info)
        self.assertIn('test', databases)

        schemas = pg_utils.get_schemas('test', self.conn_info)
        self.assertSetEqual({'newschema', 'public'}, set(schemas))

    @patch('main.get_today')
    def test_double_backups(self, mock_today):
        # Test two backups on the same day; second set should override the
        # first.
        mock_today.return_value = self.starting_date()
        main.main_backup(self.config)
        main.main_backup(self.config)
        self.check_files()

    def _test_d_w_m(self, mock_today, days_to_keep, weeks_to_keep,
                    months_to_keep, total_days, expected_day_changes,
                    expected_week_changes, expected_month_changes):
        self.config.set('backup', 'days_to_keep', str(days_to_keep))
        self.config.set('backup', 'weeks_to_keep', str(weeks_to_keep))
        self.config.set('backup', 'months_to_keep', str(months_to_keep))

        # Track how many times we get a distinct backup in a
        day_number_changes = 0
        week_number_changes = 0
        month_number_changes = 0

        last_week_number = tuple()
        last_month_number = tuple()
        last_day_number = None

        for days in range(0, total_days + 1):
            # Set clock to starting date + days
            mock_today.return_value = self.starting_date() + \
                                      datetime.timedelta(days=days)
            main.main_backup(self.config)

            # Verify file counts
            numbers = self.check_files(no_count_checks=True)

            # Count the number of distinct files of each type
            new_day_number = numbers['daily']['all_schemas'][-1]
            if not last_day_number or new_day_number != last_day_number:
                if last_day_number:
                    day_number_changes += 1
                last_day_number = new_day_number

            new_week_number = numbers['weekly']['all_schemas'][-1]
            if not last_week_number or new_week_number != last_week_number:
                if last_week_number:
                    week_number_changes += 1
                last_week_number = new_week_number

            new_month_number = numbers['monthly']['all_schemas'][-1]
            if not last_month_number or new_month_number != last_month_number:
                if last_month_number:
                    month_number_changes += 1
                last_month_number = new_month_number

        self.check_files(daily_count=days_to_keep, weekly_count=weeks_to_keep,
                         monthly_count=months_to_keep)

        self.assertEqual(day_number_changes, expected_day_changes)
        self.assertEqual(week_number_changes, expected_week_changes)
        self.assertEqual(month_number_changes, expected_month_changes)

    @patch('main.get_today')
    def test_1d_1w_1m(self, mock_today):
        # Takes 15 sec on my Macbook Pro
        self._test_d_w_m(mock_today, days_to_keep=1, weeks_to_keep=1,
                         months_to_keep=1, total_days=32,
                         expected_day_changes=32, expected_week_changes=4,
                         expected_month_changes=1)

    @patch('main.get_today')
    def test_7d_4w_12m(self, mock_today):
        # Takes 2.6 min on my Macbook Pro
        self._test_d_w_m(mock_today, days_to_keep=7, weeks_to_keep=4,
                         months_to_keep=12, total_days=365,
                         expected_day_changes=365, expected_week_changes=52,
                         expected_month_changes=12)
