import six
import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from pgbackup import backup


class UnitTest(unittest.TestCase):

    @patch('socket.gethostname')
    def test_hostname_label(self, new_gethostname):
        new_gethostname.return_value = 'ahost'
        self.assertEqual(backup.hostname_label(''), 'ahost')

        new_gethostname.return_value = 'ahost.sub.com'
        self.assertEqual(backup.hostname_label(''), 'ahost')

        self.assertEqual(backup.hostname_label('passed'), 'passed')

    def test_port_label(self):
        self.assertEqual(backup.port_label(''), '5432')
        self.assertEqual(backup.port_label('1234'), '1234')

    def test_schema_label(self):
        self.assertEqual(backup.schema_label(['alabel'], False), 'alabel')
        self.assertEqual(backup.schema_label(['alabel', 'another'], False), 'selected_schemas')
        with six.assertRaisesRegex(self, ValueError, 'Programmer'):
            backup.schema_label({}, False)

    def test_backup_missing_required_args(self):
        with six.assertRaisesRegex(self, ValueError, 'Required argument'):
            backup.Backup(backup_type='globals')
        with six.assertRaisesRegex(self, ValueError, 'Required argument'):
            backup.Backup(backup_type='daily', backup_dir='foo', conn_info={})
        with six.assertRaisesRegex(self, ValueError, 'Required argument'):
            backup.Backup(backup_type='daily', database='foo', conn_info={})
        with six.assertRaisesRegex(self, ValueError, 'Required argument'):
            backup.Backup(backup_dir='foo', database='foo', conn_info={})

    def test_backup_invalid_conn_info(self):
        with six.assertRaisesRegex(self, ValueError, 'Invalid key'):
            backup.Backup(backup_type='daily', database='foo', backup_dir='foo',
                          conn_info={'bad': 'juju'})

    def test_backup_invalid_backup_type(self):
        with six.assertRaisesRegex(self, ValueError, 'backup_type.*not in'):
            backup.Backup(backup_type='bad', database='foo', backup_dir='foo', conn_info={
                'hostname': 'foo'})

    def test_backup_filename_invalid(self):
        with six.assertRaisesRegex(self, ValueError, 'Invalid.*filename'):
            backup.Backup(filename='a.backup.file')

    def test_backup_filename_invalid_date(self):
        with six.assertRaisesRegex(self, ValueError, 'invalid date'):
            f = 'localhost.5432.adb.aschema.12-04-2016.daily.pg_dump_Fc'
            backup.Backup(filename=f)

    def test_backup_filename_invalid_suffix(self):
        with six.assertRaisesRegex(self, ValueError, 'Invalid backup suffix'):
            f = 'localhost.5432.adb.aschema.2016-12-04.daily.bad_suffix'
            backup.Backup(filename=f)
