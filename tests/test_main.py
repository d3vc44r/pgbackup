import unittest

from pgbackup import main


class TestMain(unittest.TestCase):

    def test_MyConfigParser(self):
        m = main.MyConfigParser()
        m.add_section('a_section')
        m.set('a_section', 'fookey', 'fooval')
        self.assertEqual(m.get('a_section', 'fookey'), 'fooval')
        self.assertEqual(m.safe_get('a_section', 'notexists', 'default'), 'default')
        self.assertEqual(m.safe_get('not_a_section', 'fookey', 'default'), 'default')

    def test_read_config(self):
        with self.assertRaisesRegexp(ValueError, 'does not exist'):
            main.read_config('not_a_file')


    def test_boolify(self):
        self.assertEqual(main._boolify('yes'), True)
        self.assertEqual(main._boolify('Yes'), True)
        self.assertEqual(main._boolify('true'), True)
        self.assertEqual(main._boolify('TRUE'), True)
        self.assertEqual(main._boolify('no'), False)
        self.assertEqual(main._boolify('No'), False)
        self.assertEqual(main._boolify('false'), False)
        self.assertEqual(main._boolify('FALSE'), False)
        self.assertEqual(main._boolify(True), True)
        with self.assertRaisesRegexp(ValueError, 'Not a string-format boolean'):
            main._boolify('gurgle')
