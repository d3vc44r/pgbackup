from setuptools import setup, find_packages
from pgbackup import __version__

with open('README.md', 'r') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    install_requires = f.readlines()

with open('test_requirements.txt') as f:
    test_requires = [x for x in f.readlines() if not x.startswith('-r')]

kwargs = {
    'name': 'pgbackup',
    'version': __version__,
    'author': 'The Children\'s Hospital of Philadelphia',
    'author_email': 'cbmisupport@email.chop.edu',
    'url': 'https://github.com/chop-dbhi/postgres-backup',
    'description': ('PostgreSQL pg_backup interface with database and schema regexes '
                    'and retention policy.'),
    'long_description': long_description,
    'license': 'Other/Proprietary',
    'packages': ['pgbackup'],
    'install_requires': install_requires,
    'download_url': ('https://github.com/chop-dbhi/'
                     'pgbackup/tarball/{}'.format( __version__)),
    'keywords': ['backup', 'postgres', 'postgresql', 'backup', 'retention policy', 'pg_dump'],
    'classifiers': [
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Database Administrators',
        'License :: Other/Proprietary License',
        'Topic :: Database',
        'Topic :: Backup',
        'Natural Language :: English'
    ],
    'entry_points': {
        'console_scripts': [
            'pgbackup = pgbackup.main:main'
        ]
    },
    # Note: tests require the PostgreSQL binaries (initdb, psql, pg_dump, etc) be in the path
    'test_suite': 'pgbackup.test_pgbackup',
    'tests_require': test_requires
}

setup(**kwargs)
