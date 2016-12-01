# pgbackup

[![Circle CI](https://circleci.com/gh/murphyke/pgbackup/tree/master.svg?style=svg)](https://circleci.com/gh/murphyke/pgbackup/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/murphyke/pgbackup/badge.svg?branch=master)](https://coveralls.io/github/murphyke/pgbackup?branch=master)

`pgbackup` is a Python package and script that wraps the PostgreSQL logical (SQL-based) backup
programs `pg_dump` and `pg_dumpall`.

It can manage daily, weekly, and monthly backups of selected (or excluded) databases and
selected (or excluded) schemas within those databases.

Backups can either be "normal" (all schemas in the same file) or divided into separate files for
each schema, in addition to a database-level backup file that can create the database but not any
of its contents. A globals backup is also managed. Even in the case of a "normal" backup file, if
you specify schema inclusion or exclusion patterns, then an additional database-level backup file
is created so the database itself can be restored. This latter feature is made necessary by the
behavior of `pg_dump` and/or `pg_restore`.

Files are named as follows:

    hostname.port.database.schema.date.backup_type.suffix

where:

* `hostname` defaults to 'localhost' if not specified.
* `port` defaults to 5432 if not specified.
* `schema` is a schema name, or 'all_schemas', or 'no_schemas' (in the case of a database-only
backup or a globals backup).
* `date` is a datestamp, as YYYY-mm-dd.
* `backup_type` is 'daily', 'weekly', or 'monthly'.
* `suffix` is 'globals.sql' or 'pg_dump.Fc'.

Weekly backups are created as hard links to suitable daily backups if possible, and likewise, 
monthly backups are created as hard links to weekly backups if possible. The weekly backups are 
made on the basis of [ISO weeks](https://en.wikipedia.org/wiki/ISO_week_date) for easy 
computation, so it is not possible to define a particular day of the week that weekly backups 
will be made on.multiple

## Configuration

The program behavior is determined by an INI-style configuration file. If you need multiple
retention policies, you will need to run multiple instances of `pgbackup`, each driven by its own
configuration file.

[sample.config](./sample.config) is a template configuration file. It allows you to define 
`backup_user`, `hostname`, `port`, `username`, `backup_dir`, `enable_globals_backups`, 
`separate_schema_dumps`, `include_databases`, `exclude_databases`, `include_schemas`, 
`exclude_schemas`, `days_to_keep`, `weeks_to_keep`, and `months_to_keep`.

## Credits

This script was originally based on the [pg_backup_rotated.py script on the PostgreSQL
Wiki](https://wiki.postgresql.org/wiki/Automated_Backup_on_Linux).
