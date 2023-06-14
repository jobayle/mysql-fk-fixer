# MySQL invalid Foreign References

[![codecov](https://codecov.io/gh/jobayle/mysql-fk-fixer/branch/main/graph/badge.svg?token=PWJQBHUW7D)](https://codecov.io/gh/jobayle/mysql-fk-fixer)

This project finds all rows having invalid foreign references.

It can dump all rows containing such invalid references, in CSV format.

Then you can either fix these rows, or use the option to automatically delete
all rows containing such invalid references.

## Build:

`cargo build`

## Usage:

`./mysql_fk_fixer <db url> [--schema schema-name] [--dump-invalid-rows] [--dump-folder folder_location] [--auto-delete]`

example : 

```
$ ./mysql_fk_fixer 'mysql://root:root@localhost/' --schema foobar
Connecting to mysql://root:root@localhost/
MySQL server version: 5.7.40
Found 2 Foreign Key Constraints to check...
Checking Foreign Key constraint baz_ibfk_1 in schema foobar on table baz column foo_id referencing table foo column id
Checking Foreign Key constraint baz_ibfk_2 in schema foobar on table baz column bar_id referencing table bar column id
1 invalid foreign references found in table baz column bar_id
```
