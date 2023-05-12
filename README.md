# MySQL invalid Foreign References

[![codecov](https://codecov.io/gh/jobayle/mysql-fk-fixer/branch/main/graph/badge.svg?token=PWJQBHUW7D)](https://codecov.io/gh/jobayle/mysql-fk-fixer)

This project finds all rows having invalid foreign references.

It can dump all rows containing such invalid references, in CSV format.

Then you can either fix these rows, or use the option to automatically delete
all rows containing such invalid references.