#!/bin/sh
dbx schema -d pgx -d sqlite3 cachedb.dbx .
dbx golang -d pgx -d sqlite3 -p dbx -t templates cachedb.dbx .
