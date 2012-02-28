#!/bin/bash

MOUNT="$1"
STORE="$2"
DB="$STORE/.beholddb"

dbrun()
{
  sqlite3 -nullvalue NULL "$DB"
}

dbdump()
{
  dbrun <<-EOF
    select 'OBJECTS';
    select * from objects;
    select 'OBJECTS_OWNERS';
    select * from objects_owners;
    select 'OBJECTS_TAGS';
    select * from objects_tags;
EOF
}

echo "+Basic operations"

[[ -z "$MOUNT" || -z "$STORE" ]] && {
  echo "-invalid arguments"
  return 1
}

# simple file operations
echo "++simple..."

set -x

touch "$MOUNT/file1"
touch "$MOUNT/file2"

mkdir "$MOUNT/dir1"
touch "$MOUNT/dir1/file3"
touch "$MOUNT/dir1/file4"

mkdir "$MOUNT/dir2"
touch "$MOUNT/dir2/file5"

mkdir "$MOUNT/dir1/dir3"
touch "$MOUNT/dir1/dir3/file6"
touch "$MOUNT/dir1/dir3/file7"
touch "$MOUNT/dir1/dir3/file8"

set +x

# check database
echo "++simple check..."
dbdump

sleep 5s
echo "++tags..."

set -x

touch "$MOUNT/%tag1/file1" # negative test case - EACCES
touch "$MOUNT/%tag1/file9"
touch "$MOUNT/%tag1/dir1/file10"
touch "$MOUNT/dir1/%tag1/dir3/file11"
touch "$MOUNT/dir1/dir3/%tag1/file12"

set +x

echo "++tags check..."
dbdump

