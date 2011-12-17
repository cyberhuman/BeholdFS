#!/bin/bash

MOUNT="$1"
STORE="$2"
DB="$STORE/.beholddb"

echo "+Real Life tests"

set -x

touch "$MOUNT/file0"
touch "$MOUNT/%tag1/file1"
mkdir "$MOUNT/dir0"
mkdir "$MOUNT/dir1"
touch "$MOUNT/%tag2/dir1/file2"
ls "$MOUNT"
ls "$MOUNT/%tag1"
ls "$MOUNT/%tag2"
ls "$MOUNT/%tag1%tag2"
ls "$MOUNT/%tag2%tag1"
ls "$MOUNT/%-tag1"
ls "$MOUNT/%-tag2"
ls "$MOUNT/%-tag1%-tag2"
ls "$MOUNT/%-tag2%-tag1"
ls "$MOUNT/%tag1%-tag2"
ls "$MOUNT/%-tag2%-tag1"
ls "$MOUNT/%-tag1%tag2"
ls "$MOUNT/%tag2%tag1"

ls "$MOUNT/%tagX"
ls "$MOUNT/%tag1%-tagX"
ls "$MOUNT/%-tag1%tagX"
ls "$MOUNT/%-tag1%-tagX"
ls "$MOUNT/%tag2%-tagX"
ls "$MOUNT/%-tag2%tagX"
ls "$MOUNT/%-tag2%-tagX"

set +x

