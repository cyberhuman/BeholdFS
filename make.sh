#!/bin/bash

gcc -g `pkg-config fuse --cflags --libs` -lsqlite3 -std=gnu99 -DFUSE_USE_VERSION=26 -o beholdfs beholdfs.c beholddb.c fs.c

