bin_PROGRAMS = beholdfs
beholdfs_SOURCES = beholddb.c beholdfs.c common.c fs.c schema.c version.c
beholdfs_CFLAGS = -g -std=gnu99 -fms-extensions -DFUSE_USE_VERSION=26 `pkg-config fuse --cflags`
LIBS = `pkg-config fuse --libs` -lsqlite3

