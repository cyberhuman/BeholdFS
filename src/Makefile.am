bin_PROGRAMS = beholdfs
beholdfs_SOURCES = beholddb.c beholdfs.c common.c fs.c schema.c version.c
beholdfs_CFLAGS = -g `pkg-config fuse --cflags --libs` -lsqlite3 -std=gnu99 -fms-extensions -DFUSE_USE_VERSION=26

