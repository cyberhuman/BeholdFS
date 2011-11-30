/*
 Copyright 2011 Roman Vorobets

 This file is part of BeholdFS.

 BeholdFS is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 BeholdFS is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with BeholdFS.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <sys/times.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <unistd.h>
#include <stddef.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <syslog.h>

#include <fuse/fuse.h>
#include <fuse/fuse_opt.h>

#include "beholdfs.h"
#include "beholddb.h"

// find file by mixed path
// return error if not found
static int beholdfs_get_file(const char *path, beholddb_path **pbpath)
{
	syslog(LOG_DEBUG, "beholdfs_get_file(path=%s)", path);

	int rc;

	(rc = beholddb_parse_path(path, pbpath)) ||
	(rc = beholddb_locate_file(*pbpath));

	if (rc)
		syslog(LOG_ERR, "beholdfs_get_file: error %d", rc);
	return rc;
}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.	 The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int beholdfs_getattr(const char *path, struct stat *stat)
{
  int rc, ret = -ENOENT;
  beholddb_path *bpath;

  syslog(LOG_DEBUG, "beholdfs_getattr(path=%s)", path);
  if (!beholddb_parse_path(path, &bpath))
  {
    if (bpath->listing)
    {
      syslog(LOG_DEBUG, "beholdfs_getattr: listing was requested");
    }
    if (lstat(bpath->realpath, stat))
    {
      ret = -errno;
    } else
    {
      rc = beholddb_locate_file(bpath);
      switch (rc)
      {
      case BEHOLDDB_HIDDEN:
        if (!S_ISDIR(stat->st_mode))
        {
          ret = -EACCES;
          break;
        }
        // is a directory, fall through and succeed for hidden files
      case BEHOLDDB_OK:
        ret = 0;
      }
    }
  }
  syslog(LOG_DEBUG, "beholdfs_getattr: ret=%d", ret);
  beholddb_free_path(bpath);
  return ret;
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.	If the linkname is too long to fit in the
 * buffer, it should be truncated.	The return value should be 0
 * for success.
 */
int beholdfs_readlink(const char *path, char *buf, size_t bufsiz)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_readlink(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = readlink(bpath->realpath, buf, bufsiz)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_readlink: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Create a file node
 *
 * This is called for creation of all non-directory, non-symlink
 * nodes.  If the filesystem defines a create() method, then for
 * regular files that will be called instead.
 */
int beholdfs_mknod(const char *path, mode_t mode, dev_t dev)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_mknod(path=%s)", path);
	if (!(beholddb_parse_path(path, &bpath)))
	{
		if ((ret = mknod(bpath->realpath, mode, dev)))
			ret = -errno; else
			beholddb_create_file(bpath, 0);
	}
	syslog(LOG_DEBUG, "beholdfs_mknod: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Create a directory 
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits use  mode|S_IFDIR
 * */
int beholdfs_mkdir(const char *path, mode_t mode)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_mkdir(path=%s)", path);
	if (!(beholddb_parse_path(path, &bpath)))
	{
		if ((ret = mkdir(bpath->realpath, mode)))
			ret = -errno; else
			beholddb_create_file(bpath, 1);
	}
	syslog(LOG_DEBUG, "beholdfs_mkdir: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Remove a file */
int beholdfs_unlink(const char *path)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_unlink(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = unlink(bpath->realpath)))
			ret = -errno; else
			beholddb_delete_file(bpath);
	}
	syslog(LOG_DEBUG, "beholdfs_unlink: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Remove a directory */
int beholdfs_rmdir(const char *path)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_rmdir(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = rmdir(bpath->realpath)))
			ret = -errno; else
			beholddb_delete_file(bpath);
	}
	syslog(LOG_DEBUG, "beholdfs_rmdir: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Create a symbolic link */
int beholdfs_symlink(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	beholddb_path *oldbpath;
	beholddb_path *newbpath;

	syslog(LOG_DEBUG, "beholdfs_symlink(oldpath=%s, newpath=%s)", oldpath, newpath);

	int rc1 = beholdfs_get_file(oldpath, &oldbpath);
	int rc2 = beholddb_parse_path(newpath, &newbpath);

	if (!rc1 && !rc2)
	{
		if (-1 == (ret = symlink(oldbpath->realpath, newbpath->realpath)))
			ret = -errno; else
			beholddb_create_file(newbpath, 0); // TODO: how to deal with symlinks to directories?
	}
	syslog(LOG_DEBUG, "beholdfs_symlink: ret=%d", ret);
	if (!rc1)
		beholddb_free_path(oldbpath);
	if (!rc2)
		beholddb_free_path(newbpath);
	return ret;
}

/** Rename a file */
int beholdfs_rename(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	beholddb_path *oldbpath;
	beholddb_path *newbpath;

	syslog(LOG_DEBUG, "beholdfs_rename(oldpath=%s, newpath=%s)", oldpath, newpath);

	int rc1 = beholdfs_get_file(oldpath, &oldbpath);
	int rc2 = beholddb_parse_path(newpath, &newbpath);

	if (!rc1 && !rc2)
	{
		if ((ret = rename(oldbpath->realpath, newbpath->realpath)))
			ret = -errno; else
		{
			syslog(LOG_DEBUG, "beholdfs_rename: rename was successful");
			// TODO: optimize rename within the same directory
			beholddb_rename_file(oldbpath, newbpath);
		}
	}
	syslog(LOG_DEBUG, "beholdfs_rename: ret=%d", ret);
	if (!rc1)
		beholddb_free_path(oldbpath);
	if (!rc2)
		beholddb_free_path(newbpath);
	return ret;
}

/** Create a hard link to a file */
int beholdfs_link(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	beholddb_path *oldbpath;
	beholddb_path *newbpath;

	syslog(LOG_DEBUG, "beholdfs_link(oldpath=%s, newpath=%s)", oldpath, newpath);

	int rc1 = beholdfs_get_file(oldpath, &oldbpath);
	int rc2 = beholddb_parse_path(newpath, &newbpath);

	if (!rc1 && !rc2)
	{
		if (-1 == (ret = link(oldbpath->realpath, newbpath->realpath)))
			ret = -errno; else
			beholddb_create_file(newbpath, 0);
	}
	syslog(LOG_DEBUG, "beholdfs_link: ret=%d", ret);
	if (!rc1)
		beholddb_free_path(oldbpath);
	if (!rc2)
		beholddb_free_path(newbpath);
	return ret;
}

/** Change the permission bits of a file */
int beholdfs_chmod(const char *path, mode_t mode)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_chmod(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = chmod(bpath->realpath, mode)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_chmod: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Change the owner and group of a file */
int beholdfs_chown(const char *path, uid_t owner, gid_t group)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_chown(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = chown(bpath->realpath, owner, group)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_chown: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Change the size of a file */
int beholdfs_truncate(const char *path, off_t length)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_truncate(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = truncate(bpath->realpath, length)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_truncate: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** File open operation
 *
 * No creation (O_CREAT, O_EXCL) and by default also no
 * truncation (O_TRUNC) flags will be passed to open(). If an
 * application specifies O_TRUNC, fuse first calls truncate()
 * and then open(). Only if 'atomic_o_trunc' has been
 * specified and kernel version is 2.6.24 or later, O_TRUNC is
 * passed on to open.
 *
 * Unless the 'default_permissions' mount option is given,
 * open should check if the operation is permitted for the
 * given flags. Optionally open may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to all file operations.
 * * Changed in version 2.2
 */
int beholdfs_open(const char *path, struct fuse_file_info *fi)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_open(path=%s, flags=%2x)", path, fi->flags);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if (-1 == (fi->fh = open(bpath->realpath, fi->flags)))
			ret = -errno; else
			ret = 0;
	}
	syslog(LOG_DEBUG, "beholdfs_open: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.	 An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int beholdfs_read(const char *path, char *buf, size_t count, off_t offset,
	     struct fuse_file_info *fi)
{
	int ret;
	syslog(LOG_DEBUG, "beholdfs_read(path=%s...)", path);
	if (-1 == (ret = pread(fi->fh, buf, count, offset)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_read: ret=%d", ret);
	return ret;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.	 An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
int beholdfs_write(const char *path, const char *buf, size_t count, off_t offset,
		   struct fuse_file_info *fi)
{
	int ret;
	syslog(LOG_DEBUG, "beholdfs_write(path=%s...)", path);
	if (-1 == (ret = pwrite(fi->fh, buf, count, offset)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_write: ret=%d", ret);
	return ret;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int beholdfs_statfs(const char *path, struct statvfs *statv)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_statfs(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = statvfs(bpath->realpath, statv)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_statfs: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().	This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.	It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int beholdfs_flush(const char *path, struct fuse_file_info *fi)
{
	int ret = 0;
	// nothing to do
	return ret;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.	 It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int beholdfs_release(const char *path, struct fuse_file_info *fi)
{
	int ret;
	syslog(LOG_DEBUG, "beholdfs_release(path=%s...)", path);
	if (-1 == (ret = close(fi->fh)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_release: ret=%d", ret);
	return ret;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int beholdfs_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
	int ret;
	syslog(LOG_DEBUG, "beholdfs_fsync(path=%s...)", path);
	if (-1 == (ret = datasync ? fdatasync(fi->fh) : fsync(fi->fh)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_fsync: ret=%d", ret);
	return ret;
}

/** Set extended attributes */
int beholdfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_setxattr(path=%s,name=%s,value=%s,size=%d)", path, name, value, size);
	if (!(beholddb_get_file(path, &bpath)))
	{
		if ((ret = lsetxattr(bpath->realpath, name, value, size, flags)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_setxattr: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

static const char BEHOLDFS_TAG_XATTR[] = "user.tags";

/** Get extended attributes */
int beholdfs_getxattr(const char *path, const char *name, char *value, size_t size)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_getxattr(path=%s,name=%s,size=%d)", path, name, size);
	if (!(beholddb_get_file(path, &bpath)))
	{
		if (!strcmp(name, BEHOLDFS_TAG_XATTR))
		{
			void *handle;

			if (beholddb_opentags(bpath, &handle))
				ret = -ENOENT; else
			{
				ret = 0;
				while (!beholddb_listdir(handle, &name))
				{
					size_t len = strlen(name);

					ret += 1 + len;
					if (size)
					{
						if (ret > size)
						{
							ret = -ERANGE;
							break;
						}
						*value++ = BEHOLDFS_STATE->tagchar;
						memcpy(value, name, len);
						value += len;
					}
				}
				beholddb_closedir(handle);
			}
		} else
		{
			if ((ret = lgetxattr(bpath->realpath, name, value, size)) < 0)
				ret = -errno;
		}
	}
	syslog(LOG_DEBUG, "beholdfs_getxattr: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** List extended attributes */
int beholdfs_listxattr(const char *path, char *list, size_t size)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_listxattr(path=%s,size=%d)", path, size);
	if (!(beholddb_get_file(path, &bpath)))
	{
		if (size && size < sizeof(BEHOLDFS_TAG_XATTR))
			ret = -ERANGE; else
		{
			if (size)
			{
				memcpy(list, BEHOLDFS_TAG_XATTR, sizeof(BEHOLDFS_TAG_XATTR));
				list += sizeof(BEHOLDFS_TAG_XATTR);
				size -= sizeof(BEHOLDFS_TAG_XATTR);
			}
			if ((ret = llistxattr(bpath->realpath, list, size)) < 0)
				ret = -errno; else
				ret += sizeof(BEHOLDFS_TAG_XATTR);
		}
	}
	syslog(LOG_DEBUG, "beholdfs_listxattr: ret=%d", ret);
	for (int i = 0; i < ret - sizeof(BEHOLDFS_TAG_XATTR); i += 1 + strlen(&list[i]))
		syslog(LOG_DEBUG, "beholdfs_listxattr: list[]=%s", &list[i]);
	beholddb_free_path(bpath);
	return ret;
}

/** Remove extended attributes */
int beholdfs_removexattr(const char *path, const char *name)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_removexattr(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = lremovexattr(bpath->realpath, name)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_removexattr: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Open directory
 *
 * Unless the 'default_permissions' mount option is given,
 * this method should check if opendir is permitted for this
 * directory. Optionally opendir may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to readdir, closedir and fsyncdir.
 *
 * Introduced in version 2.3
 */
int beholdfs_opendir(const char *path, struct fuse_file_info *fi)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_opendir(path=%s)", path);
	if (!beholdfs_get_file(path, &bpath)) // TODO: handle errors
	{
		DIR *dir = NULL;
		void *handle = NULL;
		struct dirent *entry = NULL;
		int stage = 0;

		ret = 0;
		if (bpath->listing)
		{
			if (beholddb_opendir(bpath, &handle))
				ret = -ENOENT;
		} else
		{
			if (!(dir = opendir(bpath->realpath)))
				ret = -errno; else
			if (beholddb_opendir(bpath, &handle))
				ret = -ENOENT; else
			{
				int len = offsetof(struct dirent, d_name) +
					pathconf(bpath->realpath, _PC_NAME_MAX) + 1;

				entry = (struct dirent*)malloc(len);
				stage = BEHOLDFS_STATE->tagshow ? 2 : 1;
			}
		}
		if (ret)
		{
			if (dir)
				closedir(dir);
		} else
		{
			beholdfs_dir *fsdir = (beholdfs_dir*)malloc(sizeof(beholdfs_dir));

			fsdir->dir = dir;
			fsdir->handle = handle;
			fsdir->stage = stage;
			fsdir->entry = entry;

			fsdir->result = NULL;
			fsdir->dbresult = NULL;

			fi->fh = (intptr_t)fsdir;
		}
	}
	syslog(LOG_DEBUG, "beholdfs_opendir: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int beholdfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset,
		struct fuse_file_info *fi)
{
	syslog(LOG_DEBUG, "beholdfs_readdir(path=%s, offset=%d)", path, (int)offset);

	beholdfs_dir *fsdir = (beholdfs_dir*)(intptr_t)fi->fh;
	struct stat stat;
	int ret;

	if (fsdir->stage)
	{
		if (2 == fsdir->stage) // show tag character
		{
			const char LISTING_DIR[] = { BEHOLDFS_STATE->tagchar, 0 };

			memset(&stat, 0, sizeof(stat));
			stat.st_mode = S_IFDIR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
			stat.st_nlink = 1;

			if (filler(buffer, LISTING_DIR, &stat, ++offset))
			{
				syslog(LOG_ERR, "beholdfs_readdir: could not add listing dir");
				return 0; // buffer is full (should not happen)
			}

			--fsdir->stage;
		}

		// stage 1
		while (1)
		{
			if (!fsdir->result)
			{
				// read the directory and test each entry with database layer
				while (!(ret = -readdir_r(fsdir->dir, fsdir->entry, &fsdir->result)) && fsdir->result &&
					beholddb_readdir(fsdir->handle, fsdir->result->d_name));
				syslog(LOG_DEBUG, "beholdfs_readdir: ret=%d, result=%p", ret, fsdir->result);
				if (ret || !fsdir->result) // error or end of listing
					break;
			}

			char *name = fsdir->result->d_name;
			int fd = dirfd(fsdir->dir);

			ret = fstatat(fd, name, &stat, AT_SYMLINK_NOFOLLOW);
			if (filler(buffer, name, ret ? NULL : &stat, ++offset))
			{
				syslog(LOG_DEBUG, "beholdfs_readdir: buffer is full, offset=%d", (int)offset);
				return 0; // buffer is full
			}
			syslog(LOG_DEBUG, "beholdfs_readdir: added '%s'", name);
			fsdir->result = NULL;
		}
	} else
	{
		if (!fsdir->handle) // temporary workaround
		{
			syslog(LOG_ERR, "beholdfs_readdir: trying to list tags in directory without metadata");
			return -ENOENT;
		}
		ret = 0;

		memset(&stat, 0, sizeof(stat));
		stat.st_mode = S_IFDIR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
		stat.st_nlink = 1;

		// stage 0
		while (1)
		{
			if (!fsdir->dbresult)
			{
				if (beholddb_listdir(fsdir->handle, &fsdir->dbresult) || !fsdir->dbresult)
					break;
				syslog(LOG_DEBUG, "beholdfs_readdir: ret=%d, result=%p", ret, fsdir->dbresult);
			}

			if (filler(buffer, fsdir->dbresult, &stat, ++offset))
			{
				syslog(LOG_DEBUG, "beholdfs_readdir: buffer is full, offset=%d", (int)offset);
				return 0; // buffer is full
			}
			syslog(LOG_DEBUG, "beholdfs_readdir: added '%s'", fsdir->dbresult);
			fsdir->dbresult = NULL;
		}
	}
	return ret;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int beholdfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	beholdfs_dir *fsdir = (beholdfs_dir*)(intptr_t)fi->fh;

	syslog(LOG_DEBUG, "beholdfs_releasedir(path=%s)", path);
	beholddb_closedir(fsdir->handle);
	closedir(fsdir->dir);
	free(fsdir->entry);
	free(fsdir);
	syslog(LOG_DEBUG, "beholdfs_releasedir");
	return 0;
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
int beholdfs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
	syslog(LOG_DEBUG, "beholdfs_fsyncdir(path=%s)", path);
	beholdfs_dir *fsdir = (beholdfs_dir*)(intptr_t)fi->fh;
	int fd = dirfd(fsdir->dir);
	int ret;

	if (fd < 0 || (ret = datasync ? fdatasync(fd) : fsync(fd)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_fsyncdir: ret=%d)", ret);
	return ret;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
void *beholdfs_init(struct fuse_conn_info *conn)
{
	syslog(LOG_DEBUG, "beholdfs_init()");

	beholdfs_state *state = BEHOLDFS_STATE;
	extern char beholddb_tagchar;
	extern int beholddb_new_locate;

	beholddb_tagchar = state->tagchar;
	beholddb_new_locate = state->new_locate;

	if (fchdir(state->rootdir))
	{
		syslog(LOG_ERR, "beholdfs_init(): cannot fchdir: %s", strerror(errno));
		// TODO: how to handle this?
	}
	close(state->rootdir);

	return state;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void beholdfs_destroy(void *private_data)
{
	syslog(LOG_DEBUG, "beholdfs_destroy()");
	beholdfs_state *state = (beholdfs_state*)private_data;

	free(state);
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int beholdfs_access(const char *path, int mode)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_access(path=%s)", path);
	if (!beholdfs_get_file(path, &bpath))
	{
		if ((ret = access(bpath->realpath, mode)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_access: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int beholdfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_create(path=%s)", path);
	if (!beholddb_parse_path(path, &bpath))
	{
		if (-1 == (fi->fh = creat(bpath->realpath, mode)))
			ret = -errno; else
		{
			ret = 0;
			beholddb_create_file(bpath, 0);
		}
	}
	syslog(LOG_DEBUG, "beholdfs_create: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int beholdfs_ftruncate(const char *path, off_t length, struct fuse_file_info *fi)
{
	int ret;

	syslog(LOG_DEBUG, "beholdfs_ftruncate(path=%s)", path);
	if ((ret = ftruncate(fi->fh, length)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_ftruncate: ret=%d", ret);
	return ret;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int beholdfs_fgetattr(const char *path, struct stat *stat, struct fuse_file_info *fi)
{
	int ret;

	syslog(LOG_DEBUG, "beholdfs_fgetattr(path=%s)", path);
	if ((ret = fstat(fi->fh, stat)))
		ret = -errno;
	syslog(LOG_DEBUG, "beholdfs_fgetattr: ret=%d", ret);
	return ret;
}

/**
 * Perform POSIX file locking operation
 *
 * The cmd argument will be either F_GETLK, F_SETLK or F_SETLKW.
 *
 * For the meaning of fields in 'struct flock' see the man page
 * for fcntl(2).  The l_whence field will always be set to
 * SEEK_SET.
 *
 * For checking lock ownership, the 'fuse_file_info->owner'
 * argument must be used.
 *
 * For F_GETLK operation, the library will first check currently
 * held locks, and if a conflicting lock is found it will return
 * information without calling this method.	 This ensures, that
 * for local locks the l_pid field is correctly filled in.	The
 * results may not be accurate in case of race conditions and in
 * the presence of hard links, but it's unlikly that an
 * application would rely on accurate GETLK results in these
 * cases.  If a conflicting lock is not found, this method will be
 * called, and the filesystem may fill out l_pid by a meaningful
 * value, or it may leave this field zero.
 *
 * For F_SETLK and F_SETLKW the l_pid field will be set to the pid
 * of the process performing the locking operation.
 *
 * Note: if this method is not implemented, the kernel will still
 * allow file locking to work locally.  Hence it is only
 * interesting for network filesystems and similar.
 *
 * Introduced in version 2.6
 */
int beholdfs_lock(const char *path, struct fuse_file_info *fi, int cmd,
	     struct flock *lock)
{
	syslog(LOG_DEBUG, "beholdfs_lock(path=%s)", path);
	return 0;
}

/**
 * Change the access and modification times of a file with
 * nanosecond resolution
 *
 * Introduced in version 2.6
 */
int beholdfs_utimens(const char *path, const struct timespec times[2])
{
	int ret = -ENOENT;
	beholddb_path *bpath;

	syslog(LOG_DEBUG, "beholdfs_utimens(path=%s)", path);
	if (!(beholdfs_get_file(path, &bpath)))
	{
		if ((ret = utimensat(AT_FDCWD, bpath->realpath, times, AT_SYMLINK_NOFOLLOW)))
			ret = -errno;
	}
	syslog(LOG_DEBUG, "beholdfs_utimens: ret=%d", ret);
	beholddb_free_path(bpath);
	return ret;
}

/**
 * Map block index within file to block index within device
 *
 * Note: This makes sense only for block device backed filesystems
 * mounted with the 'blkdev' option
 *
 * Introduced in version 2.6
 */
int beholdfs_bmap(const char *path, size_t blocksize, uint64_t *idx)
{
	syslog(LOG_DEBUG, "beholdfs_bmap(path=%s)", path);
	return 0;
}

/**
 * Ioctl
 *
 * flags will have FUSE_IOCTL_COMPAT set for 32bit ioctls in
 * 64bit environment.  The size and direction of data is
 * determined by _IOC_*() decoding of cmd.  For _IOC_NONE,
 * data will be NULL, for _IOC_WRITE data is out area, for
 * _IOC_READ in area and if both are set in/out area.  In all
 * non-NULL cases, the area is of _IOC_SIZE(cmd) bytes.
 *
 * Introduced in version 2.8
 */
int beholdfs_ioctl(const char *path, int cmd, void *arg,
	      struct fuse_file_info *fi, unsigned int flags, void *data)
{
}

/**
 * Poll for IO readiness events
 *
 * Note: If ph is non-NULL, the client should notify
 * when IO readiness events occur by calling
 * fuse_notify_poll() with the specified ph.
 *
 * Regardless of the number of times poll with a non-NULL ph
 * is received, single notification is enough to clear all.
 * Notifying more times incurs overhead but doesn't harm
 * correctness.
 *
 * The callee is responsible for destroying ph with
 * fuse_pollhandle_destroy() when no longer in use.
 *
 * Introduced in version 2.8
 */
int beholdfs_poll(const char *path, struct fuse_file_info *fi,
	     struct fuse_pollhandle *ph, unsigned *reventsp)
{
}

struct fuse_operations beholdfs_operations =
{
	.getattr =	beholdfs_getattr,
	.readlink =	beholdfs_readlink,
	.mknod =	beholdfs_mknod,
	.mkdir =	beholdfs_mkdir,
	.unlink =	beholdfs_unlink,
	.rmdir =	beholdfs_rmdir,
	.symlink =	beholdfs_symlink,
	.rename =	beholdfs_rename,
	.link =		beholdfs_link,
	.chmod =	beholdfs_chmod,
	.chown =	beholdfs_chown,
	.truncate =	beholdfs_truncate,
	.open =		beholdfs_open,
	.read =		beholdfs_read,
	.write =	beholdfs_write,
	.statfs =	beholdfs_statfs,
	.flush =	beholdfs_flush,
	.release =	beholdfs_release,
	.fsync =	beholdfs_fsync,
	.setxattr =	beholdfs_setxattr,
	.getxattr =	beholdfs_getxattr,
	.listxattr =	beholdfs_listxattr,
	.removexattr =	beholdfs_removexattr,
	.opendir =	beholdfs_opendir,
	.readdir =	beholdfs_readdir,
	.releasedir =	beholdfs_releasedir,
	.fsyncdir =	beholdfs_fsyncdir,
	.init =		beholdfs_init,
	.destroy =	beholdfs_destroy,
	.access = 	beholdfs_access,
	.create =	beholdfs_create,
	.ftruncate =	beholdfs_ftruncate,
	.fgetattr =	beholdfs_fgetattr,
	//.lock =		beholdfs_lock,
	.utimens =	beholdfs_utimens,
	//.bmap =		beholdfs_bmap,
	//.ioctl =	beholdfs_ioctl,
	//.poll =		beholdfs_poll,
	.flag_nullpath_ok = 1,
};

static struct fuse_opt beholdfs_opts[] =
{
	BEHOLDFS_OPT("debug=%i",	loglevel,	0),
	BEHOLDFS_OPT("char=%c",		tagchar,	0),
	BEHOLDFS_OPT("list",		tagshow,	1),
	BEHOLDFS_OPT("nolist",		tagshow,	0),
	BEHOLDFS_OPT("new_locate",	new_locate,	1),
	//FUSE_OPT("--help",		BEHOLDFS_KEY_HELP),
	//FUSE_OPT("-h",		BEHOLDFS_KEY_HELP),
	//FUSE_OPT("--version",		BEHOLDFS_KEY_VERSION),
	//FUSE_OPT("-V",		BEHOLDFS_KEY_VERSION),
	FUSE_OPT_END
};

static int beholdfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
	beholdfs_config *config = (beholdfs_config*)data;

	switch (key)
	{
	case FUSE_OPT_KEY_NONOPT:
		if (config->rootdir)
			break;
		config->rootdir = arg;
		return 0;
	}
	return 1;
}

int main(int argc, char **argv)
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	beholdfs_config config;

	memset(&config, 0, sizeof(config));
	config.tagchar = BEHOLDFS_TAG_CHAR;
	config.tagshow = BEHOLDFS_TAG_SHOW;
	fuse_opt_parse(&args, &config, beholdfs_opts, beholdfs_opt_proc);

	if (!config.rootdir)
	{
		fprintf(stderr, "Usage: beholdfs -o[options] <fsroot> <mountpoint>\n");
		exit(1);
	}

	int rootdir;

	if (-1 == (rootdir = open(config.rootdir, O_RDONLY)))
	{
		perror("Cannot mount specified directory");
		exit(2);
	}

	setlogmask(LOG_UPTO(config.loglevel <= LOG_DEBUG ? config.loglevel : LOG_DEBUG));

	beholdfs_state *state = (beholdfs_state*)malloc(sizeof(beholdfs_state));

	state->rootdir = rootdir;
	state->tagchar = config.tagchar;
	state->tagshow = config.tagshow;
	state->new_locate = config.new_locate;

	int ret = fuse_main(args.argc, args.argv, &beholdfs_operations, state);

	fuse_opt_free_args(&args);
	return ret;
}

