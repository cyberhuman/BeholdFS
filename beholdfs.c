#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include <fuse/fuse.h>

#include "beholdfs.h"
#include "beholddb.h"

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.	 The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int beholdfs_getattr(const char *path, struct stat *stat)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mymypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = lstat(mypath, stat)))
			ret = -errno;
		free(mypath);
	}
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
	char *mypath;
	if ((mymypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = readlink(mypath, buf, bufsiz)))
			ret = -errno;
		free(mypath);
	}
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
	char *mypath;
	if ((mypath = beholddb_getpath(path)))
	{
		if (-1 == (ret = mknod(mypath, mode, dev)))
			ret = -errno;
		free(mypath);
	}
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
	char *mypath;
	if ((mypath = beholddb_getpath(path)))
	{
		if (-1 == (ret = mkdir(mypath, mode)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Remove a file */
int beholdfs_unlink(const char *path)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = unlink(mypath)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Remove a directory */
int beholdfs_rmdir(const char *path)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = rmdir(mypath)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Create a symbolic link */
int beholdfs_symlink(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	char *myoldpath = beholddb_getfile(oldpath);
	char *mynewpath = beholddb_getpath(newpath);
	if (myoldpath && mynewpath)
	{
		if (-1 == (ret = symlink(myoldpath, mynewpath)))
			ret = -errno;
	}
	if (myoldpath)
		free(myoldpath);
	if (mynewpath)
		free(mynewpath);
	return ret;
}

/** Rename a file */
int beholdfs_rename(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	char *myoldpath = beholddb_getfile(oldpath);
	char *mynewpath = beholddb_getpath(newpath);
	if (myoldpath && mynewpath)
	{
		if (-1 == (ret = rename(myoldpath, mynewpath)))
			ret = -errno;
		if (!ret)
			beholddb_rename(myoldpath, mynewpath);
	}
	if (myoldpath)
		free(myoldpath);
	if (mynewpath)
		free(mynewpath);
	return ret;
}

/** Create a hard link to a file */
int beholdfs_link(const char *oldpath, const char *newpath)
{
	int ret = -ENOENT;
	char *myoldpath = beholddb_getfile(oldpath);
	char *mynewpath = beholddb_getpath(newpath);
	if (myoldpath && mynewpath)
	{
		if (-1 == (ret = link(myoldpath, mynewpath)))
			ret = -errno;
	}
	if (myoldpath)
		free(myoldpath);
	if (mynewpath)
		free(mynewpath);
	return ret;
}

/** Change the permission bits of a file */
int beholdfs_chmod(const char *path, mode_t mode)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = chmod(mypath, mode)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Change the owner and group of a file */
int beholdfs_chown(const char *path, uid_t owner, gid_t group)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = chown(mypath, owner, group)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Change the size of a file */
int beholdfs_truncate(const char *path, off_t length)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = truncate(mypath, length)))
			ret = -errno;
		free(mypath);
	}
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
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		ret = 0;
		if (-1 == (fi->fh = open(mypath, fi->flags)))
			ret = -errno;
		free(mypath);
	}
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
	if (-1 == (ret = pread(fi->fh, buf, count, offset)))
		ret = -errno;
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
	if (-1 == (ret = pwrite(fi->fh, buf, count, offset)))
		ret = -errno;
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
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = statvfs(mypath, statv)))
			ret = -errno;
		free(mypath);
	}
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
	if (-1 == (ret = close(fi->fh)))
		ret = -errno;
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
	if (-1 == (ret = datasync ? fdatasync(fi->fh) : fsync(fi->fh)))
		ret = -errno;
	return ret;
}

/** Set extended attributes */
int beholdfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = lsetxattr(mypath, name, value, size, flags)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Get extended attributes */
int beholdfs_getxattr(const char *path, const char *name, char *value, size_t size)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = lgetxattr(mypath, name, value, size)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** List extended attributes */
int beholdfs_listxattr(const char *path, char *list, size_t size)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = llistxattr(mypath, list, size)))
			ret = -errno;
		free(mypath);
	}
	return ret;
}

/** Remove extended attributes */
int beholdfs_removexattr(const char *path, const char *name)
{
	int ret = -ENOENT;
	char *mypath;
	if ((mypath = beholddb_getfile(path)))
	{
		if (-1 == (ret = lremovexattr(mypath, size)))
			ret = -errno;
		free(mypath);
	}
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
int beholdfs_readdir(const char *, void *, fuse_fill_dir_t, off_t,
		struct fuse_file_info *)
{
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int beholdfs_releasedir(const char *, struct fuse_file_info *)
{
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
int beholdfs_fsyncdir(const char *, int, struct fuse_file_info *)
{
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
	struct beholdfs_state *state = BEHOLDFS_STATE;
	fchdir(state->rootdir);
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
	struct beholdfs_state *state = (struct beholdfs_state*)private_data;
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
int beholdfs_access(const char *path, int)
{
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
int beholdfs_ftruncate(const char *path, off_t, struct fuse_file_info *)
{
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
int beholdfs_fgetattr(const char *, struct stat *, struct fuse_file_info *)
{
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
int beholdfs_lock(const char *, struct fuse_file_info *, int cmd,
	     struct flock *)
{
}

/**
 * Change the access and modification times of a file with
 * nanosecond resolution
 *
 * Introduced in version 2.6
 */
int beholdfs_utimens(const char *, const struct timespec tv[2])
{
}

/**
 * Map block index within file to block index within device
 *
 * Note: This makes sense only for block device backed filesystems
 * mounted with the 'blkdev' option
 *
 * Introduced in version 2.6
 */
int beholdfs_bmap(const char *, size_t blocksize, uint64_t *idx)
{
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
int beholdfs_ioctl(const char *, int cmd, void *arg,
	      struct fuse_file_info *, unsigned int flags, void *data)
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
int beholdfs_poll(const char *, struct fuse_file_info *,
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
	.lock =		beholdfs_lock,
	.utimens =	beholdfs_utimens,
	.bmap =		beholdfs_bmap,
	.ioctl =	beholdfs_ioctl,
	.poll =		beholdfs_poll,
	.flag_nullpath_ok = true,
};

int main(int argc, char **argv)
{
	int opt;
	while (-1 != (opt = getopt(argc, argv, "")))
	{
		switch (opt)
		{
		default:;
		}
	}
	if (optind != argc - 2)
	{
		fprintf(stderr, "Usage: beholdfs -o[options] <fsroot> <mountpoint>\n");
		exit(1);
	}

	int rootdir;
	if (-1 == (rootdir = open(argv[optind], O_RDONLY)))
	{
		perror("Cannot mount specified directory");
		exit(2);
	}
	struct beholdfs_state *state = (struct beholdfs_state*)malloc(sizeof struct beholdfs_state);
	state->rootdir = rootdir;

	optind = 1;
	return fuse_main(argc, argv, &beholdfs_operations, state);
}

