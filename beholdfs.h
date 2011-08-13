#ifndef __BEHOLDFS_H__
#define __BEHOLDFS_H__

//#include <fuse/fuse.h>

struct beholdfs_state
{
	int rootdir;
};

struct beholdfs_dir
{
	DIR *dir;
	const char *path;
	const char *const *tags;
};

#define BEHOLDFS_STATE ((struct beholdfs_state*)fuse_get_context()->private_data)

#endif // __BEHOLDFS_H__

