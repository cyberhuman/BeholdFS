#ifndef __BEHOLDFS_H__
#define __BEHOLDFS_H__

//#include <fuse/fuse.h>

struct beholdfs_state
{
	int rootdir;
	char tagchar;
};

struct beholdfs_dir
{
	DIR *dir;
	const char *path;
	int pathlen;
	const char *const *tags;
	struct dirent *entry;
};

#define BEHOLDFS_STATE ((struct beholdfs_state*)fuse_get_context()->private_data)

#endif // __BEHOLDFS_H__

