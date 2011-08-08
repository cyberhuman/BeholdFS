#ifndef __BEHOLDFS_H__
#define __BEHOLDFS_H__

//#include <fuse/fuse.h>

struct beholdfs_state
{
	int rootdir;
};

#define BEHOLDFS_STATE ((struct beholdfs_state*)fuse_get_context()->private_data)

#endif // __BEHOLDFS_H__

