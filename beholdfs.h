#ifndef __BEHOLDFS_H__
#define __BEHOLDFS_H__

//#include <fuse/fuse.h>

typedef struct beholdfs_config
{
	const char *rootdir;
	int loglevel;
	char tagchar;
} beholdfs_config;

typedef struct beholdfs_state
{
	int rootdir;
	char tagchar;
} beholdfs_state;

typedef struct beholdfs_dir
{
	int stage;
	DIR *dir;
	void *handle;
	struct dirent *entry;
	struct dirent *result;
	const char *dbresult;
} beholdfs_dir;

#define BEHOLDFS_STATE ((beholdfs_state*)fuse_get_context()->private_data)
#define BEHOLDFS_OPT(t, p, v) { t, offsetof(beholdfs_config, p), v }

#define BEHOLDFS_TAG_CHAR	'%'

#endif // __BEHOLDFS_H__

