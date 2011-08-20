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

