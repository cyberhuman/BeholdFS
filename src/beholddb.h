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

#ifndef __BEHOLDDB_H__
#define __BEHOLDDB_H__

#include <sqlite3.h>
//#define BEHOLDDB_PARSE_INVERT 1

#define BEHOLDDB_OK         0
#define BEHOLDDB_DONE       1
#define BEHOLDDB_HIDDEN     2
#define BEHOLDDB_ERROR     -1
#define BEHOLDDB_NOT_FOUND -2
#define BEHOLDDB_EXISTS    -3
//#define BEHOLDDB_FILTER  -100

typedef struct beholddb_tag_list_item
{
	const char *name;
	struct beholddb_tag_list_item *next;
} beholddb_tag_list_item;

typedef struct beholddb_tag_list
{
	beholddb_tag_list_item *head;
} beholddb_tag_list;

typedef struct beholddb_tag_list_set
{
	beholddb_tag_list include;
	beholddb_tag_list exclude;
} beholddb_tag_list_set;

typedef struct beholddb_path
{
	const char *realpath;
	const char *basename;

	beholddb_tag_list path;
	union
	{
		beholddb_tag_list_set;
		beholddb_tag_list_set tags;
	};

	int listing: 1;
} beholddb_path;

int beholddb_parse_path(const char *path, beholddb_path **pbpath);
int beholddb_get_file(const char *path, beholddb_path **pbpath);
int beholddb_locate_file(const beholddb_path *bpath);
int beholddb_free_path(beholddb_path *bpath);

int beholddb_create_file(const beholddb_path *bpath, int type);
int beholddb_delete_file(const beholddb_path *bpath);
int beholddb_rename_file(const beholddb_path *oldbpath, const beholddb_path *newbpath);
int beholddb_opendir(const beholddb_path *bpath, void **handle);
int beholddb_readdir(void *handle, const char *name);
int beholddb_listdir(void *handle, const char **pname);
int beholddb_closedir(void *handle);

int beholddb_exec(sqlite3 *db, const char *sql);

#endif // __BEHOLDDB_H__

