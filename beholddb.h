#ifndef __BEHOLDDB_H__
#define __BEHOLDDB_H__

#include <sqlite3.h>
//#define BEHOLDDB_PARSE_INVERT 1

#define BEHOLDDB_OK		0
#define BEHOLDDB_ERROR		-1
#define BEHOLDDB_FILTER		-100

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

#endif // __BEHOLDDB_H__

