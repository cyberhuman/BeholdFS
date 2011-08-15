#ifndef __BEHOLDDB_H__
#define __BEHOLDDB_H__

int beholddb_parse_path(const char *path, const char **realpath, const char *const **tags, int invert);
int beholddb_locate_file(const char *realpath, const char *const *tags);
int beholddb_get_file(const char *path, const char **realpath, const char *const **tags);
int beholddb_free_path(const char *realpath, const char *const *tags);
int beholddb_mark_file(const char *realpath, const char *const *tags);
int beholddb_create_file(const char *realpath, const char *const *tags, int type);
int beholddb_delete_file(const char *realpath);
int beholddb_rename_file(const char *oldrealpath, const char *newrealpath, const char *const *tags);
void beholddb_free_tags(const char *const *tags);


/*
struct beholddb_path
{
	char *path;
	char **tags_include;
	char **tags_exclude;
};
*/
#endif // __BEHOLDDB_H__

