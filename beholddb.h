#ifndef __BEHOLDDB_H__
#define __BEHOLDDB_H__

int beholddb_parse_path(const char *path, char **realpath, char ***tags, int invert);
int beholddb_get_file(const char *path, char **realpath, char ***tags);
int beholddb_free_path(const char *realpath, const char **tags);
int beholddb_mark(const char *realpath, const char **files_tags, const char **dirs_tags);


/*
struct beholddb_path
{
	char *path;
	char **tags_include;
	char **tags_exclude;
};
*/
#endif // __BEHOLDDB_H__

