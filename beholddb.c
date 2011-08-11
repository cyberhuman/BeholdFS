#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "beholddb.h"
#include "fs.h"

int beholddb_parse_path(const char *path, char **realpath, char ***tags, int invert)
{
	int pathlen = strlen(path);
	char *pathptr = *realpath = malloc(pathlen + 3);
	char *tagsbuf = malloc(pathlen + 1), *tagsptr = tagsbuf;
	size_t tagcount[2] = { 0, 0 }; // include, exclude

	*pathptr++ = '.';
	while (*path)
	{
		if ('/' != *path++)
		{
			free(*realpath);
			free(tagsbuf);
			return -1;
		}
		switch (*path)
		{
		case '#':
			do
			{
				// skip hash sign
				++path;
				// handle hash directory
				while (*path == '/')
					++path;
				// handle empty tag
				if (!*path)
					break;
				// handle tag type
				++tagcount[*path == '-' ^ !!invert];
				// handle glued tags
				while (*path && '/' != *path && '#' != *path)
					*tagsptr++ = *path++;
				*tagsptr++ = 0;
			} while ('#' == *path);
		case '/':
			// remove redundant slashes
		case 0:
			// remove trailing slash
			break;
		default:
			*pathptr++ = '/';
			while (*path && '/' != *path)
				*pathptr++ = *path++;
		}
	}

	// null terminate path
	*pathptr++ = 0;
	// copy tags to path buffer
	memcpy(pathptr, tagsbuf, tagsptr - tagsbuf);
	free(tagsbuf);

	// allocate space for tags
	*tags = (char**)calloc(tagcount[0] + tagcount[1] + 2, sizeof char*);
	// negative tags go after positive tags
	tagcount[1] = tagcount[0];
	// positive tags go first
	tagcount[0] = 0;
	for (; pathptr != tagsptr; pathptr += strlen(pathptr) + 1)
	{
		int minus;
		// skip minus sign
		if ((minus = '-' == *pathptr))
			++pathptr;
		// store tag
		(*tags)[tagcount[minus ^ !!invert]++] = pathptr;
	}
	// terminate tags lists
	(*tags)[tagcount[0]] = NULL;
	(*tags)[tagcount[1]] = NULL;

	return 0;
}

static const char BEHOLDDB_NAME[] = ".beholdfs";

static char *beholddb_get_name(const char *realpath)
{
	char *ptr = strrchr(realpath, '/');
	if (!ptr)
		return NULL;
	char *dbpath = (char*)malloc(ptr - realpath + 1 + sizeof(BEHOLDDB_NAME));
	memcpy(dbpath, realpath, ptr - realpath + 1);
	memcpy(dbpath + ptr - realpath + 1, BEHOLDDB_NAME, sizeof(BEHOLDDB_NAME));
	return dbpath;
}

static int beholddb_exec(sqlite3 *db, const char *sql)
{
	char *err;
	int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
	// log("%s\n", err);
	sqlite3_free(err);
	return rc;
}

static int beholddb_init(sqlite3 *db)//, const char *path)
{
	//fs_create_module(db, path);
	sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, 1, NULL);
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_open_read(const char *realpath, sqlite3 **db)
{
	if (!*realpath)
		return 0;
	char *db_name = beholddb_get_name(realpath);
	// if in root directory, assume success
	if (!db_name)
		return 0;

	// open the database
	int rc = sqlite3_open_v2(db_name, &db, SQLITE_OPEN_READWRITE, NULL);
	free(db_name);
	if (rc || (rc = beholddb_init(db)))
	{
		sqlite3_close(db);
		*db = NULL;
	}
	return rc;
}

static int beholddb_open_write(const char *realpath, sqlite3 **db)
{
	if (!*realpath)
		return 0;
	char *db_name = beholddb_get_name(realpath);
	// if in root directory, assume success
	if (!db_name)
		return 0;

	// open the database
	int rc = sqlite3_open_v2(db_name, &db, SQLITE_OPEN_READWRITE, NULL);
	free(db_name);
	if (rc && SQLITE_CANTOPEN == rc)
	{
		sqlite3_close(db);
		(rc = sqlite3_open_v2(db_name, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) ||
		(rc = beholddb_create_tables(db));
	}
	if (rc || (rc = beholddb_init(db)))
	{
		sqlite3_close(db);
		*db = NULL;
	}
	return rc;
}

static int beholddb_begin_transaction(sqlite3 *db)
{
	return beholddb_exec(db, "begin transaction;");
}

static int beholddb_commit(sqlite3 *db)
{
	return beholddb_exec(db, "commit;");
}

static int beholddb_rollback(sqlite3 *db)
{
	return beholddb_exec(db, "rollback;");
}

static int beholddb_create_tables(sqlite3 *db)
{
	return beholddb_exec(db,
		"create table files"
		"("
			"id integer primary key,"
			"type integer not null,"
			"name text unique on conflict ignore"
		");"
		"create table tags"
		"("
			"id integer primary key,"
			"name text unique on conflict ignore"
		");"
		"create table files_tags"
		"("
			//"id integer primary key,"
			"id_file integer not null references files(id) on delete cascade,"
			"id_tag integer not null references tags(id),"// on delete cascade,"
			"unique ( id_file, id_tag ) on conflict ignore"
		");"
		"create table dirs_tags"
		"("
			//"id integer primary key,"
			"id_file integer not null references files(id) on delete cascade,"
			"id_tag integer not null references tags(id),"// on delete cascade,"
			"unique ( id_file, id_tag ) on conflict ignore"
		");"
		"create view strong_tags as "
			"select dt.*, 1 type from dirs_tags dt "
			"union "
			"select ft.*, 0 type from files_tags ft "
			"join files f on f.id = ft.id_file "
			"where not f.type;"
//		"create virtual table filesystem "
//		"using sqlitefs;"
//		"select f.id, fs.name, fs.type from filesystem fs "
//		"left join files f on f.name = fs.name;"
		);
}

static int beholddb_exec_bind_text(sqlite3 *db, const char *sql, const char *text)
{
	sqlite3_stmt *stmt;
	int rc;
	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, text, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));
	sqlite3_finalize(stmt);
	return rc;
}

static int behloddb_exec_select_text(sqlite3 *db, const char *sql, char *buf, char **rows, size_t *count, size_t *size)
{
	sqlite3_stmt *stmt;
	int rc;
	*count = 0;
	*size = 0;
	if (!(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)))
	{
		while (SQLITE_ROW == (rc = sqlite3_step(stmt)))
		{
			const char *text = sqlite3_column_text(stmt, 0);
			size_t len = strlen(text) + 1;
			memcpy(buf, text, len);
			*rows++ = buf;
			buf += len;
			++*count;
			*size += len;
		}
		if (SQLITE_DONE != rc)
			*count = 0;
	}
	sqlite3_finalize(stmt);
	return rc;
}

static int beholddb_set_tags_worker(sqlite3 *db, const char *sql_create,
	const char *sql_include, const char *sql_exclude, const char **tags)
{
	if (!tags)
		return 0;
	beholddb_begin_transaction(db);
	beholddb_exec(db, sql_create);
	for (; *tags; ++tags)
		beholddb_exec_bind_text(db, sql_include, *tags);
	for (++tags; *tags; ++tags)
		beholddb_exec_bind_text(db, sql_exclude, *tags);
	beholddb_commit(db);
	return 0; // TODO: handle errors
}

static const char *beholddb_ddl_files_tags =
	"create temporary table include"
	"("
		"id_tag integer unique on conflict ignore,"
		"name text"
	");"
	"create temporary table exclude"
	"("
		"id_tag integer unique on conflict ignore,"
		"name text"
	");";

static const char *beholddb_ddl_dirs_tags =
	"create temporary table dirs_include"
	"("
		"id_tag integer unique on conflict ignore,"
		"name text"
	");"
	"create temporary table dirs_exclude"
	"("
		"id_tag integer unique on conflict ignore,"
		"name text"
	");";

static int beholddb_set_tags(sqlite3 *db, const char **files_tags, const char **dirs_tags)
{
	beholddb_begin_transaction(db);
	beholddb_set_tags_worker(db,
		beholddb_ddl_files_tags,
		"insert into include "
		"select id, name from tags "
		"where name = ?",
		"insert into exclude "
		"select id, name from tags "
		"where name = ?",
		files_tags);
	beholddb_set_tags_worker(db,
		beholddb_ddl_dirs_tags,
		"insert into dirs_include "
		"select id, name from tags "
		"where name = ?",
		"insert into dirs_exclude "
		"select id, name from tags "
		"where name = ?",
		dirs_tags);
	beholddb_commit(db);
}

// calculates number of entries in the list
// returns pointer to the next list
static char **list_stat(const char **list, size_t *count, size_t *len)
{
	*count = 0;
	for (; *list; ++list)
	{
		++*count;
		if (len)
			*len += strlen(*list) + 1;
	}
	return ++list;
}

// calculates numbers of entries in multiple lists
static void lists_stat(const char **list, size_t **counts, size_t **lens, size_t size)
{
	const char **init = list;
	for (int i = 0; i < size; ++i)
		list = list_stat(list, counts[i], lens ? lens[i] : NULL);
}

// returns numbers of entries in tags string
static void tags_stat(const char **tags, size_t *count_p, size_t *count_m, size_t *len_p, size_t *len_m)
{
	size_t **counts[2] = { count_p, count_m };
	size_t **lens[2] = { len_p, len_m };
	lists_stat(tags, counts, lens, 2);
}

static int beholddb_locate_file(const char *realpath, const char **tags)
{
	char *file = strrchr(realpath, '/');
	if (!file++)
		return 0;

	size_t tagcount_p, tagcount_m;
	tags_stat(tags, &tagcount_p, &tagcount_m, NULL, NULL);
	// if no tags are defined, assume path exists
	if (!tagcount_p && !tagcount_m)
		return 0;

	sqlite3 *db;
	int rc = beholddb_open_read(realpath, &db);
	if (rc)
		// if inclusive tags are not defined, assume no error
		return tagcount_p ? -1 : 0;

	sqlite3_stmt *stmt;
	const char *sql =
		"select "
			"(select count(*) from ( "
				"select t.id_tag from include t "
				"intersect "
				"select ft.id_tag from files f "
				"join files_tags ft on ft.id_file = f.id "
				"where f.name = ?1 "
			")), "
			"(select count(*) from ( "
				"select t.id_tag from exclude t "
				"intersect "
				"select st.id_tag from files f "
				"join strong_tags st on st.id_file = f.id "
				"where f.name = ?1 "
			"))";

	(rc = beholddb_set_tags(db, tags, NULL)) || 
	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, file, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));
	if (rc)
	{
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return -1;
	}

	int count_include = sqlite3_column_int(stmt, 0);
	int count_exclude = sqlite3_column_int(stmt, 1);

	sqlite3_finalize(stmt);
	sqlite3_close(db);

	// all inclusive tags must be present
	// and no exclusive tags must be present
	return count_include == tagcount_p && !count_exclude ? 0 : 1;
}

// find file by mixed path
// return error if not found
int beholddb_get_file(const char *path, char **realpath, char ***tags)
{
	int status = beholddb_parse_path(path, realpath, tags);
	if (!status)
	{
		status = beholddb_locate_file(*realpath, *tags);
	}
	return status;
}

// free memory for path and tags
int beholddb_free_path(const char *realpath, const char **tags)
{
	free(realpath);
	free(tags);
	return 0;
}

static int beholddb_mark_recursive(const char *realpath, const char *file, const char **files_tags)
{
	size_t tagcount_p, tagcount_m, tagsize_p, tagsize_m;
	size_t pathlen, count, size;
	char *path, *pathbuf;
	char **rows, **pfiles_tags, **pdirs_tags;

	tags_stat(files_tags, &tagcount_p, &tagcount_m, &tagsize_p, &tagsize_m);
	pathlen = file - realpath - 1;
	pathbuf = path = (char*)malloc(pathlen + 1 + tagsize_p + tagsize_m);
	rows = (char**)calloc(2 * (tagcount_p + tagcount_m + 2), sizeof(char*));

	memcpy(path, realpath, pathlen);
	pathbuf += pathlen;
	*pathbuf++ = 0;

	// include => at least one file has this tag
	// exclude => at least one file has no tag

	//	include becomes include for parent's files_tags
	//	exclude becomes exclude for parent's files_tags (filtered by 'no files have tag')
	pfiles_tags = rows;
	memcpy(pfiles_tags, files_tags, (tagcount_p + 1) * sizeof(char*));
	beholddb_exec_select_text(db,
		"select t.name from exclude t "
		"where not exists "
			"(select * from files_tags ft "
			"where ft.id_tag = t.id_tag)",
		pathbuf,
		pfiles_tags + tagcount_p + 1,
		&count,
		&size);
	pfiles_tags[count++] = NULL;

	//	include becomes include for parent's dirs_tags (filtered by 'all files have tag, all dirs have dirs_tag')
	//	exclude becomes exclude for parent's dirs_tags
	pathbuf += size;
	pdirs_tags = pfiles_tags + count;
	beholddb_exec_select_text(db,
		"select t.name from include t "
		"where not exists "
			"(select f.id, t.id_tag from files f "
			"except "
			"select st.id_file, st.id_tag from strong_tags st)";
		pathbuf,
		pdirs_tags,
		&count,
		&size);
	pdirs_tags[count++] = NULL;
	memcpy(pdirs_tags + count, files_tags + tagcount_p + 1, (tagcount_m + 1) * sizeof(char*));

	static int beholddb_mark_object(const char *realpath, const char **files_tags, const char **dirs_tags);
	beholddb_mark_object(path, pfiles_tags, pdirs_tags);
	beholddb_free_path(path, rows);
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_mark_worker(sqlite3 *db, const char *file, int *pchanges)
{
	int changes = 0;

	beholddb_exec_bind_text(db,
		"insert into files_tags ( id_file, id_tag ) "
		"select f.id, t.id "
		"from files f "
		"join include t "
		"where f.name = ?",
		file);
	changes += sqlite3_changes(db);

	beholddb_exec_bind_text(db,
		"delete from files_tags ft "
		"where ft.id_file = "
			"( select f.id from files f where f.name = ? ) "
		"and ft.id_tag in "
			"( select t.id_tag from exclude t )",
		file);
	changes += sqlite3_changes(db);

	//if (dirs_tags)
	{
		beholddb_exec_bind_text(db,
			"insert into dirs_tags ( id_file, id_tag ) "
			"select f.id, t.id "
			"from files f "
			"join dirs_include t "
			"where f.name = ?",
			file);
		changes += sqlite3_changes(db);

		beholddb_exec_bind_text(db,
			"delete from dirs_tags ft "
			"where ft.id_file = "
				"( select f.id from files f where f.name = ? ) "
			"and ft.id_tag in "
				"( select t.id_tag from dirs_exclude t )",
			file);
		changes += sqlite3_changes(db);
	}
	if (pchanges)
		*pchanges = changes;
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_mark(sqlite3 *db, const char *realpath, const char **files_tags, const char **dirs_tags)
{
	const char *file = strrchr(realpath, '/');
	if (!file++)
		return 0;

	int changes;

	beholddb_begin_transaction(db);
	beholddb_set_tags(db, files_tags, dirs_tags);
	beholddb_mark_worker(db, file, &changes);
	beholddb_commit(db);
	if (!changes)
		return 0;

	return beholddb_mark_recursive(realpath, file, files_tags);
}

static int beholddb_mark_object(const char *realpath, const char **files_tags, const char **dirs_tags)
{
	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
		return rc;

	rc = beholddb_mark(db, realpath, files_tags, dirs_tags);

	sqlite3_close(db);
	return rc;
}

int beholddb_mark_file(const char *realpath, const char **tags)
{
	return beholddb_mark_object(realpath, tags, NULL);
}

static int beholddb_get_tags_worker(sqlite3 *db, const char *sql_count, const char *sql_select, char ***tags)
{
	if (!tags)
		return SQLITE_OK;

	(rc = sqlite3_prepare_v2(db, sql_count, -1, &stmt, NULL)) ||
	(rc = sqlite3_step(stmt));
	if (!rc)
	{
		count_p = sqlite3_column_int(stmt, 0);
		count_m = sqlite3_column_int(stmt, 1);
	}
	sqlite3_finalize(stmt);
	if (rc)
		return rc;

	tagsptr = *tags = (char**)calloc(count_p + count_m + 2, sizeof(char*));
	if (!(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)))
	{
		while (SQLITE_ROW == (rc = sqlite3_step(stmt)))
		{
			char *text;
			int textlen = sqlite3_column_bytes(stmt, 0);
			if (textlen++)
			{
				text = (char*)malloc(textlen);
				memcpy(text, sqlite3_column_text(stmt, 0), textlen);
			} else
				text = NULL;
			*tagsptr++ = text;
		}
	}
	sqlite3_finalize(stmt);
	return rc;
}

static int beholddb_get_tags(sqlite3 *db, char ***files_tags, char ***dirs_tags)
{
	sqlite3_stmt *stmt;
	const char *sql;
	char **tags, **tagsptr;
	int rc, count;

	beholddb_begin_transaction(db);
	beholddb_get_tags_worker(db,
		"select "
			"(select count(*) from include), "
			"(select count(*) from exclude)",
		"select name from include "
		"union all "
		"select NULL "
		"union all "
		"select name from exclude "
		"union all "
		"select NULL ",
		files_tags);
	beholddb_get_tags_worker(db,
		"select "
			"(select count(*) from dirs_include) + "
			" select count(*) from dirs_exclude)",
		"select name from dirs_include "
		"union all "
		"select NULL "
		"union all "
		"select name from dirs_exclude "
		"union all "
		"select NULL ",
		dirs_tags);
	beholddb_commit(db);
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_free_tags(char **tags)
{
	if (!tags)
		return;
	for (; *tags; ++tags)
		free(*tags);
	free(tags);
}

static const char *no_tags[] = { NULL, NULL };

int beholddb_create_file(const char *realpath, const char **tags, int type)
{
	char *file = strrchr(realpath, '/');
	if (!file++)
		return 0;

	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
		return rc;

	beholddb_begin_transaction(db);

	char *sql = sqlite3_mprintf(
		"insert into files ( type, name ) "
		"values ( %d, ? )", !!type);
	rc = beholddb_exec_bind_text(db, sql, file);
	sqlite3_free(sql);

	if (type)
		tags = no_tags;
	beholddb_set_tags(db, tags, NULL);
	beholddb_exec(db,
		"insert into exclude "
		"select id, name from tags "
		"except "
		"select id_tag, name "
		"from include;");
	beholddb_mark_worker(db, file, &changes);

	char **files_tags;
	beholddb_get_tags(db, &files_tags, NULL);
	beholddb_mark_recursive(realpath, file, files_tags);
	beholddb_free_tags(files_tags);

	beholddb_commit(db);
	sqlite3_close(db);
	return rc; // TODO: error handling
}

int beholddb_delete_file(const char *realpath)
{
	char *file = strrchr(realpath, '/');
	if (!file++)
		return 0;

	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
		return rc;

	beholddb_begin_transaction(db);
	beholddb_set_tags(db, tags, NULL);
	beholddb_exec_bind_text(db,
		"delete from files "
		"where name = ?;",
		file);
	beholddb_exec(db,
		"insert into exclude "
		"select t.id, t.name from tags t "
		"where not exists "
			"(select * from files_tags ft where ft.id_tag = t.id);");
	beholddb_exec(db,
		"delete from tags t "
		"where t.id in "
			"(select * from exclude);");
	beholddb_exec(db,
		"insert into include "
		"select id, name from tags "
		"except "
		"select id_tag, name "
		"from exclude;");

	char **files_tags;
	beholddb_get_tags(db, &files_tags, NULL);
	beholddb_mark_recursive(realpath, file, files_tags);
	beholddb_free_tags(files_tags);

	beholddb_commit(db);
	sqlite3_close(db);
	return rc; // TODO: error handling
}

