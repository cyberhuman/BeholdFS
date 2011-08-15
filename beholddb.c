#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <syslog.h>

#include "beholddb.h"
#include "fs.h"

char beholddb_tagchar;

int beholddb_parse_path(const char *path, const char **realpath, const char *const **tags, int invert)
{
	syslog(LOG_DEBUG, "beholddb_parse_path(path=%s)", path);

	int pathlen = strlen(path);
	char *pathbuf = (char*)malloc(pathlen + 3), *pathptr = pathbuf;
	char *tagsbuf = (char*)malloc(pathlen + 1), *tagsptr = tagsbuf;
	size_t tagcount[2] = { 0, 0 }; // include, exclude

	*pathptr++ = '.';
	while (*path)
	{
		if ('/' != *path++)
		{
			syslog(LOG_DEBUG, "beholddb_parse_path: path is relative");
			free(pathbuf);
			free(tagsbuf);
			return -1;
		}
		switch (*path)
		{
		default:
			if (beholddb_tagchar != *path)
			{
				*pathptr++ = '/';
				while (*path && '/' != *path)
					*pathptr++ = *path++;
				break;
			}
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
				while (*path && '/' != *path && beholddb_tagchar != *path)
					*tagsptr++ = *path++;
				*tagsptr++ = 0;
			} while (beholddb_tagchar == *path);
		case '/':
			; // remove redundant slashes
		case 0:
			; // remove trailing slash
		}
	}

	// null terminate path
	*pathptr++ = 0;

	// copy tags to path buffer
	memcpy(pathptr, tagsbuf, tagsptr - tagsbuf);
	tagsptr = pathptr + (tagsptr - tagsbuf);
	free(tagsbuf);

	// allocate space for tags
	const char **tagsarr = (const char**)calloc(tagcount[0] + tagcount[1] + 2, sizeof(char*));
	// negative tags go after positive tags
	tagcount[1] = tagcount[0] + 1;
	// positive tags go first
	tagcount[0] = 0;
	for (; pathptr != tagsptr; pathptr += strlen(pathptr) + 1)
	{
		int minus;
		// skip minus sign
		if ((minus = '-' == *pathptr))
			++pathptr;
		// store tag
		tagsarr[tagcount[minus ^ !!invert]++] = pathptr;
		syslog(LOG_DEBUG, "new tag: %c%s%s%s", beholddb_tagchar, invert ? "!" : "", minus ? "-" : "", pathptr);
	}
	// terminate tags lists
	tagsarr[tagcount[0]] = NULL;
	tagsarr[tagcount[1]] = NULL;

	syslog(LOG_DEBUG, "beholddb_parse_path: realpath=%s, count_p=%d, count_m=%d",
		pathbuf, tagcount[0], tagcount[1]);
	*realpath = pathbuf;
	*tags = tagsarr;
	return 0;
}

static const char BEHOLDDB_NAME[] = ".beholdfs";

static char *beholddb_get_name(const char *realpath)
{
	char *file = strrchr(realpath, '/');
	if (!file++)
		return NULL;
	syslog(LOG_DEBUG, "beholddb_get_name: path=%s, file=%s, bytes=%d, dbname=%s", realpath, file, file - realpath, BEHOLDDB_NAME);
	char *dbpath = (char*)malloc(file - realpath + 1 + sizeof(BEHOLDDB_NAME));
	memcpy(dbpath, realpath, file - realpath);
	memcpy(dbpath + (file - realpath), BEHOLDDB_NAME, sizeof(BEHOLDDB_NAME));
	syslog(LOG_DEBUG, "beholddb_get_name: dbpath=%s", dbpath);
	return dbpath;
}

static int beholddb_exec(sqlite3 *db, const char *sql)
{
	char *err;
	int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
	syslog(LOG_DEBUG, "beholddb_exec: sql=%s, rc=%d, err=%s", sql, rc, err ? err : "ok");
	sqlite3_free(err);
	return rc;
}

static int beholddb_init(sqlite3 *db)//, const char *path)
{
	//fs_create_module(db, path);
	//sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, 1, NULL);
	beholddb_exec(db, "pragma foreign_keys = on;");
	sqlite3_extended_result_codes(db, 1);
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_create_tables(sqlite3 *db)
{
	return beholddb_exec(db,
		"create table if not exists files "
		"("
			"id integer primary key,"
			"type integer not null,"
			"name text unique on conflict ignore"
		");"
		"create table if not exists tags"
		"("
			"id integer primary key,"
			"name text unique on conflict ignore"
		");"
		"create table if not exists files_tags"
		"("
			//"id integer primary key,"
			"id_file integer not null references files(id) on delete cascade,"
			"id_tag integer not null references tags(id),"// on delete cascade,"
			"unique ( id_file, id_tag ) on conflict ignore"
		");"
		"create table if not exists dirs_tags"
		"("
			//"id integer primary key,"
			"id_file integer not null references files(id) on delete cascade,"
			"id_tag integer not null references tags(id),"// on delete cascade,"
			"unique ( id_file, id_tag ) on conflict ignore"
		");"
		"create view if not exists strong_tags as "
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

static int beholddb_open_read(const char *realpath, sqlite3 **db)
{
	if (!*realpath)
		return 0;
	char *db_name = beholddb_get_name(realpath);
	// if in root directory, assume success
	if (!db_name)
		return 0;

	// open the database
	int rc = sqlite3_open_v2(db_name, db, SQLITE_OPEN_READWRITE, NULL);
	free(db_name);
	if (rc || (rc = beholddb_init(*db)))
	{
		sqlite3_close(*db);
		*db = NULL;
	}
	return rc;
}

static int beholddb_open_write(const char *realpath, sqlite3 **db)
{
	syslog(LOG_DEBUG, "beholddb_open_write(path=%s)", realpath);
	if (!*realpath)
		return 0;
	char *db_name = beholddb_get_name(realpath);

	syslog(LOG_DEBUG, "beholddb_open_write: dbname=%s", db_name);
	// if in root directory, assume success
	if (!db_name)
		return 0;

	// open the database
	int rc = sqlite3_open_v2(db_name, db, SQLITE_OPEN_READWRITE, NULL);

	syslog(LOG_DEBUG, "beholddb_open_write: first open result=%d", rc);
	if (rc && SQLITE_CANTOPEN == rc)
	{
		sqlite3_close(*db);
		(rc = sqlite3_open_v2(db_name, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) ||
		(rc = beholddb_create_tables(*db));
		syslog(LOG_DEBUG, "beholddb_open_write: second open result=%d", rc);
	}
	if (rc || (rc = beholddb_init(*db)))
	{
		sqlite3_close(*db);
		*db = NULL;
	}
	free(db_name);
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

static int beholddb_exec_bind_text(sqlite3 *db, const char *sql, const char *text)
{
	sqlite3_stmt *stmt;
	int rc;
	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, text, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));
	if (SQLITE_ROW == rc)
		rc = SQLITE_OK;
	syslog(LOG_DEBUG, "beholddb_exec_bind_text: sql=%s, text=%s, rc=%d, err=%s", sql, text, rc, sqlite3_errmsg(db));
	sqlite3_finalize(stmt);
	return rc;
}

static int beholddb_exec_select_text(sqlite3 *db, const char *sql, char *buf, const char **rows, size_t *count, size_t *size)
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
		if (SQLITE_DONE == rc)
			rc = SQLITE_OK; else
		{
			*count = 0;
			*size = 0;
		}
	}
	sqlite3_finalize(stmt);
	syslog(LOG_DEBUG, "beholddb_exec_select_text: sql=%s, count=%d, size=%d, rc=%d", sql, *count, *size, rc);
	return rc;
}

static int beholddb_set_tags_worker(sqlite3 *db, const char *sql_create,
	const char *sql_include, const char *sql_exclude, const char *const *tags)
{
	if (!tags)
		return 0;

	if (sql_create)
		beholddb_exec(db, sql_create);

	int rc;
	sqlite3_stmt *stmt;

	if ((rc = sqlite3_prepare_v2(db, sql_include, -1, &stmt, NULL)))
	{
		// some error handling
		for (; *tags; ++tags);
	} else
	{
		for (; *tags; ++tags)
		{
			(rc = sqlite3_reset(stmt)) ||
			(rc = sqlite3_bind_text(stmt, 1, *tags, -1, SQLITE_STATIC)) ||
			(rc = sqlite3_step(stmt));
		}
	}
	sqlite3_finalize(stmt);

	if (!(rc = sqlite3_prepare_v2(db, sql_exclude, -1, &stmt, NULL)))
	{
		for (++tags; *tags; ++tags)
		{
			(rc = sqlite3_reset(stmt)) ||
			(rc = sqlite3_bind_text(stmt, 1, *tags, -1, SQLITE_STATIC)) ||
			(rc = sqlite3_step(stmt));
		}
	}
	sqlite3_finalize(stmt);
	return 0; // TODO: handle errors
}

static int beholddb_create_tags(sqlite3 *db, const char *const *tags)
{
	if (!tags)
		return 0;

	int rc;
	sqlite3_stmt *stmt;
	const char *sql =
		"insert into tags ( name ) "
		"values ( ? )";
	if (!(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)))
	{
		for (; *tags; ++tags)
		{
			(rc = sqlite3_reset(stmt)) ||
			(rc = sqlite3_bind_text(stmt, 1, *tags, -1, SQLITE_STATIC)) ||
			(rc = sqlite3_step(stmt));
		}
	}
	sqlite3_finalize(stmt);
	return 0; // TODO: handle errors
}

static const char *beholddb_ddl_files_tags =
	"create temp table include"
	"("
		"id integer primary key on conflict ignore,"
		"name text"
	");"
	"create temp table exclude"
	"("
		"id integer primary key on conflict ignore,"
		"name text"
	");";

static const char *beholddb_ddl_dirs_tags =
	"create temp table dirs_include"
	"("
		"id integer primary key on conflict ignore,"
		"name text"
	");"
	"create temp table dirs_exclude"
	"("
		"id integer primary key on conflict ignore,"
		"name text"
	");";

static int beholddb_set_tags(sqlite3 *db, const char *const *files_tags, const char *const *dirs_tags)
{
	//beholddb_begin_transaction(db);
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
	//beholddb_commit(db);
}

// calculates number of entries in the list
// returns pointer to the next list
static const char *const *list_stat(const char *const *list, size_t *count, size_t *len)
{
	*count = 0;
	if (len)
		*len = 0;
	for (; *list; ++list)
	{
		syslog(LOG_DEBUG, "list_stat: %s", *list);
		++*count;
		if (len)
			*len += strlen(*list) + 1;
	}
	syslog(LOG_DEBUG, "list_stat: NULL");
	return ++list;
}

// calculates numbers of entries in multiple lists
static void lists_stat(const char *const *list, size_t **counts, size_t **lens, size_t size)
{
	//const char **init = list;
	for (int i = 0; i < size; ++i)
		list = list_stat(list, counts[i], lens ? lens[i] : NULL);
}

// returns numbers of entries in tags string
static void tags_stat(const char *const *tags, size_t *count_p, size_t *count_m, size_t *len_p, size_t *len_m)
{
	size_t *counts[2] = { count_p, count_m };
	size_t *lens[2] = { len_p, len_m };
	lists_stat(tags, counts, lens, 2);
}

static int beholddb_locate_file_worker(sqlite3 *db, const char *file, const char *const *tags)
{
	if (!strcmp(file, BEHOLDDB_NAME))
	{
		syslog(LOG_DEBUG, "The file is a metadata file (%s)", file);
		return -1;
	}

	size_t tagcount_p, tagcount_m;
	tags_stat(tags, &tagcount_p, &tagcount_m, NULL, NULL);
	// if no tags are defined, assume path exists
	if (!tagcount_p && !tagcount_m)
	{
		syslog(LOG_DEBUG, "beholddb_locate_file: there are no tags to consider");
		return 0;
	}
	syslog(LOG_DEBUG, "beholddb_locate_file: count_p=%d, count_m=%d", tagcount_p, tagcount_m);

	sqlite3_stmt *stmt;
	const char *sql =
		"select "
			"(select count(*) from ( "
				"select t.id from include t "
				"intersect "
				"select ft.id_tag from files f "
				"join files_tags ft on ft.id_file = f.id "
				"where f.name = ?1 "
			")), "
			"(select count(*) from ( "
				"select t.id from exclude t "
				"intersect "
				"select st.id_tag from files f "
				"join strong_tags st on st.id_file = f.id "
				"where f.name = ?1 "
			"))";

	int rc;
	(rc = beholddb_set_tags(db, tags, NULL)) ||
	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, file, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));
	if (SQLITE_ROW == rc)
		rc = SQLITE_OK;
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_locate_file: database error (%d)", rc);
		sqlite3_finalize(stmt);
		return -1;
	}

	int count_include = sqlite3_column_int(stmt, 0);
	int count_exclude = sqlite3_column_int(stmt, 1);

	sqlite3_finalize(stmt);

	syslog(LOG_DEBUG, "beholddb_locate_file: include=%d, exclude=%d", count_include, count_exclude);
	// all inclusive tags must be present
	// and no exclusive tags must be present
	return count_include == tagcount_p && !count_exclude ? 0 : 1;
}

int beholddb_locate_file(const char *realpath, const char *const *tags)
{
	syslog(LOG_DEBUG, "beholddb_locate_file(realpath=%s)", realpath);
	char *file = strrchr(realpath, '/');
	if (!file++)
	{
		syslog(LOG_DEBUG, "beholddb_locate_file: realpath is relative");
		return 0;
	}

	sqlite3 *db;
	int rc = beholddb_open_read(realpath, &db);
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_locate_file: error opening database (%d)", rc);
		return -1;
		// if inclusive tags are not defined, assume no error
		//return tagcount_p ? -1 : 0;
	}

	rc = beholddb_locate_file_worker(db, file, tags);
	sqlite3_close(db);
	return rc;
}

// find file by mixed path
// return error if not found
int beholddb_get_file(const char *path, const char **realpath, const char *const **tags)
{
	syslog(LOG_DEBUG, "beholddb_get_file(path=%s)", path);
	int status = beholddb_parse_path(path, realpath, tags, 0);
	if (!status)
		status = beholddb_locate_file(*realpath, *tags);
	syslog(LOG_DEBUG, "beholddb_get_file: status=%d", status);
	return status;
}

// free memory for path and tags
int beholddb_free_path(const char *realpath, const char *const *tags)
{
	free((void*)realpath);
	free((void*)tags);
	return 0;
}

static int beholddb_mark_object(const char *realpath, const char *const *files_tags, const char *const *dirs_tags);

static int beholddb_mark_recursive(sqlite3 *db, const char *realpath, const char *file, const char *const *files_tags)
{
	size_t tagcount_p, tagcount_m, tagsize_p, tagsize_m;
	size_t pathlen, count, size;
	char *path, *pathbuf;
	const char **rows, **pfiles_tags, **pdirs_tags;

	syslog(LOG_DEBUG, "beholddb_mark_recursive(path=%s, file=%s)", realpath, file);
	pathlen = file - realpath - 1;
	if (!pathlen || 1 == pathlen && '.' == *realpath)
	{
		syslog(LOG_DEBUG, "beholddb_mark_recursive: root directory");
		return SQLITE_OK;
	}

	tags_stat(files_tags, &tagcount_p, &tagcount_m, &tagsize_p, &tagsize_m);
	pathbuf = path = (char*)malloc(pathlen + 1 + tagsize_p + tagsize_m);
	rows = (const char**)calloc(2 * (tagcount_p + tagcount_m + 2), sizeof(char*));
	syslog(LOG_DEBUG, "beholddb_mark_recursive: count_p=%d, count_m=%d, size_p=%d, size_m=%d", tagcount_p, tagcount_m, tagsize_p, tagsize_m);

	memcpy(path, realpath, pathlen);
	pathbuf += pathlen;
	*pathbuf++ = 0;

	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 1");
	// include => at least one file has this tag
	// exclude => at least one file has no tag

	//	include becomes include for parent's files_tags
	//	exclude becomes exclude for parent's files_tags (filtered by 'no files have tag')
	pfiles_tags = rows;
	memcpy(pfiles_tags, files_tags, (tagcount_p + 1) * sizeof(char*));
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 2");
	beholddb_exec_select_text(db,
		"select t.name from exclude t "
		"where not exists "
			"(select * from files_tags ft "
			"where ft.id_tag = t.id)",
		pathbuf,
		pfiles_tags + tagcount_p + 1,
		&count,
		&size);
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 3");
	pfiles_tags[tagcount_p + 1 + count++] = NULL;
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 4");

	//	include becomes include for parent's dirs_tags (filtered by 'all files have tag, all dirs have dirs_tag')
	//	exclude becomes exclude for parent's dirs_tags
	pathbuf += size;
	pdirs_tags = pfiles_tags + tagcount_p + 1 + count;
	beholddb_exec_select_text(db,
		"select t.name from include t "
		"where not exists "
			"(select f.id, t.id from files f "
			"except "
			"select st.id_file, st.id_tag from strong_tags st)",
		pathbuf,
		pdirs_tags,
		&count,
		&size);
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 5");
	pdirs_tags[count++] = NULL;
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 6");
	memcpy(pdirs_tags + count, files_tags + tagcount_p + 1, (tagcount_m + 1) * sizeof(char*));
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 7");

	beholddb_mark_object(path, pfiles_tags, pdirs_tags);
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 8");
	beholddb_free_path(path, rows);
	syslog(LOG_DEBUG, "beholddb_mark_recursive: checkpoint 9");
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
		"delete from files_tags "
		"where id_file = "
			"( select id from files where name = ? ) "
		"and id_tag in "
			"( select id from exclude )",
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
			"delete from dirs_tags "
			"where id_file = "
				"( select id from files where name = ? ) "
			"and id_tag in "
				"( select id from dirs_exclude )",
			file);
		changes += sqlite3_changes(db);
	}
	if (pchanges)
		*pchanges = changes;
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_mark(sqlite3 *db, const char *realpath, const char *const *files_tags, const char *const *dirs_tags)
{
	const char *file = strrchr(realpath, '/');
	if (!file++)
		return 0;

	int changes;

	syslog(LOG_DEBUG, "beholddb_mark(%s)", realpath);

	//beholddb_begin_transaction(db);
	beholddb_create_tags(db, files_tags);
	//?beholddb_create_tags(db, dirs_tags);
	beholddb_set_tags(db, files_tags, dirs_tags);
	beholddb_mark_worker(db, file, &changes);
	//beholddb_commit(db);
	if (!changes)
		return 0;

	return beholddb_mark_recursive(db, realpath, file, files_tags);
}

static int beholddb_mark_object(const char *realpath, const char *const *files_tags, const char *const *dirs_tags)
{
	syslog(LOG_DEBUG, "beholddb_mark_object(path=%s)", realpath);

	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_mark_object: error opening database (%d)", rc);
		return rc;
	}

	rc = beholddb_mark(db, realpath, files_tags, dirs_tags);
	syslog(LOG_DEBUG, "beholddb_mark_object: result=%d", rc);

	sqlite3_close(db);
	return rc;
}

int beholddb_mark_file(const char *realpath, const char *const *tags)
{
	return beholddb_mark_object(realpath, tags, NULL);
}

static int beholddb_get_tags_worker(sqlite3 *db, const char *sql_count, const char *sql_select, const char *const **tags)
{
	syslog(LOG_DEBUG, "beholddb_get_tags_worker(sql_count=%s, sql_select=%s)", sql_count, sql_select);
	if (!tags)
		return SQLITE_OK;

	int rc;
	sqlite3_stmt *stmt;
	int count_p, count_m;

	(rc = sqlite3_prepare_v2(db, sql_count, -1, &stmt, NULL)) ||
	(rc = sqlite3_step(stmt));
	if (SQLITE_ROW == rc)
	{
		count_p = sqlite3_column_int(stmt, 0);
		count_m = sqlite3_column_int(stmt, 1);
		syslog(LOG_DEBUG, "beholddb_get_tags_worker: count_p=%d, count_m=%d", count_p, count_m);
		rc = SQLITE_OK;
	}
	sqlite3_finalize(stmt);
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_get_tags_worker: database error (%d)", rc);
		*tags = NULL;
		return rc;
	}

	const char **tagsbuf = (const char**)calloc(count_p + count_m + 2, sizeof(char*)), **tagsptr = tagsbuf;
	if (!(rc = sqlite3_prepare_v2(db, sql_select, -1, &stmt, NULL)))
	{
		while (SQLITE_ROW == (rc = sqlite3_step(stmt)))
		{
			char *text;
			int textlen = sqlite3_column_bytes(stmt, 0);
			if (textlen++)
			{
				const char *tag = sqlite3_column_text(stmt, 0);
				syslog(LOG_DEBUG, "beholddb_get_tags_worker: got tag '%s'", tag);
				text = (char*)malloc(textlen);
				memcpy(text, tag, textlen);
			} else
			{
				syslog(LOG_DEBUG, "beholddb_get_tags_worker: got NULL");
				text = NULL;
			}
			*tagsptr++ = text;
		}
		if (SQLITE_DONE == rc)
			rc = SQLITE_OK;
	}
	sqlite3_finalize(stmt);
	*tags = tagsbuf;
	if (rc)
		syslog(LOG_DEBUG, "beholddb_get_tags_worker: database error (%d)", rc);
	return rc;
}

static int beholddb_get_tags(sqlite3 *db, const char *const **files_tags, const char *const **dirs_tags)
{
	//beholddb_begin_transaction(db);
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
	//beholddb_commit(db);
	return SQLITE_OK; // TODO: handle errors
}

static int beholddb_get_file_tags(sqlite3 *db, const char *file,
	const char *const **files_tags, const char *const **dirs_tags,
	int *type)
{
	// TODO: optimize?
	char *sql = sqlite3_mprintf(
		"create temp view v_files_tags as "
			"select t.name from tags t "
			"join files_tags ft on ft.id_tag = t.id "
			"join files f on f.id = ft.id_file "
			"where f.name = '%q';"

		"create temp view v_dirs_tags as "
			"select t.name from tags t "
			"join dirs_tags dt on dt.id_tag = t.id "
			"join files f on f.id = dt.id_file "
			"where f.name = '%q';",
		file, file);
	beholddb_exec(db, sql);
	sqlite3_free(sql);

	if (type)
	{
		int rc;
		sqlite3_stmt *stmt;
		const char *sql =
			"select type from files "
			"where name = ?";
		if (!(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)))
		{
			(rc = sqlite3_bind_text(stmt, 1, file, -1, SQLITE_STATIC)) ||
			(rc = sqlite3_step(stmt));
			if (SQLITE_ROW == rc)
				*type = sqlite3_column_int(stmt, 1); else
				*type = 0;
		}
		sqlite3_finalize(stmt);
	}

	beholddb_get_tags_worker(db,
		"select count(*), 0 from v_files_tags",
		"select name from v_files_tags "
		"union all "
		"select NULL "
		"union all "
		"select NULL ",
		files_tags);
	beholddb_get_tags_worker(db,
		"select count(*), 0 from v_dirs_tags",
		"select name from v_dirs_tags "
		"union all "
		"select NULL "
		"union all "
		"select NULL ",
		dirs_tags);
	beholddb_exec(db,
		"drop view v_files_tags;"
		"drop view v_dirs_tags;");
	return SQLITE_OK;
}

void beholddb_free_tags(const char *const *tags)
{
	if (!tags)
		return;
	const char *const *tagsptr = tags;
	for (; *tagsptr; ++tagsptr)
		free((void*)*tagsptr);
	for (++tagsptr; *tagsptr; ++tagsptr)
		free((void*)*tagsptr);
	free((void*)tags);
}

static const char *no_tags[] = { NULL, NULL };

static int beholddb_create_file_with_tags(const char *realpath,
	const char *const *files_tags, const char *const *dirs_tags,
	const char *const *tags, int type)
{
	syslog(LOG_DEBUG, "beholddb_create_file(realpath=%s)", realpath);

	char *file = strrchr(realpath, '/');
	if (!file++)
	{
		syslog(LOG_DEBUG, "beholddb_create_file: path is relative");
		return 0;
	}

	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_create_file: error opening database (%d)", rc);
		return rc;
	}

	beholddb_begin_transaction(db);

	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 1");
	char *sql = sqlite3_mprintf(
		"insert into files ( type, name ) "
		"values ( %d, '%q' )", !!type, file);
	rc = beholddb_exec(db, sql);
	sqlite3_free(sql);

	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 2");
	int changes;
	if (type)
		tags = no_tags;
	beholddb_create_tags(db, tags);
	if (files_tags)
		beholddb_create_tags(db, files_tags);
	if (type && dirs_tags)
		beholddb_create_tags(db, dirs_tags);
	beholddb_set_tags(db, files_tags, dirs_tags);
	beholddb_set_tags(db, tags, NULL);
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 3");
	beholddb_exec(db,
		"delete from include "
		"where id in "
		"(select id from exclude);"

		"insert into exclude "
		"select id, name from tags "
		"except "
		"select id, name from include;");
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 4");
	beholddb_mark_worker(db, file, &changes);
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 5");

	const char *const *create_tags;
	beholddb_get_tags(db, &create_tags, NULL);
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 6");
	beholddb_mark_recursive(db, realpath, file, create_tags);
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 7");
	beholddb_free_tags(create_tags);
	syslog(LOG_DEBUG, "beholddb_create_file: checkpoint 8");

	beholddb_commit(db);
	sqlite3_close(db);
	return rc; // TODO: error handling
}

int beholddb_create_file(const char *realpath, const char *const *tags, int type)
{
	return beholddb_create_file_with_tags(realpath, NULL, NULL, tags, type);
}

static int beholddb_delete_file_with_tags(const char *realpath,
	const char *const **files_tags, const char *const **dirs_tags,
	int *type)
{
	syslog(LOG_DEBUG, "beholddb_delete_file(realpath=%s)", realpath);
	char *file = strrchr(realpath, '/');
	if (!file++)
	{
		syslog(LOG_DEBUG, "beholddb_delete_file: path is relative");
		return 0;
	}

	sqlite3 *db;
	int rc = beholddb_open_write(realpath, &db);
	if (rc)
	{
		syslog(LOG_DEBUG, "beholddb_delete_file: error opening database (%d)", rc);
		return rc;
	}

	beholddb_begin_transaction(db);
	beholddb_get_file_tags(db, file, files_tags, dirs_tags, type);
	beholddb_set_tags(db, no_tags, NULL);
	beholddb_exec_bind_text(db,
		"delete from files "
		"where name = ?;",
		file);
	beholddb_exec(db,
		"insert into exclude "
		"select t.id, t.name from tags t "
		"where not exists "
			"(select * from files_tags ft where ft.id_tag = t.id);"

		"delete from tags "
		"where id in "
			"(select id from exclude);"

		"insert into include "
		"select id, name from tags "
		"except "
		"select id, name from exclude;");

	const char *const *delete_tags;
	beholddb_get_tags(db, &delete_tags, NULL);
	beholddb_mark_recursive(db, realpath, file, delete_tags);
	syslog(LOG_DEBUG, "beholddb_delete_file: checkpoint 3");
	beholddb_free_tags(delete_tags);
	syslog(LOG_DEBUG, "beholddb_delete_file: checkpoint 4");

	beholddb_commit(db);
	sqlite3_close(db);
	syslog(LOG_DEBUG, "beholddb_delete_file: result=%d", rc);
	return rc; // TODO: error handling
}

int beholddb_delete_file(const char *realpath)
{
	return beholddb_delete_file_with_tags(realpath, NULL, NULL, NULL);
}

int beholddb_rename_file(const char *oldrealpath, const char *newrealpath, const char *const *tags)
{
	int type;
	const char *const *files_tags, *const *dirs_tags;

	beholddb_delete_file_with_tags(oldrealpath, &files_tags, &dirs_tags, &type);
	beholddb_create_file_with_tags(newrealpath, files_tags, dirs_tags, tags, 0);
	beholddb_free_tags(files_tags);
	beholddb_free_tags(dirs_tags);
	return 0; // TODO: error handling
}


