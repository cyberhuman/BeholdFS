
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <stddef.h>
#include <unistd.h>
#include <sqlite3.h>
//extern const sqlite3_api_routines *sqlite3_api;

#include "fs.h"

typedef struct fs_vtab
{
	sqlite3_vtab base;
	sqlite3 *db;
	char *path;
} fs_vtab;

typedef struct fs_cursor
{
	sqlite3_vtab_cursor base;
	DIR *dir;
	struct dirent *entry;
	struct dirent *result;
} fs_cursor;

static const char *fs_ddl =
	"create table sqlitefs"
	"("
		"name text,"
		"type integer"
	")";

int fs_create(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVTab, char **pzErr)
{
	fs_vtab *pvtab = (fs_vtab*)sqlite3_malloc(sizeof(fs_vtab));
	if (!pvtab)
		return SQLITE_NOMEM;
	pvtab->base.zErrMsg = NULL;
	pvtab->db = db;
	pvtab->path = sqlite3_mprintf("%s", argc > 3 ? argv[3] : (char*)pAux);

	*ppVTab = &pvtab->base;
	return sqlite3_declare_vtab(db, fs_ddl);
}

int fs_destroy(sqlite3_vtab *pVTab)
{
	fs_vtab *pvtab = (fs_vtab*)pVTab;

	sqlite3_free(pvtab->path);
	sqlite3_free(pvtab);
	return SQLITE_OK;
}

int fs_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
{
	fs_vtab *pvtab = (fs_vtab*)pVTab;
	fs_cursor *pcur = (fs_cursor*)sqlite3_malloc(sizeof(fs_cursor));
	if (!pcur)
		return SQLITE_NOMEM;

	int len = offsetof(struct dirent, d_name) +
		pathconf(pvtab->path, _PC_NAME_MAX) + 1;
	pcur->dir = NULL;
	pcur->entry = (struct dirent*)sqlite3_malloc(len);

	*ppCursor = &pcur->base;
	return SQLITE_OK;
}

int fs_close(sqlite3_vtab_cursor* pCursor)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;

	if (pcur->dir)
		closedir(pcur->dir);
	sqlite3_free(pcur->entry);
	sqlite3_free(pcur);
	return SQLITE_OK;
}

int fs_best_index(sqlite3_vtab *pVTab, sqlite3_index_info *pIndex)
{
	fs_vtab *pvtab = (fs_vtab*)pVTab;

	return SQLITE_OK;
}

int read_fs(fs_cursor *pcur)
{
	if (readdir_r(pcur->dir, pcur->entry, &pcur->result))
		return SQLITE_IOERR;
	return SQLITE_OK;
}

int fs_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;
	fs_vtab *pvtab = (fs_vtab*)pcur->base.pVtab;

	pcur->dir = opendir(pvtab->path);
	return read_fs(pcur);
}

int fs_next(sqlite3_vtab_cursor *pCursor)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;

	return read_fs(pcur);
}

int fs_eof(sqlite3_vtab_cursor *pCursor)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;

	return !pcur->result;
}

int fs_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *pContext, int iCol)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;

	switch (iCol)
	{
	case 0: // name
		sqlite3_result_text(pContext, pcur->result->d_name, -1, SQLITE_STATIC);
		break;
	case 1: // type
		sqlite3_result_int(pContext, DT_DIR == pcur->result->d_type);
		break;
	default:
		return SQLITE_INTERNAL;
	}
	return SQLITE_OK;
}

int fs_rowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid)
{
	fs_cursor *pcur = (fs_cursor*)pCursor;

	*pRowid = pcur->result->d_ino;
	return SQLITE_OK;
}

int fs_find_function(sqlite3_vtab *pVtab, int nArg, const char *zName,
	void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
	void **ppArg)
{
	return 0;
}

static sqlite3_module fs_module =
{
	.iVersion	= 1,
	.xCreate	= fs_create,
	.xConnect	= fs_create,
	.xBestIndex	= fs_best_index,
	.xDisconnect	= fs_destroy,
	.xDestroy	= fs_destroy,
	.xOpen		= fs_open,
	.xClose		= fs_close,
	.xFilter	= fs_filter,
	.xNext		= fs_next,
	.xEof		= fs_eof,
	.xColumn	= fs_column,
	.xRowid		= fs_rowid,
	.xUpdate	= NULL,
	.xBegin		= NULL,
	.xSync		= NULL,
	.xCommit	= NULL,
	.xRollback	= NULL,
	.xFindFunction	= NULL,//fs_find_function,
	.xRename	= NULL,
};

int fs_create_module(sqlite3 *db, const char *path)
{
	char *root = sqlite3_mprintf("%s", path);
	return sqlite3_create_module_v2(db, "sqlitefs", &fs_module, root, sqlite3_free);
}

