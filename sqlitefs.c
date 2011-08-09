#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "fs.h"

/*
 * SQLite invokes this routine once when it loads the extension.
 * Create new functions, collating sequences, and virtual table
 * modules here.  This is usually the only exported symbol in
 * the shared library.
 */
int sqlite3_extension_init(
	sqlite3 *db,
	char **pzErrMsg,
	const sqlite3_api_routines *pApi)
{
	SQLITE_EXTENSION_INIT2(pApi)
	fs_create_module(db, "/");
	return 0;
}

