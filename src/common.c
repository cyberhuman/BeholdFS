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

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sqlite3.h>
#include <syslog.h>

#include "beholddb.h"
#include "common.h"

const char *beholddb_column_text(sqlite3_stmt *stmt, int column)
{
	int textlen = sqlite3_column_bytes(stmt, column) + 1;
	char *text = malloc(textlen);

	memcpy(text, sqlite3_column_text(stmt, column), textlen);
	return text;
}

int beholddb_get_param(sqlite3 *db, const char *param, const char **pvalue)
{
	int rc;
	sqlite3_stmt *stmt;
	const char *sql = "select value from config where param = ?";

	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, param, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));
	*pvalue = NULL;

	switch (rc)
	{
	case SQLITE_ROW:
		*pvalue = beholddb_column_text(stmt, 0);
	case SQLITE_DONE:
		rc = BEHOLDDB_OK;
		break;
	default:
		rc = BEHOLDDB_ERROR;
	}

	sqlite3_finalize(stmt);
	return rc;
}

int beholddb_set_param(sqlite3 *db, const char *param, const char *value)
{
	int rc;
	sqlite3_stmt *stmt;
	const char *sql = "insert into config ( param, value ) values ( ?, ? )";

	(rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
	(rc = sqlite3_bind_text(stmt, 1, param, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC)) ||
	(rc = sqlite3_step(stmt));

	sqlite3_finalize(stmt);
	return rc ? BEHOLDDB_ERROR : BEHOLDDB_OK;
}

int beholddb_set_vfparam(sqlite3 *db, const char *param, const char *format, va_list args)
{
	const char *value = sqlite3_vmprintf(format, args);
	int rc = beholddb_set_param(db, param, value);

	sqlite3_free(value);
	return rc;
}

int beholddb_set_fparam(sqlite3 *db, const char *param, const char *format, ...)
{
	int rc;
	va_list args;

	va_start(args, format);
	rc = beholddb_set_vfparam(db, param, format, args);
	va_end(args);
	return rc;
}


