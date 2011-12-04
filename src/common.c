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
		rc = SQLITE_OK;
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
	return SQLITE_DONE == rc ? SQLITE_OK : rc;
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

int beholddb_exec(sqlite3 *db, const char *sql)
{
  int rc;
  sqlite3_stmt *stmt;

  (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
  (rc = sqlite3_step(stmt));
  if (rc && SQLITE_DONE != rc)
  {
    syslog(LOG_DEBUG, "beholddb_exec(%s): rc=%d, %s", sql, rc, sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  return SQLITE_DONE == rc ? SQLITE_OK : rc;
}

int beholddb_exec_bind_text(sqlite3 *db, const char *sql, const char *text)
{
  int rc;
  sqlite3_stmt *stmt;
  char *sql_text = sqlite3_mprintf(sql, text);

  (rc = sqlite3_prepare_v2(db, sql_text, -1, &stmt, NULL)) ||
  (rc = sqlite3_step(stmt));
  if (rc && SQLITE_DONE != rc)
  {
    syslog(LOG_DEBUG, "beholddb_exec_bind_text(%s, text=%s): rc=%d, %s", sql, text, rc, sqlite3_errmsg(db));
  }
  sqlite3_finalize(stmt);
  sqlite3_free(sql_text);
  return SQLITE_DONE == rc ? SQLITE_OK : rc;
}

int beholddb_bind_text(sqlite3_stmt *stmt, const char *param, const char *value)
{
  syslog(LOG_DEBUG, "beholddb_bind_text: %s=%s", param, value);
  int i = sqlite3_bind_parameter_index(stmt, param);
  return sqlite3_bind_text(stmt, i, value, -1, SQLITE_STATIC);
}

int beholddb_bind_int(sqlite3_stmt *stmt, const char *param, int value)
{
  int i = sqlite3_bind_parameter_index(stmt, param);
  syslog(LOG_DEBUG, "beholddb_bind_int: %s=%d, index=%d", param, value, i);
  return sqlite3_bind_int(stmt, i, value);
}

int beholddb_bind_int64(sqlite3_stmt *stmt, const char *param, sqlite3_int64 value)
{
  int i = sqlite3_bind_parameter_index(stmt, param);
  syslog(LOG_DEBUG, "beholddb_bind_int64: %s=%lld, index=%d", param, value, i);
  return sqlite3_bind_int64(stmt, i, value);
}

int beholddb_bind_blob(sqlite3_stmt *stmt, const char *param, const void *value, int size, void (*free)(void*))
{
  int i = sqlite3_bind_parameter_index(stmt, param);
  syslog(LOG_DEBUG, "beholddb_bind_blob: %s=%p of size %d, index=%d", param, value, size, i);
  return sqlite3_bind_blob(stmt, i, value, size, free);
}

int beholddb_bind_null(sqlite3_stmt *stmt, const char *param)
{
  int i = sqlite3_bind_parameter_index(stmt, param);
  syslog(LOG_DEBUG, "beholddb_bind_null: %s, index=%d", param, i);
  return sqlite3_bind_null(stmt, i);
}

int beholddb_bind_int64_null(sqlite3_stmt *stmt, const char *param, sqlite3_int64 value)
{
  int i = sqlite3_bind_parameter_index(stmt, param);
  syslog(LOG_DEBUG, "beholddb_bind_int64_null: %s=%lld, index=%d", param, value, i);
  return -1 == value ?
    sqlite3_bind_null(stmt, i) :
    sqlite3_bind_int64(stmt, i, value);
}

int beholddb_begin(sqlite3 *db, const char *name)
{
  syslog(LOG_DEBUG, "beholddb_begin: %s", name);
  return name ?
    beholddb_exec_bind_text(db, "savepoint %s", name) :
    beholddb_exec(db, "begin");
}

int beholddb_end(sqlite3 *db, const char *name)
{
  syslog(LOG_DEBUG, "beholddb_end: %s", name);
  return name ?
    beholddb_exec_bind_text(db, "release %s", name) :
    beholddb_exec(db, "end");
}

int beholddb_rollback(sqlite3 *db, const char *name)
{
  syslog(LOG_DEBUG, "beholddb_rollback: %s", name);
  return name ?
    beholddb_exec_bind_text(db, "rollback to %s", name) :
    beholddb_exec(db, "rollback");
}

int beholddb_end_result(sqlite3 *db, const char *name, int rc)
{
  syslog(LOG_DEBUG, "beholddb_end_result: %s, rc=%d", name, rc);
  if (SQLITE_OK == rc || SQLITE_DONE == rc)
  {
    if (!beholddb_end(db, name))
      return BEHOLDDB_OK;
  }

  beholddb_rollback(db, name);
  return BEHOLDDB_ERROR;
}

