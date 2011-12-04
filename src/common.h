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

#ifndef __COMMON_H__
#define __COMMON_H__

#include <sqlite3.h>
#include <stdarg.h>

const char *beholddb_column_text(sqlite3_stmt *stmt, int column);
int beholddb_get_param(sqlite3 *db, const char *param, const char **pvalue);
int beholddb_set_param(sqlite3 *db, const char *param, const char *value);
int beholddb_set_vfparam(sqlite3 *db, const char *param, const char *format, va_list args);
int beholddb_set_fparam(sqlite3 *db, const char *param, const char *format, ...);
int beholddb_exec(sqlite3 *db, const char *sql);
int beholddb_exec_bind_text(sqlite3 *db, const char *sql, const char *text);
int beholddb_bind_text(sqlite3_stmt *stmt, const char *param, const char *value);
int beholddb_bind_int(sqlite3_stmt *stmt, const char *param, int value);
int beholddb_bind_int64(sqlite3_stmt *stmt, const char *param, sqlite3_int64 value);
int beholddb_bind_blob(sqlite3_stmt *stmt, const char *param, const void *value, int size, void (*free)(void*));
int beholddb_bind_null(sqlite3_stmt *stmt, const char *param);
int beholddb_bind_int64_null(sqlite3_stmt *stmt, const char *param, sqlite3_int64 value);
int beholddb_begin(sqlite3 *db, const char *name);
int beholddb_end(sqlite3 *db, const char *name);
int beholddb_rollback(sqlite3 *db, const char *name);
int beholddb_end_result(sqlite3 *db, const char *name, int rc);

#endif // __COMMON_H__
