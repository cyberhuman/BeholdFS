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

#endif // __COMMON_H__
