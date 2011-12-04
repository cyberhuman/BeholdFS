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

#ifndef __VERSION_H__
#define __VERSION_H__

#include <sqlite3.h>

#define BEHOLDDB_VERSION_MAJOR	1
#define BEHOLDDB_VERSION_MINOR	0
#define BEHOLDDB_VERSION_PARAM  "version"
#define BEHOLDDB_VERSION_FORMAT "%d.%d"

int version_init(sqlite3 *db);

#endif // __VERSION_H__

