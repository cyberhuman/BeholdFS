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
#include <string.h>
#include <sqlite3.h>
#include <syslog.h>

#include "version.h"
#include "common.h"
#include "beholddb.h"

#define BEHOLDDB_VERSION_MAJOR	1
#define BEHOLDDB_VERSION_MINOR	0

static int beholddb_decode_version(const char *version, int *pmajor, int *pminor)
{
	if (!version)
	{
		*pmajor = *pminor = 0;
		return BEHOLDDB_OK;
	}
	if (2 == sscanf(version, "%d.%d", pmajor, pminor))
		return BEHOLDDB_OK;
	return BEHOLDDB_ERROR;
}

int beholddb_init_version(sqlite3 *db)
{
	int rc;

	beholddb_exec(db,
		"create table if not exists config "
		"( "
			"id integer primary key, "
			"param text unique on conflict replace, "
			"value text "
		") ");

	const char *version;
	int major, minor;

	(rc = beholddb_get_param(db, "version", &version)) ||
	(rc = beholddb_decode_version(version, &major, &minor));
	free(version);

	if (rc)
	{
		// error reading configuration
		return BEHOLDDB_ERROR;
	}
	if (BEHOLDDB_VERSION_MAJOR < major)
	{
		// metadata format is too new
		return BEHOLDDB_ERROR;
	}

	if (BEHOLDDB_VERSION_MAJOR == major && BEHOLDDB_VERSION_MINOR < minor)
	{
		// notice: database version is newer
	} else
	{
		beholddb_set_fparam(db, "version", "%d.%d",
			BEHOLDDB_VERSION_MAJOR, BEHOLDDB_VERSION_MINOR);
	}


	switch (major)
	{
	case 0:
		// database is just created, nothing to do
	case BEHOLDDB_VERSION_MAJOR:
		// database version currently supported, nothing to do
		break;
	//case 1:
		// some old version, probably convert to the new format
	}

	return BEHOLDDB_OK;
}


