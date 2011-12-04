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

static int version_decode(const char *version, int *pmajor, int *pminor)
{
	if (!version)
	{
		*pmajor = -1;
		*pminor = 0;
		return BEHOLDDB_OK;
	}
	if (2 == sscanf(version, BEHOLDDB_VERSION_FORMAT, pmajor, pminor))
		return BEHOLDDB_OK;
	return BEHOLDDB_ERROR;
}

int version_init(sqlite3 *db)
{
	int rc;

	const char *version;
	int major, minor;

	(rc = beholddb_get_param(db, BEHOLDDB_VERSION_PARAM, &version)) ||
	(rc = version_decode(version, &major, &minor));
	free(version);

	if (rc)
	{
		// error reading configuration
		return BEHOLDDB_ERROR;
	}
	if (BEHOLDDB_VERSION_MAJOR < major)
	{
		// metadata format is too new
		syslog(LOG_ERR, "Metadata format is too new");
		return BEHOLDDB_ERROR;
	}

	if (BEHOLDDB_VERSION_MAJOR == major && BEHOLDDB_VERSION_MINOR < minor)
	{
		// notice: database version is newer
		syslog(LOG_NOTICE, "Metadata format is newer than the current");
	} else
	{
		syslog(LOG_INFO, "Update version to " BEHOLDDB_VERSION_FORMAT,
			BEHOLDDB_VERSION_MAJOR, BEHOLDDB_VERSION_MINOR);
		beholddb_set_fparam(db,
      BEHOLDDB_VERSION_PARAM, BEHOLDDB_VERSION_FORMAT,
			BEHOLDDB_VERSION_MAJOR, BEHOLDDB_VERSION_MINOR);
	}

	switch (major)
	{
	case 0:
		;
	case BEHOLDDB_VERSION_MAJOR:
		// database version currently supported, nothing to do
		break;
	//case 1:
		// some old version, probably convert to the new format
	}

	return BEHOLDDB_OK;
}

