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

#include <sqlite3.h>

#include "schema.h"
#include "common.h"
#include "version.h"

const char *BEHOLDDB_DML_LOCATE =
	"select 1 from ( select ? name ) fs "
	"left outer join files f on f.name = fs.name "
	"where not exists ( "
		"select t.id from include t "
		"except "
		"select t.id from include t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"and case when f.type = 0 "
	"then not exists ( "
		"select t.id from exclude t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"else not exists ( "
		"select t.id from exclude t "
		"join dirs_tags dt on dt.id_tag = t.id "
		"where dt.id_file = f.id ) "
	"end ";

const char *BEHOLDDB_DDL_FAST_LOCATE_START =
	"create temp table fast_files ( id integer primary key, name text unique );"
	"insert into fast_files "
	"select f.id, f.name from files f "
	"where not exists ( "
		"select t.id from include t "
		"except "
		"select t.id from include t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"and case when f.type = 0 "
	"then not exists ( "
		"select t.id from exclude t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"else not exists ( "
		"select t.id from exclude t "
		"join dirs_tags dt on dt.id_tag = t.id "
		"where dt.id_file = f.id ) "
	"end ";

const char *BEHOLDDB_DML_FAST_LOCATE =
	"select 1 from fast_files f "
	"where f.name = ?";

const char *BEHOLDDB_DDL_FAST_LOCATE_STOP =
	"drop table fast_files;";

const char *BEHOLDDB_DML_TAG_LISTING =
	"select t.name from ( "
	"select distinct ft.id_tag id from files f "
	"join files_tags ft on ft.id_file = f.id "
	"where not exists ( "
		"select t.id from include t "
		"except "
		"select t.id from include t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"and case when f.type = 0 "
	"then not exists ( "
		"select t.id from exclude t "
		"join files_tags ft on ft.id_tag = t.id "
		"where ft.id_file = f.id ) "
	"else not exists ( "
		"select t.id from exclude t "
		"join dirs_tags dt on dt.id_tag = t.id "
		"where dt.id_file = f.id ) "
	"end "
	"except select id from include "
	"except select id from exclude ) tt "
	"join tags t on t.id = tt.id "
	"join files_tags ft on ft.id_tag = tt.id "
	"group by tt.id "
	"order by count(*) desc ";

const char *BEHOLDDB_DML_FILE_TAG_LISTING =
	"select t.name "
	"from files f "
	"join files_tags ft on ft.id_file = f.id "
	"join tags t on t.id = ft.id_tag "
	"where f.name = ?";

const char *BEHOLDDB_DDL_CREATE_CONFIG =
	"create table if not exists config "
	"( "
		"id integer primary key, "
		"param text unique on conflict replace, "
		"value text "
	") ";

int schema_create(sqlite3 *db)
{
  int rc;
  static const char *sp = "tables";

  (rc = beholddb_begin(db, sp)) ||

  // create schema
	(rc = beholddb_exec(db, BEHOLDDB_DDL_CREATE_CONFIG)) ||
  (rc = beholddb_exec(db,
    "create table if not exists objects "
    "( "
    "id integer primary key, "
    "id_parent integer references objects ( id ) on delete restrict, "
    "type integer, "
    "name text, "
    "unique ( id_parent, type, name ) "
    ") ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_parent on objects ( id_parent ) ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_name on objects ( name ) ")) ||
  (rc = beholddb_exec(db,
    "create table if not exists objects_owners "
    "( "
    "id_owner integer references objects ( id ) on delete cascade, "
    "id_object integer references objects ( id ) on delete cascade, "
    "unique ( id_owner, id_object ) " // on conflict ignore ?
    ") ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_owners_owner on objects_owners ( id_owner ) ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_owners_object on objects_owners ( id_object ) ")) ||
  (rc = beholddb_exec(db,
    "create table if not exists objects_tags "
    "( "
    "id_object integer references objects ( id ) on delete cascade, "
    "id_tag integer references objects ( id ) on delete cascade, "
    "unique ( id_object, id_tag ) " // on conflict ignore ?
    ") ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_tags_object on objects_tags ( id_object ) ")) ||
  (rc = beholddb_exec(db,
    "create index if not exists objects_tags_tag on objects_tags ( id_tag ) ")) ||

  // initialize filesystem root (TODO: move to beholddb.c)
  (rc = beholddb_exec(db,
    "insert into objects ( name, type ) values ( '/', 1 ) ")) ||
  (rc = beholddb_exec(db,
    "insert into objects_owners ( id_object, id_owner ) "
    "select id, id from objects where name is '/' and type = 1")) ||

  // set metadata version (TODO: move to version.c)
  (rc = beholddb_set_fparam(db,
    BEHOLDDB_VERSION_PARAM, BEHOLDDB_VERSION_FORMAT,
    BEHOLDDB_VERSION_MAJOR, BEHOLDDB_VERSION_MINOR));

  return beholddb_end(db, rc);
}

