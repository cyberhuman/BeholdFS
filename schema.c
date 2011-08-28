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

#include "schema.h"

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
	"select distinct ft.id_tag id "
	"from files f "
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


