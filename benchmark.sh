#!/bin/bash

SQL_INIT="\
create temp table include ( id integer primary key, name text unique on conflict ignore );\
create temp table exclude ( id integer primary key, name text unique on conflict ignore );\
"

SQL_DONE="\
"

DATABASE="$HOME/Pictures/.beholdfs"
ITERATIONS=100
INCLUDE_TAGS="'girl'"

SQL_INCLUDE="\
insert into include select id, name from tags where name in ( $INCLUDE_TAGS );\
"

SQL_SELECT_TAGS_1="select distinct t.name from files f join files_tags ft on ft.id_file = f.id join tags t on t.id = ft.id_tag where not exists ( select t.id from include t except select t.id from include t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) and not exists ( select * from exclude t join files_tags ft on ft.id_tag = t.id join files f on f.id = ft.id_file where ft.id_file = f.id and f.type = 0 ) and not exists ( select * from exclude t join dirs_tags dt on dt.id_tag = t.id where dt.id_file = f.id ) and t.id not in ( select id from include ) and t.id not in ( select id from exclude );"

SQL_SELECT_TAGS_2="select distinct t.name from files f join files_tags ft on ft.id_file = f.id join tags t on t.id = ft.id_tag where not exists ( select t.id from include t except select t.id from include t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) and case when f.type = 0 then not exists ( select * from exclude t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) else not exists ( select * from exclude t join dirs_tags dt on dt.id_tag = t.id where dt.id_file = f.id ) end except select name from include except select name from exclude;"

SQL_SELECT_TAGS_3="select distinct t.name from files f join files_tags ft on ft.id_file = f.id join tags t on t.id = ft.id_tag where not exists ( select t.id from include t except select t.id from include t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) and not exists ( select * from exclude t join strong_tags st on st.id_tag = t.id where st.id_file = f.id ) except select name from include except select name from exclude;"

SQL_SELECT_TAGS_4="select t.name from ( select distinct ft.id_tag id from files f join files_tags ft on ft.id_file = f.id where not exists ( select t.id from include t except select t.id from include t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) and case when f.type = 0 then not exists ( select * from exclude t join files_tags ft on ft.id_tag = t.id where ft.id_file = f.id ) else not exists ( select * from exclude t join dirs_tags dt on dt.id_tag = t.id where dt.id_file = f.id ) end except select id from include except select id from exclude) tt join tags t on t.id = tt.id join files_tags ft on ft.id_tag = tt.id group by tt.id order by count(*) desc;"

benchmark()
{
	local SQL

	SQL_SELECT=$1
	SQL="$SQL $SQL_INIT $SQL_INCLUDE"
	for (( i = 0; i < $ITERATIONS; ++i )); do
		SQL="$SQL $SQL_SELECT"
	done
	SQL="$SQL $SQL_DONE"

	echo
	echo "$2: "
	echo "$SQL" | time -f "usr:%U\tsys:%S\twall:%E" sqlite3 $DATABASE >/dev/null
}

no_benchmark()
{
	echo
	echo "$2: "
	echo "Option discarded - takes too long"
}

no_benchmark "$SQL_SELECT_TAGS_1" "where clause"
benchmark "$SQL_SELECT_TAGS_2" "separate tags and except"
no_benchmark "$SQL_SELECT_TAGS_3" "strong tags and except"
benchmark "$SQL_SELECT_TAGS_4" "sorted separate tags and except"

