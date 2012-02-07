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

#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <syslog.h>

#include "beholddb.h"
#include "fs.h"
#include "common.h"
#include "schema.h"

typedef struct beholddb_iterator
{
  sqlite3 *db;
  sqlite3_stmt *stmt;
  const char *sp;
} beholddb_iterator;

typedef struct beholddb_dir
{
  sqlite3 *db;
  beholddb_iterator *it;
} beholddb_dir;

char beholddb_tagchar;
int beholddb_new_locate;
static const char BEHOLDDB_NAME[] = ".beholddb";

static char *strrdup(const char *begin, const char *end)
{
  int len = end - begin;
  char *str = (char*)malloc(1 + len);

  memcpy(str, begin, len);
  str[len] = 0;
  return str;
}

static void beholddb_insert_tag(beholddb_tag_list_item **phead, const char *name)
{
  beholddb_tag_list_item *item = (beholddb_tag_list_item*)malloc(sizeof(beholddb_tag_list_item));

  item->name = name;
  item->next = *phead;
  *phead = item;
}

static void beholddb_append_tag(beholddb_tag_list_item ***pphead, const char *name)
{
  beholddb_tag_list_item *item = (beholddb_tag_list_item*)malloc(sizeof(beholddb_tag_list_item));

  item->name = name;
  item->next = NULL;
  **pphead = item;
  *pphead = &item->next;
}

static void beholddb_delete_tag(beholddb_tag_list_item **pitem)
{
  beholddb_tag_list_item *save = *pitem;

  *pitem = save->next;
  free((char*)save->name);
  free(save);
}

static void beholddb_remove_tag(beholddb_tag_list_item **pitem)
{
  beholddb_tag_list_item *save = *pitem;

  *pitem = save->next;
  free(save);
}

static void beholddb_move_tag(beholddb_tag_list_item **pfrom, beholddb_tag_list_item **pto)
{
  beholddb_tag_list_item *save = (*pfrom)->next;

  (*pfrom)->next = *pto;
  *pto = *pfrom;
  *pfrom = save;
}

static beholddb_tag_list_item *beholddb_shadow_tag_list(beholddb_tag_list_item *head)
{
  beholddb_tag_list_item *cur = NULL;

  for (; head; head = head->next)
    beholddb_insert_tag(&cur, head->name);
  return cur;
}

//static int beholddb_copy_tag

static int beholddb_count_tags(beholddb_tag_list_item *head)
{
  int count = 0;

  for (; head; head = head->next)
    ++count;
  return count;
}

static void beholddb_free_tag_list(beholddb_tag_list *list)
{
  for (beholddb_tag_list_item **phead = &list->head; *phead; )
    beholddb_delete_tag(phead);
}

static void beholddb_remove_tag_list(beholddb_tag_list_item *head)
{
  while (head)
    beholddb_remove_tag(&head);
}

int beholddb_parse_path(const char *path, beholddb_path **pbpath)
{
  syslog(LOG_DEBUG, "beholddb_parse_path(path=%s)", path);

  int pathlen = strlen(path);
  beholddb_path *bpath = (beholddb_path*)malloc(sizeof(beholddb_path));
  beholddb_tag_list_item **ptail = &bpath->path.head;
  char *pathptr = (char*)malloc(pathlen + 3);

  bpath->realpath = pathptr;
  bpath->basename = NULL;
  bpath->path.head = bpath->include.head = bpath->exclude.head = NULL;
  bpath->listing = 0;

  *pathptr++ = '.';
  while (*path)
  {
    if ('/' != *path++)
    {
      syslog(LOG_DEBUG, "beholddb_parse_path: path is relative");
      beholddb_free_path(bpath);
      *pbpath = NULL;
      return BEHOLDDB_ERROR;
    }
    switch (*path)
    {
    default:
      if (beholddb_tagchar != *path)
      {
        const char *item = path;

        *pathptr++ = '/';
        bpath->basename = pathptr;
        while (*path && '/' != *path)
          *pathptr++ = *path++;
        beholddb_append_tag(&ptail, strrdup(item, path));
      } else
      do
      {
        // skip hash sign
        ++path;

        // handle hash directory
        while ('/' == *path)
          ++path;

        // handle empty tag
        if (!*path)
        {
          bpath->listing = 1;
          break;
        }

        // handle tag type
        beholddb_tag_list *list = '-' == *path ? ++path, &bpath->exclude : &bpath->include;
        const char *tag = path;

        // handle tag name
        while (*path && '/' != *path && beholddb_tagchar != *path)
          ++path;

        // add new tag to the list
        beholddb_insert_tag(&list->head, strrdup(tag, path));

      } while (beholddb_tagchar == *path);

    case '/':
      ; // remove redundant slashes
    case 0:
      ; // remove trailing slash
    }
  }

  // null terminate path
  *pathptr++ = 0;

  syslog(LOG_DEBUG, "beholddb_parse_path: realpath=%s", bpath->realpath);
  *pbpath = bpath;
  return BEHOLDDB_OK;
}

int beholddb_free_path(beholddb_path *bpath)
{
  syslog(LOG_DEBUG, "beholddb_free_path(realpath=%s)", bpath->realpath);

  beholddb_free_tag_list(&bpath->include);
  beholddb_free_tag_list(&bpath->exclude);
  beholddb_free_tag_list(&bpath->path);

  free((char*)bpath->realpath);
  free(bpath);
}

typedef struct beholddb_tag_context
{
  beholddb_tag_list_item *notpresent;
  int init: 1;
  int isdir: 1;
  int present: 1;
} beholddb_tag_context;

typedef sqlite3_int64 beholddb_object;

static void beholddb_tags_step(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
  beholddb_tag_list_set *tags = (beholddb_tag_list_set*)sqlite3_value_blob(argv[0]);
  const char *tag = sqlite3_value_text(argv[1]);
  const int type = sqlite3_value_int(argv[2]);
  const char *name = argc >= 4 ? sqlite3_value_text(argv[3]) : NULL;
  beholddb_tag_context *tctx =
    (beholddb_tag_context*)sqlite3_aggregate_context(ctx, sizeof(beholddb_tag_context));

  syslog(LOG_DEBUG, "beholddb_tags_step(argc=%d, tags=%p, tag=%s, name=%s)", argc, tags, tag, name);

  // initialize a copy of inclusive tags' list
  if (!tctx->init)
  {
    tctx->notpresent = beholddb_shadow_tag_list(tags->include.head);
    tctx->init = 1;
  }

  if (BEHOLDDB_TYPE_DIRECTORY == type)
    tctx->isdir = 1;

  if (BEHOLDDB_TYPE_FILE != type || !tag)
    return;

  // TODO: optimize !!!
  for (beholddb_tag_list_item **pinclude = &tctx->notpresent; *pinclude; pinclude = &(*pinclude)->next)
    if (!strcmp(tag, (*pinclude)->name))
    {
      beholddb_remove_tag(pinclude);
      break;
    }
  for (beholddb_tag_list_item *exclude = tags->exclude.head; exclude; exclude = exclude->next)
    if (!strcmp(tag, exclude->name))
    {
      ++tctx->present;
      break;
    }
}

static void beholddb_tags_final(sqlite3_context *ctx)
{
  beholddb_tag_context *tctx =
    (beholddb_tag_context*)sqlite3_aggregate_context(ctx, sizeof(beholddb_tag_context));
  int present, notpresent;

  if (tctx->init)
  {
    present = tctx->present;
    notpresent = !!tctx->notpresent;
    beholddb_remove_tag_list(tctx->notpresent);
  } else
    present = notpresent = -1;

  syslog(LOG_DEBUG, "beholddb_tags_final(present=%d, notpresent=%d)",
    present, notpresent);

  // positive result if:
  // - no exclusive tags are present
  // and
  // - no inclusive tags are not present
  sqlite3_result_int(ctx, !present && !notpresent);
}

static void beholddb_include_exclude(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
  int include = *(const int *const)sqlite3_user_data(ctx);
  beholddb_tag_list_set *tags = (beholddb_tag_list_set*)sqlite3_value_blob(argv[0]);
  beholddb_tag_list *list = include ? &tags->include : &tags->exclude;
  const char *tag = sqlite3_value_text(argv[1]);
  int found = 0;

  // TODO: optimize!!!
  for (beholddb_tag_list_item *item = list->head; item; item = item->next)
    if (!strcmp(tag, item->name))
    {
      found = 1;
      break;
    }
  sqlite3_result_int(ctx, found);
}

static const int BEHOLDDB_INCLUDE = 1;
static const int BEHOLDDB_EXCLUDE = 0;

static int beholddb_init(sqlite3 *db)
{
  int rc;

  //sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, 1, NULL);
  (rc = beholddb_exec(db, "pragma foreign_keys = on")) ||
  (rc = sqlite3_extended_result_codes(db, 1)) ||
  (rc = sqlite3_create_function_v2(db, "tags", 3, SQLITE_ANY, NULL, NULL, beholddb_tags_step, beholddb_tags_final, NULL)) ||
  (rc = sqlite3_create_function_v2(db, "tags", 4, SQLITE_ANY, NULL, NULL, beholddb_tags_step, beholddb_tags_final, NULL)) ||
  (rc = sqlite3_create_function_v2(db, "include", 2, SQLITE_ANY, (void*)&BEHOLDDB_INCLUDE, beholddb_include_exclude, NULL, NULL, NULL)) ||
  (rc = sqlite3_create_function_v2(db, "exclude", 2, SQLITE_ANY, (void*)&BEHOLDDB_EXCLUDE, beholddb_include_exclude, NULL, NULL, NULL));

  syslog(LOG_DEBUG, "beholddb_init: %d", rc);
  return rc;
}

static int beholddb_open_read(sqlite3 **pdb)
{
  syslog(LOG_DEBUG, "beholddb_open_read()");

  int rc;

  // open and initialize the database
  if ((rc = sqlite3_open_v2(BEHOLDDB_NAME, pdb, SQLITE_OPEN_READONLY, NULL)) ||
    (rc = beholddb_init(*pdb)))
  {
    syslog(LOG_ERR, "beholddb_open_read error: rc=%d", rc);
    sqlite3_close(*pdb);
    *pdb = NULL;
  }
  return rc;
}

static int beholddb_open_write(sqlite3 **pdb)
{
  syslog(LOG_DEBUG, "beholddb_open_write()");

  int rc;

  // open the database
  (rc = sqlite3_open_v2(BEHOLDDB_NAME, pdb, SQLITE_OPEN_READWRITE, NULL)) ||
  (rc = beholddb_init(*pdb));
  if (SQLITE_CANTOPEN == rc)
  {
    syslog(LOG_INFO, "beholddb_open_write: create metadata file");
    sqlite3_close(*pdb);
    (rc = sqlite3_open_v2(BEHOLDDB_NAME, pdb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) ||
    (rc = beholddb_init(*pdb)) ||
    (rc = schema_create(*pdb));
  }
  if (rc)
  {
    syslog(LOG_ERR, "beholddb_open_write error: rc=%d", rc);
    sqlite3_close(*pdb);
    *pdb = NULL;
  }
  return rc;
}

// TODO: there should not be object type
static int beholddb_get_object(sqlite3 *db, int type, const char *name, beholddb_object id_parent, beholddb_object *pid)
{
  int rc;
  beholddb_object id = -1;
  sqlite3_stmt *stmt;
  const char *sql;

  {
    sql =
      "select id "
      "from objects "
      "where name = @name "
      "and type = @type "
      "and id_parent is @id_parent ";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_text(stmt, "@name", name)) ||
    (rc = beholddb_bind_int(stmt, "@type", type)) ||
    (rc = beholddb_bind_int64_null(stmt, "@id_parent", id_parent)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_ROW == rc)
    {
      id = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
  }

  if (pid)
    *pid = id;
  switch (rc)
  {
  case SQLITE_ROW:
    // row returned - object found
    return BEHOLDDB_OK;
  case SQLITE_DONE:
    // no rows returned - object not found
    return BEHOLDDB_NOT_FOUND;
  default:
    // other error
    return BEHOLDDB_ERROR;
  }
}

// TODO: there should not be object type
static int beholddb_create_object(sqlite3 *db, int type, const char *name, beholddb_object id_parent, beholddb_object *pid)
{
  int rc;
  beholddb_object id = -1;
  sqlite3_stmt *stmt;
  const char *sql;//, *sp = "create";

  //rc = beholddb_begin(db, sp);
  //if (!rc)
  {
    sql =
      "insert into objects "
      "( name, type, id_parent ) "
      "values "
      "( @name, @type, @id_parent )";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_text(stmt, "@name", name)) ||
    (rc = beholddb_bind_int(stmt, "@type", type)) ||
    (rc = beholddb_bind_int64_null(stmt, "@id_parent", id_parent)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_DEBUG, "beholddb_create_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    id = sqlite3_last_insert_rowid(db);
    sql =
      "insert into objects_owners "
      "( id_owner, id_object ) "
      "select @id, @id "
      "union "
      "select id_owner, @id "
      "from objects_owners "
      "where id_object is @id_parent  ";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = beholddb_bind_int64_null(stmt, "@id_parent", id_parent)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_DEBUG, "beholddb_create_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (pid)
    *pid = id;
  switch (rc)
  {
  case SQLITE_DONE:
    // object was inserted
    return BEHOLDDB_OK;
  case SQLITE_CONSTRAINT:
    // object already exists
    return BEHOLDDB_EXISTS;
  default:
    // other error
    return BEHOLDDB_ERROR;
  }
  //return beholddb_end_result(db, sp, rc);
}

static int beholddb_create_object_path(sqlite3 *db, int type, const beholddb_tag_list *path, beholddb_object *pid)
{
  int rc;
  beholddb_object id = -1;
  sqlite3_stmt *stmt;
  const char *sql;
  static const char *sp = "create_path";

  rc = beholddb_begin(db, sp);
  if (!rc)
  {
    sql =
      "select id "
      "from objects "
      "where name is '/' "
      "and type = @type";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int(stmt, "@type", BEHOLDDB_TYPE_DIRECTORY)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_ROW == rc)
    {
      id = sqlite3_column_int(stmt, 0);
    } else
    {
      syslog(LOG_DEBUG, "beholddb_create_object_path: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_ROW == rc)
  {
    rc = 0;
    for (beholddb_tag_list_item *item = path->head; !rc && item; item = item->next)
    {
      int parentid = id, objtype = item->next ? BEHOLDDB_TYPE_DIRECTORY : type;

      rc = beholddb_create_object(db, objtype, item->name, parentid, &id);
      if (BEHOLDDB_EXISTS == rc)
        rc = beholddb_get_object(db, objtype, item->name, parentid, &id);
    }
    // FIXME: different types of release codes are used (sqlite/beholddb)!
  }
  if (pid)
    *pid = id;
  return beholddb_end_result(db, sp, rc);
}

static int beholddb_locate_object_path(sqlite3 *db, const beholddb_path *bpath, beholddb_object *pid)
{
  int rc, visible = -1;
  beholddb_object id = -1;
  sqlite3_stmt *stmt;
  char *sql, *mem;
  //const char *sp = "get";

  //rc = beholddb_begin(db, sp);
  //if (!rc)
  {
    // TODO: REMINDER: name='/' is a special root object
    sql = mem = sqlite3_mprintf(
      "o.name is '/' and o.type = %d",
      BEHOLDDB_TYPE_DIRECTORY);
    for (beholddb_tag_list_item *item = bpath->path.head; item; item = item->next)
    {
      sql = sqlite3_mprintf(
        "o.id_parent = ( select o.id from objects o where %s ) and o.name = '%q'",
        sql, item->name);
      sqlite3_free(mem);
      mem = sql;
    }
    sql = sqlite3_mprintf(
      "select o.id, tags(@tags, t.name, ooo.type, o.name) "
      "from objects o "
      "join objects_owners oo on oo.id_owner = o.id "
      "join objects ooo on ooo.id = oo.id_object "
      "left join objects_tags ot on ot.id_object = oo.id_object "
      "left join objects_owners tt on tt.id_object = ot.id_tag "
      "left join objects t on t.id = tt.id_owner "
      "where %s "
      "group by o.id ",
      sql);
    sqlite3_free(mem);
    syslog(LOG_DEBUG, "beholddb_locate_object_path: sql=%s", sql);

    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_blob(stmt, "@tags", &bpath->tags, sizeof(beholddb_tag_list_set), SQLITE_STATIC)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_ROW == rc)
    {
      id = sqlite3_column_int64(stmt, 0);
      visible = sqlite3_column_int(stmt, 1);
      syslog(LOG_DEBUG, "beholddb_locate_object_path: id=%lld, visible=%d", id, visible);
    } else
    {
      syslog(LOG_DEBUG, "beholddb_locate_object_path: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
    sqlite3_free(sql);
  }

  //beholddb_end(db, sp);
  if (pid)
    *pid = id;
  switch (rc)
  {
  case SQLITE_DONE:
    // no rows returned - object not found
    return BEHOLDDB_NOT_FOUND;
  case SQLITE_ROW:
    // row returned - object found, check tags
    return visible ? BEHOLDDB_OK : BEHOLDDB_HIDDEN;
  default:
    // other errors
    return BEHOLDDB_ERROR;
  }
}

static int beholddb_delete_object(sqlite3 *db, beholddb_object id)
{
  int rc;
  sqlite3_stmt *stmt;
  const char *sql;//, *sp = "delete";

  //rc = beholddb_begin(db, sp);
  //if (!rc)
  {
    sql =
      "delete from objects "
      "where id = @id";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = sqlite3_step(stmt));
    sqlite3_finalize(stmt);
  }
  return SQLITE_DONE == rc ? BEHOLDDB_OK : BEHOLDDB_ERROR;
  //return beholddb_end_result(db, sp, rc);
}

static int beholddb_set_object_parent(sqlite3 *db, beholddb_object id, beholddb_object id_parent)
{
  int rc;
  sqlite3_stmt *stmt;
  const char *sql;
  static const char *sp = "parent";

  (rc = beholddb_begin(db, sp));

  if (!rc)
  {
    sql =
      "delete from objects_owners "
      "where id_owner in ( "
      "select id_owner "
      "from objects_owners "
      "where id_object = "
      "( select id_parent from objects where id = @id ) ) "
      "and id_object in ( "
      "select id_object "
      "from objects_owners "
      "where id_owner = "
      "( select id from objects where id = @id ) ) ";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = sqlite3_step(stmt));
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    sql =
      "insert into objects_owners "
      "( id_owner, id_object ) "
      "select own.id_owner, obj.id_object "
      "from objects_owners own, objects_owners obj "
      "where own.id_object = @id_parent "
      "and obj.id_owner = @id ";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = beholddb_bind_int64(stmt, "@id_parent", id_parent)) ||
    (rc = sqlite3_step(stmt));
    sqlite3_finalize(stmt);
  }

  return beholddb_end_result(db, sp, rc);
}

// TODO: tags should be object ids, not names?
static int beholddb_link_object(sqlite3 *db, beholddb_object id, int type, const beholddb_tag_list_set *tags)
{
  int rc;
  sqlite3_stmt *stmt;
  const char *sql;
  static const char *sp = "link";

  (rc = beholddb_begin(db, sp));

  if (!rc)
  {
    sql =
      "delete from objects_tags "
      "where id_object = @id "
      "and exclude(@tags, (select name from objects o where o.id = objects_tags.id_tag)) ";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = beholddb_bind_blob(stmt, "@tags", tags, sizeof(beholddb_tag_list_set), SQLITE_STATIC)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_ERR, "beholddb_link_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    // TODO: do cleanup after creating new tags?
    sql =
      "delete from objects "
      "where type = @type "
      "and not exists(select 1 from objects_tags ot where ot.id_tag = objects.id) "
      "and not include(@tags, objects.name)";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int(stmt, "@type", type)) ||
    (rc = beholddb_bind_blob(stmt, "@tags", tags, sizeof(beholddb_tag_list_set), SQLITE_STATIC)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_ERR, "beholddb_link_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    const char *sql_object =
      "insert into objects "
      "( name, type, id_parent ) "
      "values "
      "( @name, @type, @id_parent ) ";
    const char *sql_owner =
      "insert into objects_owners "
      "( id_object, id_owner ) "
      "values "
      "( @id_tag, @id_tag ) ";
    const char *sql_tag =
      "insert into objects_tags "
      "( id_object, id_tag ) "
      "values "
      "( @id, @id_tag ) ";

    sqlite3_stmt *stmt_object = NULL, *stmt_owner = NULL, *stmt_tag = NULL;

    (rc = sqlite3_prepare_v2(db, sql_object, -1, &stmt_object, NULL)) ||
    (rc = sqlite3_prepare_v2(db, sql_owner, -1, &stmt_owner, NULL)) ||
    (rc = sqlite3_prepare_v2(db, sql_tag, -1, &stmt_tag, NULL)) ||

    (rc = beholddb_bind_int(stmt_object, "@type", type)) ||
    (rc = beholddb_bind_null(stmt_object, "@id_parent")) ||
    (rc = beholddb_bind_int64(stmt_tag, "@id", id)) ||
    (rc = SQLITE_DONE);

    const int i_object_name = sqlite3_bind_parameter_index(stmt_object, "@name");
    const int i_owner_id = sqlite3_bind_parameter_index(stmt_owner, "@id_tag");
    const int i_tag_id = sqlite3_bind_parameter_index(stmt_tag, "@id_tag");

    for (beholddb_tag_list_item *item = tags->include.head; SQLITE_DONE == rc && item; item = item->next)
    {
      beholddb_object id_tag;

      (rc = sqlite3_reset(stmt_object)) ||
      (rc = sqlite3_bind_text(stmt_object, i_object_name, item->name, -1, SQLITE_STATIC)) ||
      SQLITE_DONE != (rc = sqlite3_step(stmt_object)) ||
      (id_tag = sqlite3_last_insert_rowid(db));

      switch (rc)
      {
      case SQLITE_DONE:
        // make a newly created tag its owner
        (rc = sqlite3_reset(stmt_owner)) ||
        (rc = sqlite3_bind_int64(stmt_owner, i_owner_id, id_tag)) ||
        (rc = sqlite3_step(stmt_owner));
        if (SQLITE_DONE != rc)
          break;
        // fall through
      case SQLITE_CONSTRAINT:
        // link the object with the tag
        (rc = sqlite3_reset(stmt_tag)) ||
        (rc = sqlite3_bind_int64(stmt_tag, i_tag_id, id_tag)) ||
        (rc = sqlite3_step(stmt_tag));
        break;
      default:
        syslog(LOG_ERR, "beholddb_link_object: %s, rc=%d", sqlite3_errmsg(db), rc);
      }
    }

    if (SQLITE_DONE != rc)
    {
      syslog(LOG_ERR, "beholddb_link_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }

    sqlite3_finalize(stmt_object);
    sqlite3_finalize(stmt_owner);
    sqlite3_finalize(stmt_tag);
  }

  return beholddb_end_result(db, sp, rc);
}

static int beholddb_open_object(sqlite3 *db, beholddb_object id, const beholddb_tag_list_set *tags, beholddb_iterator **pit)
{
  int rc;
  sqlite3_stmt *stmt;
  const char *sql;
  static const char *sp = "list";

  *pit = NULL;
  rc = beholddb_begin(db, sp);

  if (!rc)
  {
    sql =
      "create temp table fast_objects "
      "( id integer primary key, name text unique )";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_ERR, "beholddb_open_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    sql =
      "insert into fast_objects ( id, name ) "
      "select o.id, o.name "
      "from objects o "
      "join objects_owners oo on oo.id_owner = o.id "
      "join objects ooo on ooo.id = oo.id_object "
      "left join objects_tags ot on ot.id_object = oo.id_object "
      "left join objects_owners tt on tt.id_object = ot.id_tag "
      "left join objects t on t.id = tt.id_owner "
      "where o.id_parent = @id "
      "group by o.id "
      "having tags(@tags, t.name, ooo.type, o.name) ";

    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = beholddb_bind_blob(stmt, "@tags", tags, sizeof(beholddb_tag_list_set), SQLITE_STATIC)) ||
    (rc = sqlite3_step(stmt));
    if (SQLITE_DONE != rc)
    {
      syslog(LOG_ERR, "beholddb_open_object: %s, rc=%d", sqlite3_errmsg(db), rc);
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    sql = "select id, name from fast_objects";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL));
    while (SQLITE_ROW == (rc = sqlite3_step(stmt)))
    {
      syslog(LOG_DEBUG, "beholddb_open_object: read id=%d, name='%s'", sqlite3_column_int(stmt, 0), sqlite3_column_text(stmt, 1));
    }
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    *pit = (beholddb_iterator*)sqlite3_malloc(sizeof(beholddb_iterator));
    (*pit)->db = db;
    (*pit)->sp = sp;
    (*pit)->stmt = NULL;
    return BEHOLDDB_OK;
  }

  beholddb_end(db, sp);
  return BEHOLDDB_ERROR;
}

static int beholddb_open_links(sqlite3 *db, beholddb_object id, beholddb_iterator **pit)
{
  int rc;
  sqlite3_stmt *stmt;
  const char *sql;
  static const char *sp = "links";

  *pit = NULL;
  rc = beholddb_begin(db, sp);

  if (!rc)
  {
    sql =
      "create temp table fast_objects "
      "( id integer primary key, name text unique )";
    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = sqlite3_step(stmt));
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    sql =
      "insert into fast_objects ( id, name ) "
      "select t.id, t.name "
      "from objects_tags ot "
      "join objects t on t.id = ot.id_tag "
      "where ot.id_object = @id ";

    (rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) ||
    (rc = beholddb_bind_int64(stmt, "@id", id)) ||
    (rc = sqlite3_step(stmt));
    sqlite3_finalize(stmt);
  }

  if (SQLITE_DONE == rc)
  {
    *pit = (beholddb_iterator*)sqlite3_malloc(sizeof(beholddb_iterator));
    (*pit)->db = db;
    (*pit)->sp = sp;
    (*pit)->stmt = NULL;
    return BEHOLDDB_OK;
  }

  beholddb_end(db, sp);
  return BEHOLDDB_ERROR;
}

static int beholddb_item_next(beholddb_iterator *it, const char **pitem)
{
  if (!it->stmt)
  {
    int rc;
    const char *sql =
      "select name from fast_objects ";

    (rc = sqlite3_prepare_v2(it->db, sql, -1, &it->stmt, NULL));
    if (rc)
    {
      *pitem = NULL;
      return BEHOLDDB_ERROR;
    }
  }

  switch (sqlite3_step(it->stmt))
  {
  case SQLITE_ROW:
    *pitem = sqlite3_column_text(it->stmt, 0);
    return BEHOLDDB_OK;
  case SQLITE_DONE:
    *pitem = NULL;
    return BEHOLDDB_DONE;
  default:
    *pitem = NULL;
    return BEHOLDDB_ERROR;
  }
}

static int beholddb_item_check(beholddb_iterator *it, const char *item)
{
  int rc;

  if (!it->stmt)
  {
    const char *sql =
      "select 1 from fast_objects "
      "where name = @name ";

    (rc = sqlite3_prepare_v2(it->db, sql, -1, &it->stmt, NULL));
  } else
  {
    (rc = sqlite3_reset(it->stmt));
  }

  if (rc)
    syslog(LOG_DEBUG, "beholddb_item_check: %s, rc=%d", sqlite3_errmsg(it->db), rc);

  if (rc || (rc = beholddb_bind_text(it->stmt, "@name", item)))
    return BEHOLDDB_ERROR;

  rc = sqlite3_step(it->stmt);

  syslog(LOG_DEBUG, "beholddb_item_check: item='%s', rc=%d", item, rc);

  switch (rc)
  {
  case SQLITE_ROW:
    return BEHOLDDB_OK;
  case SQLITE_DONE:
    return BEHOLDDB_HIDDEN;
  default:
    return BEHOLDDB_ERROR;
  }
}

int beholddb_close_object(beholddb_iterator *it)
{
  if (it)
  {
    sqlite3_finalize(it->stmt);
    beholddb_rollback(it->db, it->sp);
    sqlite3_free(it);
  }
  return BEHOLDDB_OK;
}

int beholddb_locate_file(const beholddb_path *bpath)
{
  syslog(LOG_DEBUG, "beholddb_locate_file(realpath=%s)", bpath->realpath);

  if (!bpath->basename)
  {
    syslog(LOG_INFO, "beholddb_locate_file: no base name in %s", bpath->realpath);
    return BEHOLDDB_OK;
  }

  if (bpath->listing)
  {
    syslog(LOG_DEBUG, "beholddb_locate_file: listing");
    return BEHOLDDB_OK;
  }

  // quick optimization
  if (!bpath->include.head && !bpath->exclude.head)
  {
    syslog(LOG_DEBUG, "beholddb_locate_file: no filter, optimizing");
    return BEHOLDDB_OK;
  }

  int rc;
  sqlite3 *db;

  (rc = beholddb_open_read(&db)) ||
  (rc = beholddb_locate_object_path(db, bpath, NULL));

  sqlite3_close(db);

  if (rc < 0)
    syslog(LOG_ERR, "beholddb_locate_file: error %d", rc);
  return rc;
}

int beholddb_create_file(const beholddb_path *bpath, int type)
{
  syslog(LOG_DEBUG, "beholddb_create_file(realpath=%s)", bpath->realpath);

  int rc;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  beholddb_object id;
  beholddb_iterator *it;

  if ((rc = beholddb_open_write(&db)))
    return rc;

  (rc = beholddb_create_object_path(db, type, &bpath->path, &id)) ||
  (rc = beholddb_link_object(db, id, BEHOLDDB_TYPE_TAG, &bpath->tags));
  sqlite3_close(db);

  if (rc)
    syslog(LOG_ERR, "beholddb_create_file: error %d", rc); else
    syslog(LOG_DEBUG, "beholddb_create_file: ok");
  return rc;
}

int beholddb_delete_file(const beholddb_path *bpath)
{
  syslog(LOG_DEBUG, "beholddb_delete_file(realpath=%s)", bpath->realpath);

  int rc;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  beholddb_object id;
  beholddb_iterator *it;

  if ((rc = beholddb_open_write(&db)))
    return rc;

  (rc = beholddb_locate_object_path(db, bpath, &id)) ||
  (rc = beholddb_delete_object(db, id));
  sqlite3_close(db);

  if (rc)
    syslog(LOG_ERR, "beholddb_delete_file: error %d", rc); else
    syslog(LOG_DEBUG, "beholddb_delete_file: ok");
  return rc;
}

int beholddb_rename_file(const beholddb_path *oldbpath, const beholddb_path *newbpath)
{
  // TODO: implement renaming
  //beholddb_delete_file(oldbpath, &files_tags, &dirs_tags, &type);
  //beholddb_create_file(newbpath, &files_tags, &dirs_tags, type);
  return BEHOLDDB_ERROR; // TODO: error handling
}

int beholddb_opendir(const beholddb_path *bpath, void **phandle)
{
  syslog(LOG_DEBUG, "beholddb_opendir(realpath=%s)", bpath->realpath);

  *phandle = NULL;

  if (!bpath->listing && !bpath->include.head && !bpath->exclude.head)
  {
    syslog(LOG_DEBUG, "beholddb_opendir: no filter, optimizing");
    return BEHOLDDB_OK;
  }

  int rc;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  beholddb_object id;
  beholddb_iterator *it;

  if ((rc = beholddb_open_read(&db)))
    return rc;

  // FIXME: add listing support
  (rc = beholddb_locate_object_path(db, bpath, &id))
    && BEHOLDDB_HIDDEN != rc ||
  (rc = beholddb_open_object(db, id, &bpath->tags, &it));
  if (rc)
  {
    sqlite3_close(db);
  } else
  {
    beholddb_dir *dir = (beholddb_dir*)malloc(sizeof(beholddb_dir));

    dir->db = db;
    dir->it = it;
    *phandle = (void*)dir;
  }

  if (rc)
    syslog(LOG_ERR, "beholddb_opendir: error %d", rc); else
    syslog(LOG_DEBUG, "beholddb_opendir: ok, handle=%p", *phandle);
  return rc;
}

int beholddb_opentags(const beholddb_path *bpath, void **phandle)
{
  syslog(LOG_DEBUG, "beholddb_opentags(realpath=%s)", bpath->realpath);

  *phandle = NULL;

  int rc;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  beholddb_object id;
  beholddb_iterator *it;

  if ((rc = beholddb_open_read(&db)))
    return rc;

  (rc = beholddb_locate_object_path(db, bpath, &id)) ||
  (rc = beholddb_open_links(db, id, &it));
  if (rc)
  {
    sqlite3_close(db);
  } else
  {
    beholddb_dir *dir = (beholddb_dir*)malloc(sizeof(beholddb_dir));

    dir->db = db;
    dir->it = it;
    *phandle = (void*)dir;
  }

  if (rc)
    syslog(LOG_ERR, "beholddb_opentags: error %d", rc); else
    syslog(LOG_DEBUG, "beholddb_opentags: ok, handle=%p", *phandle);
  return rc;
}

int beholddb_readdir(void *handle, const char *name)
{
  int rc;
  beholddb_dir *dir = (beholddb_dir*)handle;

  syslog(LOG_DEBUG, "beholddb_readdir: dir=%p", dir);
  if (dir)
    rc = beholddb_item_check(dir->it, name); else
    rc = BEHOLDDB_OK; // FIXME: strcmp(name, BEHOLDDB_NAME) ? BEHOLDDB_OK : BEHOLDDB_ERROR;
  return rc;
}

int beholddb_listdir(void *handle, const char **pname)
{
  syslog(LOG_DEBUG, "beholddb_listdir()");

  int rc;
  beholddb_dir *dir = (beholddb_dir*)handle;

  if (dir)
    rc = beholddb_item_next(dir->it, pname); else
    rc = BEHOLDDB_ERROR;
  return rc;
}

int beholddb_closedir(void *handle)
{
  beholddb_dir *dir = (beholddb_dir*)handle;

  if (!dir)
    return BEHOLDDB_OK;

  beholddb_close_object(dir->it);
  sqlite3_close(dir->db);
  free(dir);
  return BEHOLDDB_OK;
}

