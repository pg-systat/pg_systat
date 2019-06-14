/*
 * Copyright (c) 2019 PostgreSQL Global Development Group
 */

#include <stdlib.h>
#ifdef __linux__
#include <bsd/stdlib.h>
#include <bsd/sys/tree.h>
#endif /* __linux__ */
#include <limits.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/vfs.h>
#include <errno.h>

#include "pg.h"
#include "pgstat.h"

#define QUERY_STAT_DBFS \
		"SELECT spcname,\n" \
		"       coalesce(nullif(pg_tablespace_location(oid), ''),\n" \
		"                current_setting('data_directory'))\n" \
		"FROM pg_tablespace;"

struct dbfs_t
{
	RB_ENTRY(dbfs_t) entry;
	char spcname[NAMEDATALEN + 1];
	char path[PATH_MAX];
	struct statfs buf;
};

int dbfscmp(struct dbfs_t *, struct dbfs_t *);
static void dbfs_info(void);
void print_dbfs(void);
int read_dbfs(void);
int select_dbfs(void);
void sort_dbfs(void);
int sort_dbfs_path_callback(const void *, const void *);
int sort_dbfs_spcname_callback(const void *, const void *);

RB_HEAD(dbfs, dbfs_t) head_dbfss = RB_INITIALIZER(&head_dbfss);
RB_PROTOTYPE(dbfs, dbfs_t, entry, dbfscmp)
RB_GENERATE(dbfs, dbfs_t, entry, dbfscmp)

field_def fields_dbfs[] = {
	{ "TABLESPACE", 11, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "PATH", 5, PATH_MAX, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "USED", 5, 5, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "AVAILABLE", 10, 5, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "%USED", 6, 5, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_DBFS_SPCNAME     FIELD_ADDR(fields_dbfs, 0)
#define FLD_DBFS_PATH        FIELD_ADDR(fields_dbfs, 1)
#define FLD_DBFS_BLOCKS      FIELD_ADDR(fields_dbfs, 2)
#define FLD_DBFS_BAVAIL      FIELD_ADDR(fields_dbfs, 3)
#define FLD_DBFS_BLOCKS_PER  FIELD_ADDR(fields_dbfs, 4)

/* Define views */
field_def *view_dbfs_0[] = {
	FLD_DBFS_SPCNAME, FLD_DBFS_PATH, FLD_DBFS_BLOCKS, FLD_DBFS_BAVAIL,
	FLD_DBFS_BLOCKS_PER, NULL
};

order_type dbfs_order_list[] = {
	{"tablespace", "tablespace", 't', sort_dbfs_spcname_callback},
	{"path", "path", 'p', sort_dbfs_path_callback},
	{"used", "used", 'u', sort_dbfs_path_callback},
	{"available", "available", 'a', sort_dbfs_path_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager dbfs_mgr = {
	"dbfs", select_dbfs, read_dbfs, sort_dbfs, print_header,
	print_dbfs, keyboard_callback, dbfs_order_list, dbfs_order_list
};

field_view views_dbfs[] = {
	{ view_dbfs_0, "dbfs", 'D', &dbfs_mgr },
	{ NULL, NULL, 0, NULL }
};

int	dbfs_count;
struct dbfs_t *dbfss;

static void
dbfs_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct dbfs_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_DBFS);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = dbfs_count;
			dbfs_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (dbfs_count > i) {
		p = reallocarray(dbfss, dbfs_count, sizeof(struct dbfs_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		dbfss = p;
	}

	for (i = 0; i < dbfs_count; i++) {
		n = malloc(sizeof(struct dbfs_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}

		strncpy(n->spcname, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(dbfs, &head_dbfss, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->path, PQgetvalue(pgresult, i, 1), PATH_MAX);

		if (statfs(n->path, &n->buf) != 0)
			error("%s statfs error: %d", n->path, errno);

		memcpy(&dbfss[i], n, sizeof(struct dbfs_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
dbfscmp(struct dbfs_t *e1, struct dbfs_t *e2)
{
	return strcmp(e1->spcname, e2->spcname);
}

int
select_dbfs(void)
{
	return (0);
}

int
read_dbfs(void)
{
	dbfs_info();
	num_disp = dbfs_count;
	return (0);
}

int
initdbfs(void)
{
	field_view	*v;

	dbfss = NULL;
	dbfs_count = 0;

	for (v = views_dbfs; v->name != NULL; v++)
		add_view(v);

	read_dbfs();

	return(1);
}

void
print_dbfs(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < dbfs_count; i++) {
		do {
			unsigned int blocks_per;
			blocks_per = dbfss[i].buf.f_blocks == 0 ? 1 :
					100 * (dbfss[i].buf.f_blocks - dbfss[i].buf.f_bavail) /
							dbfss[i].buf.f_blocks;

			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_DBFS_SPCNAME, dbfss[i].spcname);
				print_fld_str(FLD_DBFS_PATH, dbfss[i].path);
				print_fld_str(FLD_DBFS_BLOCKS,
						format_b(dbfss[i].buf.f_blocks *
								dbfss[i].buf.f_bsize));
				print_fld_str(FLD_DBFS_BAVAIL,
						format_b(dbfss[i].buf.f_bavail *
								dbfss[i].buf.f_bsize));
				print_fld_uint(FLD_DBFS_BLOCKS_PER, blocks_per);
				end_line();
			}
			if (++cur >= end)
				return;
		} while (0);
	}

	do {
		if (cur >= dispstart && cur < end)
			end_line();
		if (++cur >= end)
			return;
	} while (0);
}

void
sort_dbfs(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (dbfss == NULL)
		return;
	if (dbfs_count <= 0)
		return;

	mergesort(dbfss, dbfs_count, sizeof(struct dbfs_t), ordering->func);
}

int
sort_dbfs_path_callback(const void *v1, const void *v2)
{
	struct dbfs_t *n1, *n2;
	n1 = (struct dbfs_t *) v1;
	n2 = (struct dbfs_t *) v2;

	int ret = strcmp(n1->path, n2->path);

	if (ret < 0)
		return sortdir;
	if (ret > 0)
		return -sortdir;

	return sort_dbfs_spcname_callback(v1, v2);
}

int
sort_dbfs_spcname_callback(const void *v1, const void *v2)
{
	struct dbfs_t *n1, *n2;
	n1 = (struct dbfs_t *) v1;
	n2 = (struct dbfs_t *) v2;

	return strcmp(n1->spcname, n2->spcname) * sortdir;
}
