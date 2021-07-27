/*
 * Copyright (c) 2021 PostgreSQL Global Development Group
 */

#include <stdint.h>
#include <stdlib.h>
#ifdef __linux__
#include <bsd/stdlib.h>
#include <bsd/sys/tree.h>
#endif /* __linux__ */
#include <string.h>
#include <unistd.h>
#include <signal.h>

#include "pg.h"
#include "pg_systat.h"

#define QUERY_BUFFERCACHEREL \
		"SELECT bufferid, relfilenode, reltablespace, reldatabase, relforknumber,\n" \
		"       relblocknumber\n" \
		"FROM pg_buffercache;"

struct buffercacherel_t
{
	RB_ENTRY(buffercacherel_t) entry;

	char bufferid[NAMEDATALEN+1];
	int64_t relfilenode;
	int64_t reltablespace;
	int64_t reldatabase;
	int64_t relforknumber;
	int64_t relblocknumber;

};

int buffercacherel_cmp(struct buffercacherel_t *, struct buffercacherel_t *);
static void buffercacherel_info(void);
void print_buffercacherel(void);
int read_buffercacherel(void);
int select_buffercacherel(void);
void sort_buffercacherel(void);
int sort_buffercacherel_bufferid_callback(const void *, const void *);
int sort_buffercacherel_relfilenode_callback(const void *, const void *);
int sort_buffercacherel_reltablespace_callback(const void *, const void *);
int sort_buffercacherel_reldatabase_callback(const void *, const void *);
int sort_buffercacherel_relforknumber_callback(const void *, const void *);
int sort_buffercacherel_relblocknumber_callback(const void *, const void *);

RB_HEAD(buffercacherel, buffercacherel_t) head_buffercacherels =
		RB_INITIALIZER(&head_buffercacherels);
RB_PROTOTYPE(buffercacherel, buffercacherel_t, entry, buffercacherel_cmp)
RB_GENERATE(buffercacherel, buffercacherel_t, entry, buffercacherel_cmp)

field_def fields_buffercacherel[] = {
	{ "BUFFERID", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "RELFILENODE", 12, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "RELTABLESPACE", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "RELDATABASE", 12, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "RELFORKNUMBER", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "RELBLOCKNUMBER", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_BUFFERID        FIELD_ADDR(fields_buffercacherel, 0)
#define FLD_STMT_RELFILENODE          FIELD_ADDR(fields_buffercacherel, 1)
#define FLD_STMT_RELTABLESPACE      FIELD_ADDR(fields_buffercacherel, 2)
#define FLD_STMT_RELDATABASE  FIELD_ADDR(fields_buffercacherel, 3)
#define FLD_STMT_RELFORKNUMBER      FIELD_ADDR(fields_buffercacherel, 4)
#define FLD_STMT_RELBLOCKNUMBER FIELD_ADDR(fields_buffercacherel, 5)

/* Define views */
field_def *view_buffercacherel_0[] = {
	FLD_STMT_BUFFERID, FLD_STMT_RELFILENODE, FLD_STMT_RELTABLESPACE,
	FLD_STMT_RELDATABASE, FLD_STMT_RELFORKNUMBER, FLD_STMT_RELBLOCKNUMBER, NULL
};

order_type buffercacherel_order_list[] = {
	{"bufferid", "bufferid", 'u', sort_buffercacherel_bufferid_callback},
	{"relfilenode", "relfilenode", 'f', sort_buffercacherel_relfilenode_callback},
	{"reltablespace", "reltablespace", 't', sort_buffercacherel_reltablespace_callback},
	{"reldatabase", "reldatabase", 'a',
			sort_buffercacherel_reldatabase_callback},
	{"relforknumber", "relforknumber", 'r', sort_buffercacherel_relforknumber_callback},
	{"relblocknumber", "relblocknumber", 'b', sort_buffercacherel_relblocknumber_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager buffercacherel_mgr = {
	"buffercacherel", select_buffercacherel, read_buffercacherel, sort_buffercacherel,
	print_header, print_buffercacherel, keyboard_callback, buffercacherel_order_list,
	buffercacherel_order_list
};

field_view views_buffercacherel[] = {
	{ view_buffercacherel_0, "buffercacherel", 'P', &buffercacherel_mgr },
	{ NULL, NULL, 0, NULL }
};

int	buffercacherel_count;
struct buffercacherel_t *buffercacherels;

static void
buffercacherel_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct buffercacherel_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_BUFFERCACHEREL);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = buffercacherel_count;
			buffercacherel_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (buffercacherel_count > i) {
		p = reallocarray(buffercacherels, buffercacherel_count,
				sizeof(struct buffercacherel_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		buffercacherels = p;
	}

	for (i = 0; i < buffercacherel_count; i++) {
		n = malloc(sizeof(struct buffercacherel_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->bufferid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(buffercacherel, &head_buffercacherels, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->relfilenode = atoll(PQgetvalue(pgresult, i, 1));
		n->reltablespace = atoll(PQgetvalue(pgresult, i, 2));
		n->reldatabase = atoll(PQgetvalue(pgresult, i, 3));
		n->relforknumber = atoll(PQgetvalue(pgresult, i, 4));
		n->relblocknumber = atoll(PQgetvalue(pgresult, i, 5));

		memcpy(&buffercacherels[i], n, sizeof(struct buffercacherel_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
buffercacherel_cmp(struct buffercacherel_t *e1, struct buffercacherel_t *e2)
{
  return (e1->bufferid< e2->bufferid ? -1 : e1->bufferid > e2->bufferid);
}

int
select_buffercacherel(void)
{
	return (0);
}

int
read_buffercacherel(void)
{
	buffercacherel_info();
	num_disp = buffercacherel_count;
	return (0);
}

int
initbuffercacherel(void)
{
	field_view	*v;

	buffercacherels = NULL;
	buffercacherel_count = 0;

	for (v = views_buffercacherel; v->name != NULL; v++)
		add_view(v);
	read_buffercacherel();

	return(1);
}

void
print_buffercacherel(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < buffercacherel_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_BUFFERID, buffercacherels[i].bufferid);
						print_fld_uint(FLD_STMT_RELFILENODE,
						buffercacherels[i].relfilenode);
				print_fld_uint(FLD_STMT_RELTABLESPACE,
						buffercacherels[i].reltablespace);
				print_fld_uint(FLD_STMT_RELDATABASE,
						buffercacherels[i].reldatabase);
				print_fld_uint(FLD_STMT_RELFORKNUMBER,
						buffercacherels[i].relforknumber);
				print_fld_uint(FLD_STMT_RELBLOCKNUMBER,
						buffercacherels[i].relblocknumber);
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
sort_buffercacherel(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (buffercacherels == NULL)
		return;
	if (buffercacherel_count <= 0)
		return;

	mergesort(buffercacherels, buffercacherel_count, sizeof(struct buffercacherel_t),
			ordering->func);
}

int
sort_buffercacherel_bufferid_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	return strcmp(n1->bufferid, n2->bufferid) * sortdir;
}

int
sort_buffercacherel_relfilenode_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	if (n1->relfilenode < n2->relfilenode)
		return sortdir;
	if (n1->relfilenode > n2->relfilenode)
		return -sortdir;

	return sort_buffercacherel_bufferid_callback(v1, v2);
}

int
sort_buffercacherel_reltablespace_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	if (n1->reltablespace < n2->reltablespace)
		return sortdir;
	if (n1->reltablespace > n2->reltablespace)
		return -sortdir;

	return sort_buffercacherel_bufferid_callback(v1, v2);
}

int
sort_buffercacherel_reldatabase_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	if (n1->reldatabase < n2->reldatabase)
		return sortdir;
	if (n1->reldatabase > n2->reldatabase)
		return -sortdir;

	return sort_buffercacherel_bufferid_callback(v1, v2);
}

int
sort_buffercacherel_relforknumber_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	if (n1->relforknumber < n2->relforknumber)
		return sortdir;
	if (n1->relforknumber > n2->relforknumber)
		return -sortdir;

	return sort_buffercacherel_bufferid_callback(v1, v2);
}

int
sort_buffercacherel_relblocknumber_callback(const void *v1, const void *v2)
{
	struct buffercacherel_t *n1, *n2;
	n1 = (struct buffercacherel_t *) v1;
	n2 = (struct buffercacherel_t *) v2;

	if (n1->relblocknumber < n2->relblocknumber)
		return sortdir;
	if (n1->relblocknumber > n2->relblocknumber)
		return -sortdir;

	return sort_buffercacherel_bufferid_callback(v1, v2);
}
