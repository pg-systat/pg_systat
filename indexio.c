/*
 * Copyright (c) 2019 PostgreSQL Global Development Group
 */

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

#define QUERY_STAT_INDEXIOES \
		"SELECT indexrelid, schemaname, relname, indexrelname,\n" \
		"       idx_blks_read, idx_blks_hit\n" \
		"FROM pg_statio_all_indexes;"

struct indexio_t
{
	RB_ENTRY(indexio_t) entry;

	long long indexiorelid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];
	char indexiorelname[NAMEDATALEN + 1];

	int64_t idx_blks_read;
	int64_t idx_blks_read_diff;
	int64_t idx_blks_read_old;

	int64_t idx_blks_hit;
	int64_t idx_blks_hit_diff;
	int64_t idx_blks_hit_old;
};

int indexiocmp(struct indexio_t *, struct indexio_t *);
static void indexio_info(void);
void print_indexio(void);
int read_indexio(void);
int select_indexio(void);
void sort_indexio(void);
int sort_indexio_idx_blks_hit_callback(const void *, const void *);
int sort_indexio_idx_blks_read_callback(const void *, const void *);
int sort_indexio_indexiorelname_callback(const void *, const void *);
int sort_indexio_relname_callback(const void *, const void *);
int sort_indexio_schemaname_callback(const void *, const void *);

RB_HEAD(indexio, indexio_t) head_indexios =
		RB_INITIALIZER(&head_indexios);
RB_PROTOTYPE(indexio, indexio_t, entry, indexiocmp)
RB_GENERATE(indexio, indexio_t, entry, indexiocmp)

field_def fields_indexio[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "INDEXNAME", 10, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "TABLENAME", 10, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "BLKS_READ", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "BLKS_HIT", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_INDEXIO_SCHEMA         FIELD_ADDR(fields_indexio, 0)
#define FLD_INDEXIO_INDEXIORELNAME FIELD_ADDR(fields_indexio, 1)
#define FLD_INDEXIO_RELNAME        FIELD_ADDR(fields_indexio, 2)
#define FLD_INDEXIO_IDX_BLKS_READ  FIELD_ADDR(fields_indexio, 3)
#define FLD_INDEXIO_IDX_BLKS_HIT   FIELD_ADDR(fields_indexio, 4)

/* Define views */
field_def *view_indexio_0[] = {
	FLD_INDEXIO_SCHEMA, FLD_INDEXIO_INDEXIORELNAME, FLD_INDEXIO_RELNAME,
	FLD_INDEXIO_IDX_BLKS_READ, FLD_INDEXIO_IDX_BLKS_HIT, NULL
};

order_type indexio_order_list[] = {
	{"schema", "schema", 's', sort_indexio_schemaname_callback},
	{"indexioname", "indexioname", 'i', sort_indexio_indexiorelname_callback},
	{"tablename", "tablename", 't', sort_indexio_relname_callback},
	{"idx_blks_read", "idx_blks_read", 'r',
			sort_indexio_idx_blks_read_callback},
	{"idx_blks_hit", "idx_blks_hit", 'h',
			sort_indexio_idx_blks_hit_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager indexio_mgr = {
	"indexio", select_indexio, read_indexio, sort_indexio, print_header,
	print_indexio, keyboard_callback, indexio_order_list, indexio_order_list
};

field_view views_indexio[] = {
	{ view_indexio_0, "indexio", 'U', &indexio_mgr },
	{ NULL, NULL, 0, NULL }
};

int	indexio_count;
struct indexio_t *indexios;

static void
indexio_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct indexio_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_INDEXIOES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = indexio_count;
			indexio_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (indexio_count > i) {
		p = reallocarray(indexios, indexio_count, sizeof(struct indexio_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		indexios = p;
	}

	for (i = 0; i < indexio_count; i++) {
		n = malloc(sizeof(struct indexio_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->indexiorelid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(indexio, &head_indexios, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->indexiorelname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 3), NAMEDATALEN);

		n->idx_blks_read_old = n->idx_blks_read;
		n->idx_blks_read = atoll(PQgetvalue(pgresult, i, 4));
		n->idx_blks_read_diff = n->idx_blks_read - n->idx_blks_read_old;

		n->idx_blks_hit_old = n->idx_blks_hit;
		n->idx_blks_hit = atoll(PQgetvalue(pgresult, i, 5));
		n->idx_blks_hit_diff = n->idx_blks_hit - n->idx_blks_hit_old;

		memcpy(&indexios[i], n, sizeof(struct indexio_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
indexiocmp(struct indexio_t *e1, struct indexio_t *e2)
{
	return (e1->indexiorelid < e2->indexiorelid ?
			-1 : e1->indexiorelid > e2->indexiorelid);
}

int
select_indexio(void)
{
	return (0);
}

int
read_indexio(void)
{
	indexio_info();
	num_disp = indexio_count;
	return (0);
}

int
initindexio(void)
{
	field_view	*v;

	indexios = NULL;
	indexio_count = 0;

	for (v = views_indexio; v->name != NULL; v++)
		add_view(v);

	read_indexio();

	return(1);
}

void
print_indexio(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < indexio_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_INDEXIO_SCHEMA, indexios[i].schemaname);
				print_fld_str(FLD_INDEXIO_INDEXIORELNAME,
						indexios[i].indexiorelname);
				print_fld_str(FLD_INDEXIO_RELNAME, indexios[i].relname);
				print_fld_uint(FLD_INDEXIO_IDX_BLKS_READ,
						indexios[i].idx_blks_read);
				print_fld_uint(FLD_INDEXIO_IDX_BLKS_HIT,
						indexios[i].idx_blks_hit);
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
sort_indexio(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (indexios == NULL)
		return;
	if (indexio_count <= 0)
		return;

	mergesort(indexios, indexio_count, sizeof(struct indexio_t),
			ordering->func);
}

int
sort_indexio_idx_blks_hit_callback(const void *v1, const void *v2)
{
	struct indexio_t *n1, *n2;
	n1 = (struct indexio_t *) v1;
	n2 = (struct indexio_t *) v2;

	if (n1->idx_blks_hit_diff < n2->idx_blks_hit_diff)
		return sortdir;
	if (n1->idx_blks_hit_diff > n2->idx_blks_hit_diff)
		return -sortdir;

	return sort_indexio_relname_callback(v1, v2);
}

int
sort_indexio_idx_blks_read_callback(const void *v1, const void *v2)
{
	struct indexio_t *n1, *n2;
	n1 = (struct indexio_t *) v1;
	n2 = (struct indexio_t *) v2;

	if (n1->idx_blks_read_diff < n2->idx_blks_read_diff)
		return sortdir;
	if (n1->idx_blks_read_diff > n2->idx_blks_read_diff)
		return -sortdir;

	return sort_indexio_relname_callback(v1, v2);
}

int
sort_indexio_indexiorelname_callback(const void *v1, const void *v2)
{
	struct indexio_t *n1, *n2;
	n1 = (struct indexio_t *) v1;
	n2 = (struct indexio_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_indexio_relname_callback(const void *v1, const void *v2)
{
	struct indexio_t *n1, *n2;
	n1 = (struct indexio_t *) v1;
	n2 = (struct indexio_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return sort_indexio_indexiorelname_callback(v1, v2);
}

int
sort_indexio_schemaname_callback(const void *v1, const void *v2)
{
	struct indexio_t *n1, *n2;
	n1 = (struct indexio_t *) v1;
	n2 = (struct indexio_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
