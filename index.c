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

#define QUERY_STAT_INDEXES \
		"SELECT indexrelid, schemaname, relname, indexrelname, idx_scan,\n" \
		"       idx_tup_read, idx_tup_fetch\n" \
		"FROM pg_stat_all_indexes;"

struct index_t
{
	RB_ENTRY(index_t) entry;

	long long indexrelid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];
	char indexrelname[NAMEDATALEN + 1];

	int64_t idx_scan;
	int64_t idx_scan_diff;
	int64_t idx_scan_old;
	
	int64_t idx_tup_read;
	int64_t idx_tup_read_diff;
	int64_t idx_tup_read_old;

	int64_t idx_tup_fetch;
	int64_t idx_tup_fetch_diff;
	int64_t idx_tup_fetch_old;
};

int indexcmp(struct index_t *, struct index_t *);
static void index_info(void);
void print_index(void);
int read_index(void);
int select_index(void);
void sort_index(void);
int sort_index_indexrelname_callback(const void *, const void *);
int sort_index_relname_callback(const void *, const void *);
int sort_index_schemaname_callback(const void *, const void *);
int sort_index_idx_scan_callback(const void *, const void *);
int sort_index_idx_tup_fetch_callback(const void *, const void *);
int sort_index_idx_tup_read_callback(const void *, const void *);

RB_HEAD(index, index_t) head_indexs = RB_INITIALIZER(&head_indexs);
RB_PROTOTYPE(index, index_t, entry, indexcmp)
RB_GENERATE(index, index_t, entry, indexcmp)

field_def fields_index[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "INDEXNAME", 10, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "TABLENAME", 10, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "SCAN", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TUP_READ", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TUP_FETCH", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_INDEX_SCHEMA        FIELD_ADDR(fields_index, 0)
#define FLD_INDEX_INDEXRELNAME  FIELD_ADDR(fields_index, 1)
#define FLD_INDEX_RELNAME       FIELD_ADDR(fields_index, 2)
#define FLD_INDEX_IDX_SCAN      FIELD_ADDR(fields_index, 3)
#define FLD_INDEX_IDX_TUP_READ  FIELD_ADDR(fields_index, 4)
#define FLD_INDEX_IDX_TUP_FETCH FIELD_ADDR(fields_index, 5)

/* Define views */
field_def *view_index_0[] = {
	FLD_INDEX_SCHEMA, FLD_INDEX_INDEXRELNAME, FLD_INDEX_RELNAME,
	FLD_INDEX_IDX_SCAN, FLD_INDEX_IDX_TUP_READ, FLD_INDEX_IDX_TUP_FETCH, NULL
};

order_type index_order_list[] = {
	{"schema", "schema", 's', sort_index_schemaname_callback},
	{"indexname", "indexname", 'i', sort_index_indexrelname_callback},
	{"tablename", "tablename", 't', sort_index_relname_callback},
	{"idx_scan", "idx_scan", 'c', sort_index_idx_scan_callback},
	{"idx_tup_read", "idx_tup_read", 'r', sort_index_idx_tup_read_callback},
	{"idx_tup_fetch", "idx_tup_fetch", 'f', sort_index_idx_tup_fetch_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager index_mgr = {
	"index", select_index, read_index, sort_index, print_header,
	print_index, keyboard_callback, index_order_list, index_order_list
};

field_view views_index[] = {
	{ view_index_0, "index", 'U', &index_mgr },
	{ NULL, NULL, 0, NULL }
};

int	index_count;
struct index_t *indexs;

static void
index_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct index_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_INDEXES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = index_count;
			index_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (index_count > i) {
		p = reallocarray(indexs, index_count, sizeof(struct index_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		indexs = p;
	}

	for (i = 0; i < index_count; i++) {
		n = malloc(sizeof(struct index_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->indexrelid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(index, &head_indexs, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->indexrelname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 3), NAMEDATALEN);

		n->idx_scan_old = n->idx_scan;
		n->idx_scan = atoll(PQgetvalue(pgresult, i, 4));
		n->idx_scan_diff = n->idx_scan - n->idx_scan_old;

		n->idx_tup_read_old = n->idx_tup_read;
		n->idx_tup_read = atoll(PQgetvalue(pgresult, i, 5));
		n->idx_tup_read_diff = n->idx_tup_read - n->idx_tup_read_old;

		n->idx_tup_fetch_old = n->idx_tup_fetch;
		n->idx_tup_fetch = atoll(PQgetvalue(pgresult, i, 6));
		n->idx_tup_fetch_diff = n->idx_tup_fetch - n->idx_tup_fetch_old;

		memcpy(&indexs[i], n, sizeof(struct index_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
indexcmp(struct index_t *e1, struct index_t *e2)
{
	return (e1->indexrelid < e2->indexrelid ?
			-1 : e1->indexrelid > e2->indexrelid);
}

int
select_index(void)
{
	return (0);
}

int
read_index(void)
{
	index_info();
	num_disp = index_count;
	return (0);
}

int
initindex(void)
{
	field_view	*v;

	indexs = NULL;
	index_count = 0;

	for (v = views_index; v->name != NULL; v++)
		add_view(v);

	read_index();

	return(1);
}

void
print_index(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < index_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_INDEX_SCHEMA, indexs[i].schemaname);
				print_fld_str(FLD_INDEX_INDEXRELNAME, indexs[i].indexrelname);
				print_fld_str(FLD_INDEX_RELNAME, indexs[i].relname);
				print_fld_uint(FLD_INDEX_IDX_SCAN, indexs[i].idx_scan);
				print_fld_uint(FLD_INDEX_IDX_TUP_READ, indexs[i].idx_tup_read);
				print_fld_uint(FLD_INDEX_IDX_TUP_FETCH,
						indexs[i].idx_tup_fetch);
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
sort_index(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (indexs == NULL)
		return;
	if (index_count <= 0)
		return;

	mergesort(indexs, index_count, sizeof(struct index_t),
			ordering->func);
}

int
sort_index_idx_scan_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (n1->idx_scan_diff < n2->idx_scan_diff)
		return sortdir;
	if (n1->idx_scan_diff > n2->idx_scan_diff)
		return -sortdir;

	return sort_index_relname_callback(v1, v2);
}

int
sort_index_idx_tup_fetch_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (n1->idx_tup_fetch_diff < n2->idx_tup_fetch_diff)
		return sortdir;
	if (n1->idx_tup_fetch_diff > n2->idx_tup_fetch_diff)
		return -sortdir;

	return sort_index_relname_callback(v1, v2);
}

int
sort_index_idx_tup_read_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (n1->idx_tup_read_diff < n2->idx_tup_read_diff)
		return sortdir;
	if (n1->idx_tup_read_diff > n2->idx_tup_read_diff)
		return -sortdir;

	return sort_index_relname_callback(v1, v2);
}

int
sort_index_indexrelname_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_index_relname_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return sort_index_indexrelname_callback(v1, v2);
}

int
sort_index_schemaname_callback(const void *v1, const void *v2)
{
	struct index_t *n1, *n2;
	n1 = (struct index_t *) v1;
	n2 = (struct index_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
