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

#define QUERY_STAT_TABLES \
		"SELECT relid, schemaname, relname, seq_scan, seq_tup_read,\n" \
		"       idx_scan, idx_tup_fetch\n" \
		"FROM pg_stat_all_tables;"

struct tablescan_t
{
	RB_ENTRY(tablescan_t) entry;

	long long relid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];
	int64_t idx_scan;
	int64_t idx_scan_diff;
	int64_t idx_scan_old;
	int64_t idx_tup_fetch;
	int64_t idx_tup_fetch_diff;
	int64_t idx_tup_fetch_old;
	int64_t seq_scan;
	int64_t seq_scan_diff;
	int64_t seq_scan_old;
	int64_t seq_tup_read;
	int64_t seq_tup_read_diff;
	int64_t seq_tup_read_old;
};

int tablescancmp(struct tablescan_t *, struct tablescan_t *);
static void tablescan_info(void);
void print_tablescan(void);
int read_tablescan(void);
int select_tablescan(void);
void sort_tablescan(void);
int sort_tablescan_idx_scan_callback(const void *, const void *);
int sort_tablescan_idx_tup_fetch_callback(const void *, const void *);
int sort_tablescan_relname_callback(const void *, const void *);
int sort_tablescan_schemaname_callback(const void *, const void *);
int sort_tablescan_seq_scan_callback(const void *, const void *);
int sort_tablescan_seq_tup_read_callback(const void *, const void *);

RB_HEAD(tablescan, tablescan_t) head_tablescans = RB_INITIALIZER(&head_tablescans);
RB_PROTOTYPE(tablescan, tablescan_t, entry, tablescancmp)
RB_GENERATE(tablescan, tablescan_t, entry, tablescancmp)

field_def fields_tablescan[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "SEQ_SCAN", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "SEQ_TUP_READ", 13, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "IDX_SCAN", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "IDX_TUP_FETCH", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_TABLE_SCHEMA        FIELD_ADDR(fields_tablescan, 0)
#define FLD_TABLE_NAME          FIELD_ADDR(fields_tablescan, 1)
#define FLD_TABLE_SEQ_SCAN      FIELD_ADDR(fields_tablescan, 2)
#define FLD_TABLE_SEQ_TUP_READ  FIELD_ADDR(fields_tablescan, 3)
#define FLD_TABLE_IDX_SCAN      FIELD_ADDR(fields_tablescan, 4)
#define FLD_TABLE_IDX_TUP_FETCH FIELD_ADDR(fields_tablescan, 5)

/* Define views */
field_def *view_tablescan_0[] = {
	FLD_TABLE_SCHEMA, FLD_TABLE_NAME, FLD_TABLE_SEQ_SCAN,
	FLD_TABLE_SEQ_TUP_READ, FLD_TABLE_IDX_SCAN, FLD_TABLE_IDX_TUP_FETCH, NULL
};

order_type tablescan_order_list[] = {
	{"schema", "schema", 's', sort_tablescan_schemaname_callback},
	{"name", "name", 'n', sort_tablescan_relname_callback},
	{"seq_scan", "seq_scan", 'c', sort_tablescan_seq_scan_callback},
	{"seq_tup_read", "seq_tup_read", 't',
			sort_tablescan_seq_tup_read_callback},
	{"idx_scan", "idx_scan", 'i', sort_tablescan_idx_scan_callback},
	{"idx_tup_fetch", "idx_tup_fetch", 'f', sort_tablescan_idx_scan_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tablescan_mgr = {
	"tablescan", select_tablescan, read_tablescan, sort_tablescan,
	print_header, print_tablescan, keyboard_callback, tablescan_order_list,
	tablescan_order_list
};

field_view views_tablescan[] = {
	{ view_tablescan_0, "tablescan", 'T', &tablescan_mgr },
	{ NULL, NULL, 0, NULL }
};

int	tablescan_count;
struct tablescan_t *tablescans;

static void
tablescan_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct tablescan_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_TABLES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = tablescan_count;
			tablescan_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (tablescan_count > i) {
		p = reallocarray(tablescans, tablescan_count,
				sizeof(struct tablescan_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tablescans = p;
	}

	for (i = 0; i < tablescan_count; i++) {
		n = malloc(sizeof(struct tablescan_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tablescan, &head_tablescans, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->seq_scan_old = n->seq_scan;
		n->seq_scan = atoll(PQgetvalue(pgresult, i, 3));
		n->seq_scan_diff = n->seq_scan - n->seq_scan_old;

		n->seq_tup_read_old = n->seq_tup_read;
		n->seq_tup_read = atoll(PQgetvalue(pgresult, i, 4));
		n->seq_tup_read_diff = n->seq_tup_read - n->seq_tup_read_old;

		n->idx_scan_old = n->idx_scan;
		n->idx_scan = atoll(PQgetvalue(pgresult, i, 5));
		n->idx_scan_diff = n->idx_scan - n->idx_scan_old;

		n->idx_tup_fetch_old = n->idx_tup_fetch;
		n->idx_tup_fetch = atoll(PQgetvalue(pgresult, i, 6));
		n->idx_tup_fetch_diff = n->idx_tup_fetch - n->idx_tup_fetch_old;

		memcpy(&tablescans[i], n, sizeof(struct tablescan_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tablescancmp(struct tablescan_t *e1, struct tablescan_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tablescan(void)
{
	return (0);
}

int
read_tablescan(void)
{
	tablescan_info();
	num_disp = tablescan_count;
	return (0);
}

int
inittablescan(void)
{
	field_view	*v;

	tablescans = NULL;
	tablescan_count = 0;

	for (v = views_tablescan; v->name != NULL; v++)
		add_view(v);

	read_tablescan();

	return(1);
}

void
print_tablescan(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tablescan_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_TABLE_SCHEMA, tablescans[i].schemaname);
				print_fld_str(FLD_TABLE_NAME, tablescans[i].relname);
				print_fld_uint(FLD_TABLE_SEQ_SCAN,
						tablescans[i].seq_scan_diff);
				print_fld_uint(FLD_TABLE_SEQ_TUP_READ,
						tablescans[i].seq_tup_read_diff);
				print_fld_uint(FLD_TABLE_IDX_SCAN,
						tablescans[i].idx_scan_diff);
				print_fld_uint(FLD_TABLE_IDX_TUP_FETCH,
						tablescans[i].idx_tup_fetch_diff);
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
sort_tablescan(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tablescans == NULL)
		return;
	if (tablescan_count <= 0)
		return;

	mergesort(tablescans, tablescan_count, sizeof(struct tablescan_t),
			ordering->func);
}

int
sort_tablescan_idx_scan_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (n1->idx_scan_diff < n2->idx_scan_diff)
		return sortdir;
	if (n1->idx_scan_diff > n2->idx_scan_diff)
		return -sortdir;

	return sort_tablescan_relname_callback(v1, v2);
}

int
sort_tablescan_idx_tup_fetch_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (n1->idx_tup_fetch_diff < n2->idx_tup_fetch_diff)
		return sortdir;
	if (n1->idx_tup_fetch_diff > n2->idx_tup_fetch_diff)
		return -sortdir;

	return sort_tablescan_relname_callback(v1, v2);
}

int
sort_tablescan_relname_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tablescan_schemaname_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}

int
sort_tablescan_seq_scan_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (n1->seq_scan_diff < n2->seq_scan_diff)
		return sortdir;
	if (n1->seq_scan_diff > n2->seq_scan_diff)
		return -sortdir;

	return sort_tablescan_relname_callback(v1, v2);
}

int
sort_tablescan_seq_tup_read_callback(const void *v1, const void *v2)
{
	struct tablescan_t *n1, *n2;
	n1 = (struct tablescan_t *) v1;
	n2 = (struct tablescan_t *) v2;

	if (n1->seq_tup_read_diff < n2->seq_tup_read_diff)
		return sortdir;
	if (n1->seq_tup_read_diff > n2->seq_tup_read_diff)
		return -sortdir;

	return sort_tablescan_relname_callback(v1, v2);
}
