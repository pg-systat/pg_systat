/*
 * Copyright (c) 2021 PostgreSQL Global Development Group
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

#define QUERY_STAT_LOCAL_BLK \
		"SELECT queryid, rows, local_blks_hit, local_blks_read, local_blks_dirtied,\n" \
		"       local_blks_written\n" \
		"FROM pg_stat_statements;"

struct stmtlocalblk_t
{
	RB_ENTRY(stmtlocalblk_t) entry;

	char queryid[NAMEDATALEN+1];
	int64_t rows;
	int64_t local_blks_hit;
	int64_t local_blks_read;
	int64_t local_blks_dirtied;
	int64_t local_blks_written;

};

int stmtlocalblk_cmp(struct stmtlocalblk_t *, struct stmtlocalblk_t *);
static void stmtlocalblk_info(void);
void print_stmtlocalblk(void);
int read_stmtlocalblk(void);
int select_stmtlocalblk(void);
void sort_stmtlocalblk(void);
int sort_stmtlocalblk_queryid_callback(const void *, const void *);
int sort_stmtlocalblk_rows_callback(const void *, const void *);
int sort_stmtlocalblk_local_blks_hit_callback(const void *, const void *);
int sort_stmtlocalblk_local_blks_read_callback(const void *, const void *);
int sort_stmtlocalblk_local_blks_dirtied_callback(const void *, const void *);
int sort_stmtlocalblk_local_blks_written_callback(const void *, const void *);

RB_HEAD(stmtlocalblk, stmtlocalblk_t) head_stmtlocalblks =
    RB_INITIALIZER(&head_stmtlocalblks);
RB_PROTOTYPE(stmtlocalblk, stmtlocalblk_t, entry, stmtlocalblk_cmp)
RB_GENERATE(stmtlocalblk, stmtlocalblk_t, entry, stmtlocalblk_cmp)

field_def fields_stmtlocalblk[] = {
	{ "QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "ROWS", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LOCAL_BLK_HIT", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LOCAL_BLK_READ", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LOCAL_BLK_DIRTIED", 18, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LOCAL_BLK_WRITTEN", 18, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmtlocalblk, 0)
#define FLD_STMT_ROWS          FIELD_ADDR(fields_stmtlocalblk, 1)
#define FLD_STMT_LOCAL_BLKS_HIT      FIELD_ADDR(fields_stmtlocalblk, 2)
#define FLD_STMT_LOCAL_BLKS_READ  FIELD_ADDR(fields_stmtlocalblk, 3)
#define FLD_STMT_LOCAL_BLKS_DIRTIED      FIELD_ADDR(fields_stmtlocalblk, 4)
#define FLD_STMT_LOCAL_BLKS_WRITTEN FIELD_ADDR(fields_stmtlocalblk, 5)

/* Define views */
field_def *view_stmtlocalblk_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_ROWS, FLD_STMT_LOCAL_BLKS_HIT,
	FLD_STMT_LOCAL_BLKS_READ, FLD_STMT_LOCAL_BLKS_DIRTIED, FLD_STMT_LOCAL_BLKS_WRITTEN, NULL
};

order_type stmtlocalblk_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmtlocalblk_queryid_callback},
	{"rows", "rows", 'r', sort_stmtlocalblk_rows_callback},
	{"local_blk_hits", "local_blk_hits", 'i', sort_stmtlocalblk_local_blks_hit_callback},
	{"local_blk_read", "local_blk_read", 'e',
			sort_stmtlocalblk_local_blks_read_callback},
	{"local_blk_dirtied", "local_blk_dirtied", 'd', sort_stmtlocalblk_local_blks_dirtied_callback},
	{"local_blk_written", "local_blk_written", 'w', sort_stmtlocalblk_local_blks_written_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmtlocalblk_mgr = {
	"stmtlocalblk", select_stmtlocalblk, read_stmtlocalblk, sort_stmtlocalblk,
	print_header, print_stmtlocalblk, keyboard_callback, stmtlocalblk_order_list,
	stmtlocalblk_order_list
};

field_view views_stmtlocalblk[] = {
	{ view_stmtlocalblk_0, "stmtlocalblk", 'P', &stmtlocalblk_mgr },
	{ NULL, NULL, 0, NULL }
};

int	stmtlocalblk_count;
struct stmtlocalblk_t *stmtlocalblks;

static void
stmtlocalblk_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct stmtlocalblk_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_LOCAL_BLK);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = stmtlocalblk_count;
			stmtlocalblk_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (stmtlocalblk_count > i) {
		p = reallocarray(stmtlocalblks, stmtlocalblk_count,
				sizeof(struct stmtlocalblk_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmtlocalblks = p;
	}

	for (i = 0; i < stmtlocalblk_count; i++) {
		n = malloc(sizeof(struct stmtlocalblk_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmtlocalblk, &head_stmtlocalblks, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->rows = atoll(PQgetvalue(pgresult, i, 1));
		n->local_blks_hit = atoll(PQgetvalue(pgresult, i, 2));
		n->local_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->local_blks_dirtied = atoll(PQgetvalue(pgresult, i, 4));
		n->local_blks_written = atoll(PQgetvalue(pgresult, i, 5));

		memcpy(&stmtlocalblks[i], n, sizeof(struct stmtlocalblk_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmtlocalblk_cmp(struct stmtlocalblk_t *e1, struct stmtlocalblk_t *e2)
{
  return (e1->queryid< e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmtlocalblk(void)
{
	return (0);
}

int
read_stmtlocalblk(void)
{
	stmtlocalblk_info();
	num_disp = stmtlocalblk_count;
	return (0);
}

int
initstmtlocalblk(void)
{
	field_view	*v;

	stmtlocalblks = NULL;
	stmtlocalblk_count = 0;

	for (v = views_stmtlocalblk; v->name != NULL; v++)
		add_view(v);
	read_stmtlocalblk();

	return(1);
}

void
print_stmtlocalblk(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmtlocalblk_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_QUERYID, stmtlocalblks[i].queryid);
            			print_fld_uint(FLD_STMT_ROWS, stmtlocalblks[i].rows);
				print_fld_uint(FLD_STMT_LOCAL_BLKS_HIT,
						stmtlocalblks[i].local_blks_hit);
				print_fld_uint(FLD_STMT_LOCAL_BLKS_READ,
						stmtlocalblks[i].local_blks_read);
				print_fld_uint(FLD_STMT_LOCAL_BLKS_DIRTIED,
						stmtlocalblks[i].local_blks_dirtied);
				print_fld_uint(FLD_STMT_LOCAL_BLKS_WRITTEN,
						stmtlocalblks[i].local_blks_written);
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
sort_stmtlocalblk(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmtlocalblks == NULL)
		return;
	if (stmtlocalblk_count <= 0)
		return;

	mergesort(stmtlocalblks, stmtlocalblk_count, sizeof(struct stmtlocalblk_t),
			ordering->func);
}

int
sort_stmtlocalblk_queryid_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmtlocalblk_rows_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	if (n1->rows < n2->rows)
		return sortdir;
	if (n1->rows > n2->rows)
		return -sortdir;

	return sort_stmtlocalblk_queryid_callback(v1, v2);
}

int
sort_stmtlocalblk_local_blks_hit_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	if (n1->local_blks_hit < n2->local_blks_hit)
		return sortdir;
	if (n1->local_blks_hit > n2->local_blks_hit)
		return -sortdir;

	return sort_stmtlocalblk_queryid_callback(v1, v2);
}

int
sort_stmtlocalblk_local_blks_read_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	if (n1->local_blks_read < n2->local_blks_read)
		return sortdir;
	if (n1->local_blks_read > n2->local_blks_read)
		return -sortdir;

	return sort_stmtlocalblk_queryid_callback(v1, v2);
}

int
sort_stmtlocalblk_local_blks_dirtied_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	if (n1->local_blks_dirtied < n2->local_blks_dirtied)
		return sortdir;
	if (n1->local_blks_dirtied > n2->local_blks_dirtied)
		return -sortdir;

	return sort_stmtlocalblk_queryid_callback(v1, v2);
}

int
sort_stmtlocalblk_local_blks_written_callback(const void *v1, const void *v2)
{
	struct stmtlocalblk_t *n1, *n2;
	n1 = (struct stmtlocalblk_t *) v1;
	n2 = (struct stmtlocalblk_t *) v2;

	if (n1->local_blks_written < n2->local_blks_written)
		return sortdir;
	if (n1->local_blks_written > n2->local_blks_written)
		return -sortdir;

	return sort_stmtlocalblk_queryid_callback(v1, v2);
}