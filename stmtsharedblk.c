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

#define QUERY_STAT_SHARED_BLK \
		"SELECT queryid, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,\n" \
		"       shared_blks_written\n" \
		"FROM pg_stat_statements;"

struct stmtsharedblk_t
{
	RB_ENTRY(stmtsharedblk_t) entry;

	char queryid[NAMEDATALEN+1];
	int64_t rows;
	int64_t shared_blks_hit;
	int64_t shared_blks_read;
	int64_t shared_blks_dirtied;
	int64_t shared_blks_written;

};

int stmtsharedblk_cmp(struct stmtsharedblk_t *, struct stmtsharedblk_t *);
static void stmtsharedblk_info(void);
void print_stmtsharedblk(void);
int read_stmtsharedblk(void);
int select_stmtsharedblk(void);
void sort_stmtsharedblk(void);
int sort_stmtsharedblk_queryid_callback(const void *, const void *);
int sort_stmtsharedblk_rows_callback(const void *, const void *);
int sort_stmtsharedblk_shared_blks_hit_callback(const void *, const void *);
int sort_stmtsharedblk_shared_blks_read_callback(const void *, const void *);
int sort_stmtsharedblk_shared_blks_dirtied_callback(const void *, const void *);
int sort_stmtsharedblk_shared_blks_written_callback(const void *, const void *);

RB_HEAD(stmtsharedblk, stmtsharedblk_t) head_stmtsharedblks =
    RB_INITIALIZER(&head_stmtsharedblks);
RB_PROTOTYPE(stmtsharedblk, stmtsharedblk_t, entry, stmtsharedblk_cmp)
RB_GENERATE(stmtsharedblk, stmtsharedblk_t, entry, stmtsharedblk_cmp)

field_def fields_stmtsharedblk[] = {
	{ "QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "ROWS", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "SHARED_BLK_HIT", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "SHARED_BLK_READ", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "SHARED_BLK_DIRTIED", 19, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "SHARED_BLK_WRITTEN", 19, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmtsharedblk, 0)
#define FLD_STMT_ROWS          FIELD_ADDR(fields_stmtsharedblk, 1)
#define FLD_STMT_SHARED_BLKS_HIT      FIELD_ADDR(fields_stmtsharedblk, 2)
#define FLD_STMT_SHARED_BLKS_READ  FIELD_ADDR(fields_stmtsharedblk, 3)
#define FLD_STMT_SHARED_BLKS_DIRTIED      FIELD_ADDR(fields_stmtsharedblk, 4)
#define FLD_STMT_SHARED_BLKS_WRITTEN FIELD_ADDR(fields_stmtsharedblk, 5)

/* Define views */
field_def *view_stmtsharedblk_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_ROWS, FLD_STMT_SHARED_BLKS_HIT,
	FLD_STMT_SHARED_BLKS_READ, FLD_STMT_SHARED_BLKS_DIRTIED, FLD_STMT_SHARED_BLKS_WRITTEN, NULL
};

order_type stmtsharedblk_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmtsharedblk_queryid_callback},
	{"rows", "rows", 'r', sort_stmtsharedblk_rows_callback},
	{"shared_blk_hits", "shared_blk_hits", 'i', sort_stmtsharedblk_shared_blks_hit_callback},
	{"shared_blk_read", "shared_blk_read", 'e',
			sort_stmtsharedblk_shared_blks_read_callback},
	{"shared_blk_dirtied", "shared_blk_dirtied", 'd', sort_stmtsharedblk_shared_blks_dirtied_callback},
	{"shared_blk_written", "shared_blk_written", 'w', sort_stmtsharedblk_shared_blks_written_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmtsharedblk_mgr = {
	"stmtsharedblk", select_stmtsharedblk, read_stmtsharedblk, sort_stmtsharedblk,
	print_header, print_stmtsharedblk, keyboard_callback, stmtsharedblk_order_list,
	stmtsharedblk_order_list
};

field_view views_stmtsharedblk[] = {
	{ view_stmtsharedblk_0, "stmtsharedblk", 'P', &stmtsharedblk_mgr },
	{ NULL, NULL, 0, NULL }
};

int stmtsharedblk_exist = 1;
int	stmtsharedblk_count;
struct stmtsharedblk_t *stmtsharedblks;

static void
stmtsharedblk_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct stmtsharedblk_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_STMT_EXIST);
		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK || PQntuples(pgresult) == 0) {
			stmtsharedblk_exist = 0;
			PQclear(pgresult);
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_SHARED_BLK);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = stmtsharedblk_count;
			stmtsharedblk_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (stmtsharedblk_count > i) {
		p = reallocarray(stmtsharedblks, stmtsharedblk_count,
				sizeof(struct stmtsharedblk_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmtsharedblks = p;
	}

	for (i = 0; i < stmtsharedblk_count; i++) {
		n = malloc(sizeof(struct stmtsharedblk_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmtsharedblk, &head_stmtsharedblks, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->rows = atoll(PQgetvalue(pgresult, i, 1));
		n->shared_blks_hit = atoll(PQgetvalue(pgresult, i, 2));
		n->shared_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->shared_blks_dirtied = atoll(PQgetvalue(pgresult, i, 4));
		n->shared_blks_written = atoll(PQgetvalue(pgresult, i, 5));

		memcpy(&stmtsharedblks[i], n, sizeof(struct stmtsharedblk_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmtsharedblk_cmp(struct stmtsharedblk_t *e1, struct stmtsharedblk_t *e2)
{
  return (e1->queryid< e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmtsharedblk(void)
{
	return (0);
}

int
read_stmtsharedblk(void)
{
	stmtsharedblk_info();
	num_disp = stmtsharedblk_count;
	return (0);
}

int
initstmtsharedblk(void)
{
	field_view	*v;

	stmtsharedblks = NULL;
	stmtsharedblk_count = 0;

	read_stmtsharedblk();
	if(stmtsharedblk_exist == 0){
		return 0;
	}

	for (v = views_stmtsharedblk; v->name != NULL; v++)
		add_view(v);

	return(1);
}

void
print_stmtsharedblk(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmtsharedblk_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_QUERYID, stmtsharedblks[i].queryid);
            			print_fld_uint(FLD_STMT_ROWS, stmtsharedblks[i].rows);
				print_fld_uint(FLD_STMT_SHARED_BLKS_HIT,
						stmtsharedblks[i].shared_blks_hit);
				print_fld_uint(FLD_STMT_SHARED_BLKS_READ,
						stmtsharedblks[i].shared_blks_read);
				print_fld_uint(FLD_STMT_SHARED_BLKS_DIRTIED,
						stmtsharedblks[i].shared_blks_dirtied);
				print_fld_uint(FLD_STMT_SHARED_BLKS_WRITTEN,
						stmtsharedblks[i].shared_blks_written);
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
sort_stmtsharedblk(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmtsharedblks == NULL)
		return;
	if (stmtsharedblk_count <= 0)
		return;

	mergesort(stmtsharedblks, stmtsharedblk_count, sizeof(struct stmtsharedblk_t),
			ordering->func);
}

int
sort_stmtsharedblk_queryid_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmtsharedblk_rows_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	if (n1->rows < n2->rows)
		return sortdir;
	if (n1->rows > n2->rows)
		return -sortdir;

	return sort_stmtsharedblk_queryid_callback(v1, v2);
}

int
sort_stmtsharedblk_shared_blks_hit_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	if (n1->shared_blks_hit < n2->shared_blks_hit)
		return sortdir;
	if (n1->shared_blks_hit > n2->shared_blks_hit)
		return -sortdir;

	return sort_stmtsharedblk_queryid_callback(v1, v2);
}

int
sort_stmtsharedblk_shared_blks_read_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	if (n1->shared_blks_read < n2->shared_blks_read)
		return sortdir;
	if (n1->shared_blks_read > n2->shared_blks_read)
		return -sortdir;

	return sort_stmtsharedblk_queryid_callback(v1, v2);
}

int
sort_stmtsharedblk_shared_blks_dirtied_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	if (n1->shared_blks_dirtied < n2->shared_blks_dirtied)
		return sortdir;
	if (n1->shared_blks_dirtied > n2->shared_blks_dirtied)
		return -sortdir;

	return sort_stmtsharedblk_queryid_callback(v1, v2);
}

int
sort_stmtsharedblk_shared_blks_written_callback(const void *v1, const void *v2)
{
	struct stmtsharedblk_t *n1, *n2;
	n1 = (struct stmtsharedblk_t *) v1;
	n2 = (struct stmtsharedblk_t *) v2;

	if (n1->shared_blks_written < n2->shared_blks_written)
		return sortdir;
	if (n1->shared_blks_written > n2->shared_blks_written)
		return -sortdir;

	return sort_stmtsharedblk_queryid_callback(v1, v2);
}