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

#define QUERY_STAT_TEMP_BLK \
		"SELECT queryid, rows, temp_blks_read, temp_blks_written, blk_read_time,\n" \
		"       blk_write_time\n" \
		"FROM pg_stat_statements;"

struct stmttempblk_t
{
	RB_ENTRY(stmttempblk_t) entry;

	char queryid[NAMEDATALEN+1];
	int64_t rows;
	int64_t temp_blks_read;
	int64_t temp_blks_written;
	double blk_read_time;
	double blk_write_time;

};

int stmttempblk_cmp(struct stmttempblk_t *, struct stmttempblk_t *);
static void stmttempblk_info(void);
void print_stmttempblk(void);
int read_stmttempblk(void);
int select_stmttempblk(void);
void sort_stmttempblk(void);
int sort_stmttempblk_queryid_callback(const void *, const void *);
int sort_stmttempblk_rows_callback(const void *, const void *);
int sort_stmttempblk_temp_blks_read_callback(const void *, const void *);
int sort_stmttempblk_temp_blks_written_callback(const void *, const void *);
int sort_stmttempblk_blk_read_time_callback(const void *, const void *);
int sort_stmttempblk_blk_write_time_callback(const void *, const void *);

RB_HEAD(stmttempblk, stmttempblk_t) head_stmttempblks =
    RB_INITIALIZER(&head_stmttempblks);
RB_PROTOTYPE(stmttempblk, stmttempblk_t, entry, stmttempblk_cmp)
RB_GENERATE(stmttempblk, stmttempblk_t, entry, stmttempblk_cmp)

field_def fields_stmttempblk[] = {
	{ "QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "ROWS", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TEMP_BLK_READ", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TEMP_BLK_WRITTEN", 17, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "BLK_READ_TIME", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "BLK_WRITE_TIME", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmttempblk, 0)
#define FLD_STMT_ROWS          FIELD_ADDR(fields_stmttempblk, 1)
#define FLD_STMT_TEMP_BLKS_READ      FIELD_ADDR(fields_stmttempblk, 2)
#define FLD_STMT_TEMP_BLKS_WRITTEN  FIELD_ADDR(fields_stmttempblk, 3)
#define FLD_STMT_BLK_READ_TIME      FIELD_ADDR(fields_stmttempblk, 4)
#define FLD_STMT_BLK_WRITE_TIME FIELD_ADDR(fields_stmttempblk, 5)

/* Define views */
field_def *view_stmttempblk_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_ROWS, FLD_STMT_TEMP_BLKS_READ,
	FLD_STMT_TEMP_BLKS_WRITTEN, FLD_STMT_BLK_READ_TIME, FLD_STMT_BLK_WRITE_TIME, NULL
};

order_type stmttempblk_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmttempblk_queryid_callback},
	{"rows", "rows", 'r', sort_stmttempblk_rows_callback},
	{"temp_blk_read", "temp_blk_read", 'e', sort_stmttempblk_temp_blks_read_callback},
	{"temp_blk_written", "temp_blk_written", 'w',
			sort_stmttempblk_temp_blks_written_callback},
	{"blk_read_time", "blk_read_time", 'a', sort_stmttempblk_blk_read_time_callback},
	{"blk_write_time", "blk_write_time", 'i', sort_stmttempblk_blk_write_time_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmttempblk_mgr = {
	"stmttempblk", select_stmttempblk, read_stmttempblk, sort_stmttempblk,
	print_header, print_stmttempblk, keyboard_callback, stmttempblk_order_list,
	stmttempblk_order_list
};

field_view views_stmttempblk[] = {
	{ view_stmttempblk_0, "stmttempblk", 'P', &stmttempblk_mgr },
	{ NULL, NULL, 0, NULL }
};

int stmttempblk_exist = 1;
int	stmttempblk_count;
struct stmttempblk_t *stmttempblks;

static void
stmttempblk_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct stmttempblk_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_STMT_EXIST);
		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK || PQntuples(pgresult) == 0) {
			stmttempblk_exist = 0;
			PQclear(pgresult);
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_TEMP_BLK);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = stmttempblk_count;
			stmttempblk_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (stmttempblk_count > i) {
		p = reallocarray(stmttempblks, stmttempblk_count,
				sizeof(struct stmttempblk_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmttempblks = p;
	}

	for (i = 0; i < stmttempblk_count; i++) {
		n = malloc(sizeof(struct stmttempblk_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmttempblk, &head_stmttempblks, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->rows = atoll(PQgetvalue(pgresult, i, 1));
		n->temp_blks_read = atoll(PQgetvalue(pgresult, i, 2));
		n->temp_blks_written = atoll(PQgetvalue(pgresult, i, 3));
		n->blk_read_time = atof(PQgetvalue(pgresult, i, 4));
		n->blk_write_time = atof(PQgetvalue(pgresult, i, 5));

		memcpy(&stmttempblks[i], n, sizeof(struct stmttempblk_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmttempblk_cmp(struct stmttempblk_t *e1, struct stmttempblk_t *e2)
{
  return (e1->queryid< e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmttempblk(void)
{
	return (0);
}

int
read_stmttempblk(void)
{
	stmttempblk_info();
	num_disp = stmttempblk_count;
	return (0);
}

int
initstmttempblk(void)
{
	field_view	*v;

	stmttempblks = NULL;
	stmttempblk_count = 0;

	read_stmttempblk();
	if(stmttempblk_exist == 0){
		return 0;
	}

	for (v = views_stmttempblk; v->name != NULL; v++)
		add_view(v);

	return(1);
}

void
print_stmttempblk(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmttempblk_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_QUERYID, stmttempblks[i].queryid);
            			print_fld_uint(FLD_STMT_ROWS, stmttempblks[i].rows);
				print_fld_uint(FLD_STMT_TEMP_BLKS_READ,
						stmttempblks[i].temp_blks_read);
				print_fld_uint(FLD_STMT_TEMP_BLKS_WRITTEN,
						stmttempblks[i].temp_blks_written);
				print_fld_float(FLD_STMT_BLK_READ_TIME,
						stmttempblks[i].blk_read_time,2);
				print_fld_float(FLD_STMT_BLK_WRITE_TIME,
						stmttempblks[i].blk_write_time,2);
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
sort_stmttempblk(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmttempblks == NULL)
		return;
	if (stmttempblk_count <= 0)
		return;

	mergesort(stmttempblks, stmttempblk_count, sizeof(struct stmttempblk_t),
			ordering->func);
}

int
sort_stmttempblk_queryid_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmttempblk_rows_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	if (n1->rows < n2->rows)
		return sortdir;
	if (n1->rows > n2->rows)
		return -sortdir;

	return sort_stmttempblk_queryid_callback(v1, v2);
}

int
sort_stmttempblk_temp_blks_read_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	if (n1->temp_blks_read < n2->temp_blks_read)
		return sortdir;
	if (n1->temp_blks_read > n2->temp_blks_read)
		return -sortdir;

	return sort_stmttempblk_queryid_callback(v1, v2);
}

int
sort_stmttempblk_temp_blks_written_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	if (n1->temp_blks_written < n2->temp_blks_written)
		return sortdir;
	if (n1->temp_blks_written > n2->temp_blks_written)
		return -sortdir;

	return sort_stmttempblk_queryid_callback(v1, v2);
}

int
sort_stmttempblk_blk_read_time_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	if (n1->blk_read_time < n2->blk_read_time)
		return sortdir;
	if (n1->blk_read_time > n2->blk_read_time)
		return -sortdir;

	return sort_stmttempblk_queryid_callback(v1, v2);
}

int
sort_stmttempblk_blk_write_time_callback(const void *v1, const void *v2)
{
	struct stmttempblk_t *n1, *n2;
	n1 = (struct stmttempblk_t *) v1;
	n2 = (struct stmttempblk_t *) v2;

	if (n1->blk_write_time < n2->blk_write_time)
		return sortdir;
	if (n1->blk_write_time > n2->blk_write_time)
		return -sortdir;

	return sort_stmttempblk_queryid_callback(v1, v2);
}