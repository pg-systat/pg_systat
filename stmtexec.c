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

#define QUERY_STAT_EXEC_13 \
		"SELECT queryid, calls, total_exec_time, min_exec_time, max_exec_time,\n" \
		"       mean_exec_time, stddev_exec_time\n" \
		"FROM pg_stat_statements;"

#define QUERY_STAT_EXEC_12 \
		"SELECT queryid, calls, total_time, min_time, max_time,\n" \
		"       mean_time, stddev_time\n" \
		"FROM pg_stat_statements;"


struct stmtexec_t
{
	RB_ENTRY(stmtexec_t) entry;

	char queryid[NAMEDATALEN+1];
	int64_t calls;
	double total_exec_time;
	double min_exec_time;
	double max_exec_time;
	double mean_exec_time;
	double stddev_exec_time;

};

int stmtexec_cmp(struct stmtexec_t *, struct stmtexec_t *);
static void stmtexec_info(void);
void print_stmtexec(void);
int read_stmtexec(void);
int select_stmtexec(void);
void sort_stmtexec(void);
int sort_stmtexec_queryid_callback(const void *, const void *);
int sort_stmtexec_calls_callback(const void *, const void *);
int sort_stmtexec_total_exec_time_callback(const void *, const void *);
int sort_stmtexec_min_exec_time_callback(const void *, const void *);
int sort_stmtexec_max_exec_time_callback(const void *, const void *);
int sort_stmtexec_mean_exec_time_callback(const void *, const void *);
int sort_stmtexec_stddev_exec_time_callback(const void *, const void *);

RB_HEAD(stmtexec, stmtexec_t) head_stmtexecs =
    RB_INITIALIZER(&head_stmtexecs);
RB_PROTOTYPE(stmtexec, stmtexec_t, entry, stmtexec_cmp)
RB_GENERATE(stmtexec, stmtexec_t, entry, stmtexec_cmp)

field_def fields_stmtexec[] = {
	{ "QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "CALLS", 6, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TOTAL_EXEC_TIME", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MIN_EXEC_TIME", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MAX_EXEC_TIME", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MEAN_EXEC_TIME", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "STDDEV_EXEC_TIME", 17, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmtexec, 0)
#define FLD_STMT_CALLS          FIELD_ADDR(fields_stmtexec, 1)
#define FLD_STMT_TOTAL_EXEC_TIME      FIELD_ADDR(fields_stmtexec, 2)
#define FLD_STMT_MIN_EXEC_TIME  FIELD_ADDR(fields_stmtexec, 3)
#define FLD_STMT_MAX_EXEC_TIME      FIELD_ADDR(fields_stmtexec, 4)
#define FLD_STMT_MEAN_EXEC_TIME FIELD_ADDR(fields_stmtexec, 5)
#define FLD_STMT_STDDEV_EXEC_TIME FIELD_ADDR(fields_stmtexec, 6)

/* Define views */
field_def *view_stmtexec_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_CALLS, FLD_STMT_TOTAL_EXEC_TIME,
	FLD_STMT_MIN_EXEC_TIME, FLD_STMT_MAX_EXEC_TIME, FLD_STMT_MEAN_EXEC_TIME,
	FLD_STMT_STDDEV_EXEC_TIME, NULL
};

order_type stmtexec_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmtexec_queryid_callback},
	{"calls", "execs", 'c', sort_stmtexec_calls_callback},
	{"total_exec_time", "total_exec_time", 't', sort_stmtexec_total_exec_time_callback},
	{"min_exec_time", "min_exec_time", 'n',
			sort_stmtexec_min_exec_time_callback},
	{"max_exec_time", "max_exec_time", 'm', sort_stmtexec_max_exec_time_callback},
	{"mean_exec_time", "mean_exec_time", 'e', sort_stmtexec_mean_exec_time_callback},
	{"stddev_exec_time", "stddev_exec_time", 'd', sort_stmtexec_stddev_exec_time_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmtexec_mgr = {
	"stmtexec", select_stmtexec, read_stmtexec, sort_stmtexec,
	print_header, print_stmtexec, keyboard_callback, stmtexec_order_list,
	stmtexec_order_list
};

field_view views_stmtexec[] = {
	{ view_stmtexec_0, "stmtexec", 'P', &stmtexec_mgr },
	{ NULL, NULL, 0, NULL }
};

int	stmtexec_count;
struct stmtexec_t *stmtexecs;

static void
stmtexec_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct stmtexec_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
        if( PQserverVersion(options.connection) / 100 < 1300){
		    pgresult = PQexec(options.connection, QUERY_STAT_EXEC_12);
		} else {
		    pgresult = PQexec(options.connection, QUERY_STAT_EXEC_13);
        }

        if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = stmtexec_count;
			stmtexec_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (stmtexec_count > i) {
		p = reallocarray(stmtexecs, stmtexec_count,
				sizeof(struct stmtexec_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmtexecs = p;
	}

	for (i = 0; i < stmtexec_count; i++) {
		n = malloc(sizeof(struct stmtexec_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmtexec, &head_stmtexecs, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->calls = atoll(PQgetvalue(pgresult, i, 1));
		n->total_exec_time = atof(PQgetvalue(pgresult, i, 2));
		n->min_exec_time = atof(PQgetvalue(pgresult, i, 3));
		n->max_exec_time = atof(PQgetvalue(pgresult, i, 4));
		n->mean_exec_time = atof(PQgetvalue(pgresult, i, 5));
		n->stddev_exec_time = atof(PQgetvalue(pgresult, i, 6));

		memcpy(&stmtexecs[i], n, sizeof(struct stmtexec_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmtexec_cmp(struct stmtexec_t *e1, struct stmtexec_t *e2)
{
  return (e1->queryid< e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmtexec(void)
{
	return (0);
}

int
read_stmtexec(void)
{
	stmtexec_info();
	num_disp = stmtexec_count;
	return (0);
}

int
initstmtexec(void)
{
	field_view	*v;

	stmtexecs = NULL;
	stmtexec_count = 0;

	for (v = views_stmtexec; v->name != NULL; v++)
		add_view(v);
	read_stmtexec();

	return(1);
}

void
print_stmtexec(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmtexec_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_QUERYID, stmtexecs[i].queryid);
            			print_fld_uint(FLD_STMT_CALLS, stmtexecs[i].calls);
				print_fld_float(FLD_STMT_TOTAL_EXEC_TIME,
						stmtexecs[i].total_exec_time,2);
				print_fld_float(FLD_STMT_MIN_EXEC_TIME,
						stmtexecs[i].min_exec_time,2);
				print_fld_float(FLD_STMT_MAX_EXEC_TIME,
						stmtexecs[i].max_exec_time,2);
				print_fld_float(FLD_STMT_MEAN_EXEC_TIME,
						stmtexecs[i].mean_exec_time,2);
				print_fld_float(FLD_STMT_STDDEV_EXEC_TIME,
						stmtexecs[i].stddev_exec_time,2);
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
sort_stmtexec(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmtexecs == NULL)
		return;
	if (stmtexec_count <= 0)
		return;

	mergesort(stmtexecs, stmtexec_count, sizeof(struct stmtexec_t),
			ordering->func);
}

int
sort_stmtexec_queryid_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmtexec_calls_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->calls < n2->calls)
		return sortdir;
	if (n1->calls > n2->calls)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}

int
sort_stmtexec_total_exec_time_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->total_exec_time < n2->total_exec_time)
		return sortdir;
	if (n1->total_exec_time > n2->total_exec_time)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}

int
sort_stmtexec_min_exec_time_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->min_exec_time < n2->min_exec_time)
		return sortdir;
	if (n1->min_exec_time > n2->min_exec_time)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}

int
sort_stmtexec_max_exec_time_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->max_exec_time < n2->max_exec_time)
		return sortdir;
	if (n1->max_exec_time > n2->max_exec_time)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}

int
sort_stmtexec_mean_exec_time_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->mean_exec_time < n2->mean_exec_time)
		return sortdir;
	if (n1->mean_exec_time > n2->mean_exec_time)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}

int
sort_stmtexec_stddev_exec_time_callback(const void *v1, const void *v2)
{
	struct stmtexec_t *n1, *n2;
	n1 = (struct stmtexec_t *) v1;
	n2 = (struct stmtexec_t *) v2;

	if (n1->stddev_exec_time < n2->stddev_exec_time)
		return sortdir;
	if (n1->stddev_exec_time > n2->stddev_exec_time)
		return -sortdir;

	return sort_stmtexec_queryid_callback(v1, v2);
}
