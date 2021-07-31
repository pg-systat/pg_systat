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

#define QUERY_STAT_PLAN \
		"SELECT queryid, plans, total_plan_time, min_plan_time, max_plan_time,\n" \
		"       mean_plan_time, stddev_plan_time\n" \
		"FROM pg_stat_statements;"

struct stmtplan_t
{
	RB_ENTRY(stmtplan_t) entry;

	char queryid[NAMEDATALEN+1];
	int64_t plans;
	double total_plan_time;
	double min_plan_time;
	double max_plan_time;
	double mean_plan_time;
	double stddev_plan_time; 

};

int stmtplan_cmp(struct stmtplan_t *, struct stmtplan_t *);
static void stmtplan_info(void);
void print_stmtplan(void);
int read_stmtplan(void);
int select_stmtplan(void);
void sort_stmtplan(void);
int sort_stmtplan_queryid_callback(const void *, const void *);
int sort_stmtplan_plans_callback(const void *, const void *);
int sort_stmtplan_total_plan_time_callback(const void *, const void *);
int sort_stmtplan_min_plan_time_callback(const void *, const void *);
int sort_stmtplan_max_plan_time_callback(const void *, const void *);
int sort_stmtplan_mean_plan_time_callback(const void *, const void *);
int sort_stmtplan_stddev_plan_time_callback(const void *, const void *);

RB_HEAD(stmtplan, stmtplan_t) head_stmtplans =
    RB_INITIALIZER(&head_stmtplans);
RB_PROTOTYPE(stmtplan, stmtplan_t, entry, stmtplan_cmp)
RB_GENERATE(stmtplan, stmtplan_t, entry, stmtplan_cmp)

field_def fields_stmtplan[] = {
	{ "QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "PLANS", 6, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TOTAL_PLAN_TIME", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MIN_PLAN_TIME", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MAX_PLAN_TIME", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MEAN_PLAN_TIME", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "STDDEV_PLAN_TIME", 17, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmtplan, 0)
#define FLD_STMT_PLANS          FIELD_ADDR(fields_stmtplan, 1)
#define FLD_STMT_TOTAL_PLAN_TIME      FIELD_ADDR(fields_stmtplan, 2)
#define FLD_STMT_MIN_PLAN_TIME  FIELD_ADDR(fields_stmtplan, 3)
#define FLD_STMT_MAX_PLAN_TIME      FIELD_ADDR(fields_stmtplan, 4)
#define FLD_STMT_MEAN_PLAN_TIME FIELD_ADDR(fields_stmtplan, 5)
#define FLD_STMT_STDDEV_PLAN_TIME FIELD_ADDR(fields_stmtplan, 6)

/* Define views */
field_def *view_stmtplan_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_PLANS, FLD_STMT_TOTAL_PLAN_TIME,
	FLD_STMT_MIN_PLAN_TIME, FLD_STMT_MAX_PLAN_TIME, FLD_STMT_MEAN_PLAN_TIME,
	FLD_STMT_STDDEV_PLAN_TIME, NULL
};

order_type stmtplan_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmtplan_queryid_callback},
	{"plans", "plans", 'l', sort_stmtplan_plans_callback},
	{"total_plan_time", "total_plan_time", 't', sort_stmtplan_total_plan_time_callback},
	{"min_plan_time", "min_plan_time", 'n',
			sort_stmtplan_min_plan_time_callback},
	{"max_plan_time", "max_plan_time", 'm', sort_stmtplan_max_plan_time_callback},
	{"mean_plan_time", "mean_plan_time", 'e', sort_stmtplan_mean_plan_time_callback},
	{"stddev_plan_time", "stddev_plan_time", 'd', sort_stmtplan_stddev_plan_time_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmtplan_mgr = {
	"stmtplan", select_stmtplan, read_stmtplan, sort_stmtplan,
	print_header, print_stmtplan, keyboard_callback, stmtplan_order_list,
	stmtplan_order_list
};

field_view views_stmtplan[] = {
	{ view_stmtplan_0, "stmtplan", 'P', &stmtplan_mgr },
	{ NULL, NULL, 0, NULL }
};

int stmtplan_exist = 1;
int	stmtplan_count;
struct stmtplan_t *stmtplans;

static void
stmtplan_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct stmtplan_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		if(PQserverVersion(options.connection) / 100 < 1300){
			stmtplan_exist = 0;
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_PLAN);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = stmtplan_count;
			stmtplan_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (stmtplan_count > i) {
		p = reallocarray(stmtplans, stmtplan_count,
				sizeof(struct stmtplan_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmtplans = p;
	}

	for (i = 0; i < stmtplan_count; i++) {
		n = malloc(sizeof(struct stmtplan_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmtplan, &head_stmtplans, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		n->plans = atoll(PQgetvalue(pgresult, i, 1));
		n->total_plan_time = atof(PQgetvalue(pgresult, i, 2));
		n->min_plan_time = atof(PQgetvalue(pgresult, i, 3));
		n->max_plan_time = atof(PQgetvalue(pgresult, i, 4));
		n->mean_plan_time = atof(PQgetvalue(pgresult, i, 5));
		n->stddev_plan_time = atof(PQgetvalue(pgresult, i, 6));

		memcpy(&stmtplans[i], n, sizeof(struct stmtplan_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmtplan_cmp(struct stmtplan_t *e1, struct stmtplan_t *e2)
{
  return (e1->queryid< e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmtplan(void)
{
	return (0);
}

int
read_stmtplan(void)
{
	stmtplan_info();
	num_disp = stmtplan_count;
	return (0);
}

int
initstmtplan(void)
{
    if (pg_version() < 1300) {
        return 0;
    }

	field_view	*v;

	stmtplans = NULL;
	stmtplan_count = 0;

	read_stmtplan();
	if(stmtplan_exist == 0){
		return 0;
	}

	for (v = views_stmtplan; v->name != NULL; v++)
		add_view(v);

	return(1);
}

void
print_stmtplan(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmtplan_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_STMT_QUERYID, stmtplans[i].queryid);
            			print_fld_uint(FLD_STMT_PLANS, stmtplans[i].plans);
				print_fld_float(FLD_STMT_TOTAL_PLAN_TIME,
						stmtplans[i].total_plan_time,2);
				print_fld_float(FLD_STMT_MIN_PLAN_TIME,
						stmtplans[i].min_plan_time,2);
				print_fld_float(FLD_STMT_MAX_PLAN_TIME,
						stmtplans[i].max_plan_time,2);
				print_fld_float(FLD_STMT_MEAN_PLAN_TIME,
						stmtplans[i].mean_plan_time,2);
				print_fld_float(FLD_STMT_STDDEV_PLAN_TIME,
						stmtplans[i].stddev_plan_time,2);
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
sort_stmtplan(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmtplans == NULL)
		return;
	if (stmtplan_count <= 0)
		return;

	mergesort(stmtplans, stmtplan_count, sizeof(struct stmtplan_t),
			ordering->func);
}

int
sort_stmtplan_queryid_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmtplan_plans_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->plans < n2->plans)
		return sortdir;
	if (n1->plans > n2->plans)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}

int
sort_stmtplan_total_plan_time_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->total_plan_time < n2->total_plan_time)
		return sortdir;
	if (n1->total_plan_time > n2->total_plan_time)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}

int
sort_stmtplan_min_plan_time_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->min_plan_time < n2->min_plan_time)
		return sortdir;
	if (n1->min_plan_time > n2->min_plan_time)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}

int
sort_stmtplan_max_plan_time_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->max_plan_time < n2->max_plan_time)
		return sortdir;
	if (n1->max_plan_time > n2->max_plan_time)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}

int
sort_stmtplan_mean_plan_time_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->mean_plan_time < n2->mean_plan_time)
		return sortdir;
	if (n1->mean_plan_time > n2->mean_plan_time)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}

int 
sort_stmtplan_stddev_plan_time_callback(const void *v1, const void *v2)
{
	struct stmtplan_t *n1, *n2;
	n1 = (struct stmtplan_t *) v1;
	n2 = (struct stmtplan_t *) v2;

	if (n1->stddev_plan_time < n2->stddev_plan_time)
		return sortdir;
	if (n1->stddev_plan_time > n2->stddev_plan_time)
		return -sortdir;

	return sort_stmtplan_queryid_callback(v1, v2);
}