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
#include "pgstat.h"

#define QUERY_STAT_TABLES \
		"SELECT relid, schemaname, relname, n_tup_ins, n_tup_upd,\n" \
		"       n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup\n" \
		"FROM pg_stat_all_tables;"

struct tabletup_t
{
	RB_ENTRY(tabletup_t) entry;

	long long relid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];

	int64_t n_tup_ins;
	int64_t n_tup_ins_diff;
	int64_t n_tup_ins_old;

	int64_t n_tup_upd;
	int64_t n_tup_upd_diff;
	int64_t n_tup_upd_old;

	int64_t n_tup_del;
	int64_t n_tup_del_diff;
	int64_t n_tup_del_old;

	int64_t n_tup_hot_upd;
	int64_t n_tup_hot_upd_diff;
	int64_t n_tup_hot_upd_old;

	int64_t n_live_tup;
	int64_t n_dead_tup;
};

int tabletupcmp(struct tabletup_t *, struct tabletup_t *);
static void tabletup_info(void);
void print_tabletup(void);
int read_tabletup(void);
int select_tabletup(void);
void sort_tabletup(void);
int sort_tabletup_relname_callback(const void *, const void *);
int sort_tabletup_schemaname_callback(const void *, const void *);
int sort_tabletup_n_dead_tup_callback(const void *, const void *);
int sort_tabletup_n_live_tup_callback(const void *, const void *);
int sort_tabletup_n_tup_del_callback(const void *, const void *);
int sort_tabletup_n_tup_hot_upd_callback(const void *, const void *);
int sort_tabletup_n_tup_ins_callback(const void *, const void *);
int sort_tabletup_n_tup_upd_callback(const void *, const void *);

RB_HEAD(tabletup, tabletup_t) head_tabletups = RB_INITIALIZER(&head_tabletups);
RB_PROTOTYPE(tabletup, tabletup_t, entry, tabletupcmp)
RB_GENERATE(tabletup, tabletup_t, entry, tabletupcmp)

field_def fields_tabletup[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "INS", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "UPD", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "DEL", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "HOT_UPD", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LIVE", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "DEAD", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_TABLE_SCHEMA        FIELD_ADDR(fields_tabletup, 0)
#define FLD_TABLE_NAME          FIELD_ADDR(fields_tabletup, 1)
#define FLD_TABLE_N_TUP_INS     FIELD_ADDR(fields_tabletup, 2)
#define FLD_TABLE_N_TUP_UPD     FIELD_ADDR(fields_tabletup, 3)
#define FLD_TABLE_N_TUP_DEL     FIELD_ADDR(fields_tabletup, 4)
#define FLD_TABLE_N_TUP_HOT_UPD FIELD_ADDR(fields_tabletup, 5)
#define FLD_TABLE_N_LIVE_TUP    FIELD_ADDR(fields_tabletup, 6)
#define FLD_TABLE_N_DEAD_TUP    FIELD_ADDR(fields_tabletup, 7)

/* Define views */
field_def *view_tabletup_0[] = {
	FLD_TABLE_SCHEMA, FLD_TABLE_NAME, FLD_TABLE_N_TUP_INS, FLD_TABLE_N_TUP_UPD,
	FLD_TABLE_N_TUP_DEL, FLD_TABLE_N_TUP_HOT_UPD, FLD_TABLE_N_LIVE_TUP,
	FLD_TABLE_N_DEAD_TUP, NULL
};

order_type tabletup_order_list[] = {
	{"schema", "schema", 's', sort_tabletup_schemaname_callback},
	{"name", "name", 'n', sort_tabletup_relname_callback},
	{"n_tup_ins", "n_tup_ins", 'i', sort_tabletup_n_tup_ins_callback},
	{"n_tup_upd", "n_tup_upd", 'u', sort_tabletup_n_tup_upd_callback},
	{"n_tup_del", "n_tup_del", 'd', sort_tabletup_n_tup_del_callback},
	{"n_tup_hot_upd", "n_tup_hot_upd", 'h', sort_tabletup_n_tup_upd_callback},
	{"n_live_tup", "n_live_tup", 'V', sort_tabletup_n_live_tup_callback},
	{"n_dead_tup", "n_dead_tup", 'e', sort_tabletup_n_dead_tup_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tabletup_mgr = {
	"tabletup", select_tabletup, read_tabletup, sort_tabletup, print_header,
	print_tabletup, keyboard_callback, tabletup_order_list, tabletup_order_list
};

field_view views_tabletup[] = {
	{ view_tabletup_0, "tabletup", 'U', &tabletup_mgr },
	{ NULL, NULL, 0, NULL }
};

int	tabletup_count;
struct tabletup_t *tabletups;

static void
tabletup_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct tabletup_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_TABLES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = tabletup_count;
			tabletup_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (tabletup_count > i) {
		p = reallocarray(tabletups, tabletup_count, sizeof(struct tabletup_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tabletups = p;
	}

	for (i = 0; i < tabletup_count; i++) {
		n = malloc(sizeof(struct tabletup_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tabletup, &head_tabletups, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->n_tup_ins_old = n->n_tup_ins;
		n->n_tup_ins = atoll(PQgetvalue(pgresult, i, 3));
		n->n_tup_ins_diff = n->n_tup_ins - n->n_tup_ins_old;

		n->n_tup_upd_old = n->n_tup_upd;
		n->n_tup_upd = atoll(PQgetvalue(pgresult, i, 3));
		n->n_tup_upd_diff = n->n_tup_upd - n->n_tup_upd_old;

		n->n_tup_del_old = n->n_tup_del;
		n->n_tup_del = atoll(PQgetvalue(pgresult, i, 4));
		n->n_tup_del_diff = n->n_tup_del - n->n_tup_del_old;

		n->n_tup_hot_upd_old = n->n_tup_hot_upd;
		n->n_tup_hot_upd = atoll(PQgetvalue(pgresult, i, 5));
		n->n_tup_hot_upd_diff = n->n_tup_hot_upd - n->n_tup_hot_upd_old;

		n->n_live_tup = atoll(PQgetvalue(pgresult, i, 6));
		n->n_dead_tup = atoll(PQgetvalue(pgresult, i, 7));

		memcpy(&tabletups[i], n, sizeof(struct tabletup_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tabletupcmp(struct tabletup_t *e1, struct tabletup_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tabletup(void)
{
	return (0);
}

int
read_tabletup(void)
{
	tabletup_info();
	num_disp = tabletup_count;
	return (0);
}

int
inittabletup(void)
{
	field_view	*v;

	tabletups = NULL;
	tabletup_count = 0;

	for (v = views_tabletup; v->name != NULL; v++)
		add_view(v);

	read_tabletup();

	return(1);
}

void
print_tabletup(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tabletup_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_TABLE_SCHEMA, tabletups[i].schemaname);
				print_fld_str(FLD_TABLE_NAME, tabletups[i].relname);
				print_fld_uint(FLD_TABLE_N_TUP_INS,
						tabletups[i].n_tup_ins_diff);
				print_fld_uint(FLD_TABLE_N_TUP_UPD,
						tabletups[i].n_tup_upd_diff);
				print_fld_uint(FLD_TABLE_N_TUP_DEL,
						tabletups[i].n_tup_del_diff);
				print_fld_uint(FLD_TABLE_N_TUP_HOT_UPD,
						tabletups[i].n_tup_hot_upd_diff);
				print_fld_uint(FLD_TABLE_N_LIVE_TUP, tabletups[i].n_live_tup);
				print_fld_uint(FLD_TABLE_N_DEAD_TUP, tabletups[i].n_dead_tup);
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
sort_tabletup(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tabletups == NULL)
		return;
	if (tabletup_count <= 0)
		return;

	mergesort(tabletups, tabletup_count, sizeof(struct tabletup_t),
			ordering->func);
}

int
sort_tabletup_n_dead_tup_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_dead_tup < n2->n_dead_tup)
		return sortdir;
	if (n1->n_dead_tup > n2->n_dead_tup)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_n_live_tup_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_live_tup < n2->n_live_tup)
		return sortdir;
	if (n1->n_live_tup > n2->n_live_tup)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_n_tup_del_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_tup_del_diff < n2->n_tup_del_diff)
		return sortdir;
	if (n1->n_tup_del_diff > n2->n_tup_del_diff)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_n_tup_hot_upd_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_tup_hot_upd_diff < n2->n_tup_hot_upd_diff)
		return sortdir;
	if (n1->n_tup_hot_upd_diff > n2->n_tup_hot_upd_diff)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_n_tup_ins_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_tup_ins_diff < n2->n_tup_ins_diff)
		return sortdir;
	if (n1->n_tup_ins_diff > n2->n_tup_ins_diff)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_n_tup_upd_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (n1->n_tup_upd_diff < n2->n_tup_upd_diff)
		return sortdir;
	if (n1->n_tup_upd_diff > n2->n_tup_upd_diff)
		return -sortdir;

	return sort_tabletup_relname_callback(v1, v2);
}

int
sort_tabletup_relname_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tabletup_schemaname_callback(const void *v1, const void *v2)
{
	struct tabletup_t *n1, *n2;
	n1 = (struct tabletup_t *) v1;
	n2 = (struct tabletup_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
