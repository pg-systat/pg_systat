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

#define QUERY_STAT_DBTUP \
		"SELECT datid, coalesce(datname, '<shared object relations>'),\n" \
		"       tup_returned, tup_fetched, tup_inserted, tup_updated,\n" \
		"       tup_deleted\n" \
		"FROM pg_stat_database;"

struct dbtup_t
{
	RB_ENTRY(dbtup_t) entry;
	long long datid;
	char datname[NAMEDATALEN + 1];
	int64_t tup_returned;
	int64_t tup_returned_diff;
	int64_t tup_returned_old;
	int64_t tup_fetched;
	int64_t tup_fetched_diff;
	int64_t tup_fetched_old;
	int64_t tup_inserted;
	int64_t tup_inserted_diff;
	int64_t tup_inserted_old;
	int64_t tup_updated;
	int64_t tup_updated_diff;
	int64_t tup_updated_old;
	int64_t tup_deleted;
	int64_t tup_deleted_diff;
	int64_t tup_deleted_old;
};

int dbtupcmp(struct dbtup_t *, struct dbtup_t *);
void print_dbtup(void);
int read_dbtup(void);
int select_dbtup(void);
static void dbtup_info(void);
void sort_dbtup(void);
int sort_dbtup_datname_callback(const void *, const void *);
int sort_dbtup_deleted_callback(const void *, const void *);
int sort_dbtup_fetched_callback(const void *, const void *);
int sort_dbtup_inserted_callback(const void *, const void *);
int sort_dbtup_returned_callback(const void *, const void *);
int sort_dbtup_updated_callback(const void *, const void *);

RB_HEAD(dbtup, dbtup_t) head_dbtups = RB_INITIALIZER(&head_dbtups);
RB_PROTOTYPE(dbtup, dbtup_t, entry, dbtupcmp)
RB_GENERATE(dbtup, dbtup_t, entry, dbtupcmp)

field_def fields_dbtup[] = {
	{ "DATABASE", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "R/s", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "W/s", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "RETURNED", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "FETCHED", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "INSERTED", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "UPDATED", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "DELETED", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_DB_DATNAME       FIELD_ADDR(fields_dbtup, 0)
#define FLD_DB_TUP_R_S       FIELD_ADDR(fields_dbtup, 1)
#define FLD_DB_TUP_W_S       FIELD_ADDR(fields_dbtup, 2)
#define FLD_DB_TUP_RETURNED  FIELD_ADDR(fields_dbtup, 3)
#define FLD_DB_TUP_FETCHED   FIELD_ADDR(fields_dbtup, 4)
#define FLD_DB_TUP_INSERTED  FIELD_ADDR(fields_dbtup, 5)
#define FLD_DB_TUP_UPDATED   FIELD_ADDR(fields_dbtup, 6)
#define FLD_DB_TUP_DELETED   FIELD_ADDR(fields_dbtup, 7)

/* Define views */
field_def *view_dbtup_0[] = {
	FLD_DB_DATNAME, FLD_DB_TUP_R_S, FLD_DB_TUP_W_S, FLD_DB_TUP_RETURNED,
	FLD_DB_TUP_FETCHED, FLD_DB_TUP_INSERTED, FLD_DB_TUP_UPDATED,
	FLD_DB_TUP_DELETED, NULL
};

order_type dbtup_order_list[] = {
	{"datname", "datname", 'n', sort_dbtup_datname_callback},
	{"tup_returned", "tup_returned", 'r', sort_dbtup_returned_callback},
	{"tup_fetched", "tup_fetched", 'f', sort_dbtup_fetched_callback},
	{"tup_inserted", "tup_inserted", 'i', sort_dbtup_inserted_callback},
	{"tup_updated", "tup_updated", 'u', sort_dbtup_updated_callback},
	{"tup_deleted", "tup_deleted", 'd', sort_dbtup_deleted_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager dbtup_mgr = {
	"dbtub", select_dbtup, read_dbtup, sort_dbtup, print_header, print_dbtup,
	keyboard_callback, dbtup_order_list, dbtup_order_list
};

field_view views_dbtup[] = {
	{ view_dbtup_0, "dbtup", 'T', &dbtup_mgr },
	{ NULL, NULL, 0, NULL }
};

int	  dbtup_count;
struct dbtup_t *dbtups;

static void
dbtup_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct dbtup_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_DBTUP);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = dbtup_count;
			dbtup_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (dbtup_count > i) {
		p = reallocarray(dbtups, dbtup_count, sizeof(struct dbtup_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		dbtups = p;
	}

	for (i = 0; i < dbtup_count; i++) {
		n = malloc(sizeof(struct dbtup_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->datid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(dbtup, &head_dbtups, n);
		if (p == NULL)
			strncpy(n->datname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		else {
			free(n);
			n = p;
		}
		n->tup_returned_old = n->tup_returned;
		n->tup_returned = atoll(PQgetvalue(pgresult, i, 2));
		n->tup_returned_diff = n->tup_returned - n->tup_returned_old;

		n->tup_fetched_old = n->tup_fetched;
		n->tup_fetched = atoll(PQgetvalue(pgresult, i, 3));
		n->tup_fetched_diff = n->tup_fetched - n->tup_fetched_old;

		n->tup_inserted_old = n->tup_inserted;
		n->tup_inserted = atoll(PQgetvalue(pgresult, i, 4));
		n->tup_inserted_diff = n->tup_inserted - n->tup_inserted_old;

		n->tup_updated_old = n->tup_updated;
		n->tup_updated = atoll(PQgetvalue(pgresult, i, 5));
		n->tup_updated_diff = n->tup_updated - n->tup_updated_old;

		n->tup_deleted_old = n->tup_deleted;
		n->tup_deleted = atoll(PQgetvalue(pgresult, i, 6));
		n->tup_deleted_diff = n->tup_deleted - n->tup_deleted_old;

		memcpy(&dbtups[i], n, sizeof(struct dbtup_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
dbtupcmp(struct dbtup_t *e1, struct dbtup_t *e2)
{
	return (e1->datid < e2->datid ? -1 : e1->datid > e2->datid);
}

int
select_dbtup(void)
{
	return (0);
}

int
read_dbtup(void)
{
	dbtup_info();
	num_disp = dbtup_count;
	return (0);
}

int
initdbtup(void)
{
	field_view	*v;

	dbtups = NULL;
	dbtup_count = 0;

	for (v = views_dbtup; v->name != NULL; v++)
		add_view(v);

	read_dbtup();

	return(1);
}

void
print_dbtup(void)
{
	int		cur = 0, i;
	int		end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < dbtup_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_DB_DATNAME, dbtups[i].datname);
				print_fld_ssize(FLD_DB_TUP_R_S,
						dbtups[i].tup_returned_diff /
								((int64_t) udelay / 1000000));
				print_fld_ssize(FLD_DB_TUP_W_S,
						(dbtups[i].tup_inserted_diff +
						dbtups[i].tup_updated_diff +
						dbtups[i].tup_deleted_diff) /
						((int64_t) udelay / 1000000));
				print_fld_ssize(FLD_DB_TUP_RETURNED,
						dbtups[i].tup_returned_diff);
				print_fld_ssize(FLD_DB_TUP_FETCHED,
						dbtups[i].tup_fetched_diff);
				print_fld_ssize(FLD_DB_TUP_INSERTED,
						dbtups[i].tup_inserted_diff);
				print_fld_ssize(FLD_DB_TUP_UPDATED,
						dbtups[i].tup_updated_diff);
				print_fld_ssize(FLD_DB_TUP_DELETED,
						dbtups[i].tup_deleted_diff);
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
sort_dbtup(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (dbtups == NULL)
		return;
	if (dbtup_count <= 0)
		return;

	mergesort(dbtups, dbtup_count, sizeof(struct dbtup_t), ordering->func);
}
int
sort_dbtup_datname_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	return strcmp(n1->datname, n2->datname) * sortdir;
}

int
sort_dbtup_deleted_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	if (n1->tup_deleted_diff < n2->tup_deleted_diff)
		return sortdir;
	if (n1->tup_deleted_diff > n2->tup_deleted_diff)
		return -sortdir;

	return sort_dbtup_datname_callback(v1, v2);
}

int
sort_dbtup_fetched_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	if (n1->tup_fetched_diff < n2->tup_fetched_diff)
		return sortdir;
	if (n1->tup_fetched_diff > n2->tup_fetched_diff)
		return -sortdir;

	return sort_dbtup_datname_callback(v1, v2);
}

int
sort_dbtup_inserted_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	if (n1->tup_inserted_diff < n2->tup_inserted_diff)
		return sortdir;
	if (n1->tup_inserted_diff > n2->tup_inserted_diff)
		return -sortdir;

	return sort_dbtup_datname_callback(v1, v2);
}

int
sort_dbtup_returned_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	if (n1->tup_returned_diff < n2->tup_returned_diff)
		return sortdir;
	if (n1->tup_returned_diff > n2->tup_returned_diff)
		return -sortdir;

	return sort_dbtup_datname_callback(v1, v2);
}

int
sort_dbtup_updated_callback(const void *v1, const void *v2)
{
	struct dbtup_t *n1, *n2;
	n1 = (struct dbtup_t *) v1;
	n2 = (struct dbtup_t *) v2;

	if (n1->tup_updated_diff < n2->tup_updated_diff)
		return sortdir;
	if (n1->tup_updated_diff > n2->tup_updated_diff)
		return -sortdir;

	return sort_dbtup_datname_callback(v1, v2);
}
