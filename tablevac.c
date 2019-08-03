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
		"SELECT relid, schemaname, relname, last_vacuum, last_autovacuum,\n" \
		"       vacuum_count, autovacuum_count\n" \
		"FROM pg_stat_all_tables;"

struct tablevac_t
{
	RB_ENTRY(tablevac_t) entry;

	long long relid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];

	char last_vacuum[TIMESTAMPLEN + 1];
	char last_autovacuum[TIMESTAMPLEN + 1];

	int64_t vacuum_count;
	int64_t autovacuum_count;
};

int tablevaccmp(struct tablevac_t *, struct tablevac_t *);
static void tablevac_info(void);
void print_tablevac(void);
int read_tablevac(void);
int select_tablevac(void);
void sort_tablevac(void);
int sort_tablevac_autovacuum_count_callback(const void *, const void *);
int sort_tablevac_relname_callback(const void *, const void *);
int sort_tablevac_schemaname_callback(const void *, const void *);
int sort_tablevac_vacuum_count_callback(const void *, const void *);

RB_HEAD(tablevac, tablevac_t) head_tablevacs = RB_INITIALIZER(&head_tablevacs);
RB_PROTOTYPE(tablevac, tablevac_t, entry, tablevaccmp)
RB_GENERATE(tablevac, tablevac_t, entry, tablevaccmp)

field_def fields_tablevac[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "LAST_VACUUM", 12, 29, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "LAST_AUTOVACUUM", 16, 29, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "VACUUM_COUNT", 13, 19, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "AUTOVACUUM_COUNT", 17, 19, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
};

#define FLD_TABLEVAC_SCHEMA              FIELD_ADDR(fields_tablevac, 0)
#define FLD_TABLEVAC_NAME                FIELD_ADDR(fields_tablevac, 1)
#define FLD_TABLEVAC_LAST_VACUUM         FIELD_ADDR(fields_tablevac, 2)
#define FLD_TABLEVAC_LAST_AUTOVACUUM     FIELD_ADDR(fields_tablevac, 3)
#define FLD_TABLEVAC_VACUUM_COUNT        FIELD_ADDR(fields_tablevac, 4)
#define FLD_TABLEVAC_AUTOVACUUM_COUNT    FIELD_ADDR(fields_tablevac, 5)

/* Define views */
field_def *view_tablevac_0[] = {
	FLD_TABLEVAC_SCHEMA, FLD_TABLEVAC_NAME, FLD_TABLEVAC_LAST_VACUUM,
	FLD_TABLEVAC_LAST_AUTOVACUUM, FLD_TABLEVAC_VACUUM_COUNT,
	FLD_TABLEVAC_AUTOVACUUM_COUNT, NULL
};

order_type tablevac_order_list[] = {
	{"schema", "schema", 's', sort_tablevac_schemaname_callback},
	{"name", "name", 'n', sort_tablevac_relname_callback},
	{"vacuum_count", "vacuum_count", 'v', sort_tablevac_vacuum_count_callback},
	{"autovacuum_count", "autovacuum_count", 'a',
			sort_tablevac_autovacuum_count_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tablevac_mgr = {
	"tablevac", select_tablevac, read_tablevac, sort_tablevac, print_header,
	print_tablevac, keyboard_callback, tablevac_order_list, tablevac_order_list
};

field_view views_tablevac[] = {
	{ view_tablevac_0, "tablevac", 'T', &tablevac_mgr },
	{ NULL, NULL, 0, NULL }
};

int	tablevac_count;
struct tablevac_t *tablevacs;

static void
tablevac_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct tablevac_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_TABLES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = tablevac_count;
			tablevac_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (tablevac_count > i) {
		p = reallocarray(tablevacs, tablevac_count, sizeof(struct tablevac_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tablevacs = p;
	}

	for (i = 0; i < tablevac_count; i++) {
		n = malloc(sizeof(struct tablevac_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tablevac, &head_tablevacs, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		strncpy(n->last_vacuum, PQgetvalue(pgresult, i, 3), TIMESTAMPLEN);
		strncpy(n->last_autovacuum, PQgetvalue(pgresult, i, 4), TIMESTAMPLEN);

		n->vacuum_count = atoll(PQgetvalue(pgresult, i, 5));
		n->autovacuum_count = atoll(PQgetvalue(pgresult, i, 6));

		memcpy(&tablevacs[i], n, sizeof(struct tablevac_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tablevaccmp(struct tablevac_t *e1, struct tablevac_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tablevac(void)
{
	return (0);
}

int
read_tablevac(void)
{
	tablevac_info();
	num_disp = tablevac_count;
	return (0);
}

int
inittablevac(void)
{
	field_view	*v;

	tablevacs = NULL;
	tablevac_count = 0;

	for (v = views_tablevac; v->name != NULL; v++)
		add_view(v);

	read_tablevac();

	return(1);
}

void
print_tablevac(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tablevac_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_TABLEVAC_SCHEMA, tablevacs[i].schemaname);
				print_fld_str(FLD_TABLEVAC_NAME, tablevacs[i].relname);
				print_fld_str(FLD_TABLEVAC_LAST_VACUUM,
						tablevacs[i].last_vacuum);
				print_fld_str(FLD_TABLEVAC_LAST_AUTOVACUUM,
						tablevacs[i].last_autovacuum);
				print_fld_uint(FLD_TABLEVAC_VACUUM_COUNT,
						tablevacs[i].vacuum_count);
				print_fld_uint(FLD_TABLEVAC_AUTOVACUUM_COUNT,
						tablevacs[i].autovacuum_count);
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
sort_tablevac(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tablevacs == NULL)
		return;
	if (tablevac_count <= 0)
		return;

	mergesort(tablevacs, tablevac_count, sizeof(struct tablevac_t),
			ordering->func);
}

int
sort_tablevac_autovacuum_count_callback(const void *v1, const void *v2)
{
	struct tablevac_t *n1, *n2;
	n1 = (struct tablevac_t *) v1;
	n2 = (struct tablevac_t *) v2;

	if (n1->autovacuum_count < n2->autovacuum_count)
		return sortdir;
	if (n1->autovacuum_count > n2->autovacuum_count)
		return -sortdir;

	return sort_tablevac_relname_callback(v1, v2);
}

int
sort_tablevac_relname_callback(const void *v1, const void *v2)
{
	struct tablevac_t *n1, *n2;
	n1 = (struct tablevac_t *) v1;
	n2 = (struct tablevac_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tablevac_schemaname_callback(const void *v1, const void *v2)
{
	struct tablevac_t *n1, *n2;
	n1 = (struct tablevac_t *) v1;
	n2 = (struct tablevac_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}

int
sort_tablevac_vacuum_count_callback(const void *v1, const void *v2)
{
	struct tablevac_t *n1, *n2;
	n1 = (struct tablevac_t *) v1;
	n2 = (struct tablevac_t *) v2;

	if (n1->vacuum_count < n2->vacuum_count)
		return sortdir;
	if (n1->vacuum_count > n2->vacuum_count)
		return -sortdir;

	return sort_tablevac_relname_callback(v1, v2);
}
