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
		"SELECT relid, schemaname, relname, n_mod_since_analyze,\n" \
		"       last_analyze, last_autoanalyze, analyze_count,\n" \
		"       autoanalyze_count\n" \
		"FROM pg_stat_all_tables;"

struct tableanalyze_t
{
	RB_ENTRY(tableanalyze_t) entry;

	long long relid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];

	int64_t n_mod_since_analyze;

	char last_analyze[TIMESTAMPLEN + 1];
	char last_autoanalyze[TIMESTAMPLEN + 1];

	int64_t analyze_count;
	int64_t autoanalyze_count;
};

int tableanalyzecmp(struct tableanalyze_t *, struct tableanalyze_t *);
static void tableanalyze_info(void);
void print_tableanalyze(void);
int read_tableanalyze(void);
int select_tableanalyze(void);
void sort_tableanalyze(void);
int sort_tableanalyze_n_mod_since_analyze_callback(const void *,
		const void *);
int sort_tableanalyze_relname_callback(const void *, const void *);
int sort_tableanalyze_schemaname_callback(const void *, const void *);
int sort_tableanalyze_analyze_count_callback(const void *, const void *);
int sort_tableanalyze_autoanalyze_count_callback(const void *, const void *);

RB_HEAD(tableanalyze, tableanalyze_t) head_tableanalyze =
		RB_INITIALIZER(&head_tableanalyze);
RB_PROTOTYPE(tableanalyze, tableanalyze_t, entry, tableanalyzecmp)
RB_GENERATE(tableanalyze, tableanalyze_t, entry, tableanalyzecmp)

field_def fields_tableanalyze[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "N_MOD", 6, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "LAST_ANALYZE", 13, 29, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "LAST_AUTOANALYZE", 17, 29, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "ANALYZE_COUNT", 14, 19, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "AUTOANALYZE_COUNT", 18, 19, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
};

#define FLD_TABLEANALYZE_SCHEMA              FIELD_ADDR(fields_tableanalyze, 0)
#define FLD_TABLEANALYZE_NAME                FIELD_ADDR(fields_tableanalyze, 1)
#define FLD_TABLEANALYZE_N_MOD_SINCE_ANALYZE FIELD_ADDR(fields_tableanalyze, 2)
#define FLD_TABLEANALYZE_LAST_ANALYZE        FIELD_ADDR(fields_tableanalyze, 3)
#define FLD_TABLEANALYZE_LAST_AUTOANALYZE    FIELD_ADDR(fields_tableanalyze, 4)
#define FLD_TABLEANALYZE_ANALYZE_COUNT       FIELD_ADDR(fields_tableanalyze, 5)
#define FLD_TABLEANALYZE_AUTOANALYZE_COUNT   FIELD_ADDR(fields_tableanalyze, 6)

/* Define views */
field_def *view_tableanalyze_0[] = {
	FLD_TABLEANALYZE_SCHEMA, FLD_TABLEANALYZE_NAME,
	FLD_TABLEANALYZE_N_MOD_SINCE_ANALYZE, FLD_TABLEANALYZE_LAST_ANALYZE,
	FLD_TABLEANALYZE_LAST_AUTOANALYZE, FLD_TABLEANALYZE_ANALYZE_COUNT,
	FLD_TABLEANALYZE_AUTOANALYZE_COUNT, NULL
};

order_type tableanalyze_order_list[] = {
	{"schema", "schema", 's', sort_tableanalyze_schemaname_callback},
	{"name", "name", 'n', sort_tableanalyze_relname_callback},
	{"n_mod_since_analyze", "n_mod_since_analyze", 'm',
			sort_tableanalyze_n_mod_since_analyze_callback},
	{"analyze_count", "analyze_count", 'v',
			sort_tableanalyze_analyze_count_callback},
	{"autoanalyze_count", "autoanalyze_count", 'V',
			sort_tableanalyze_autoanalyze_count_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tableanalyze_mgr = {
	"tableanalyze", select_tableanalyze, read_tableanalyze, sort_tableanalyze,
	print_header, print_tableanalyze, keyboard_callback,
	tableanalyze_order_list, tableanalyze_order_list
};

field_view views_tableanalyze[] = {
	{ view_tableanalyze_0, "tableanalyze", 'T', &tableanalyze_mgr },
	{ NULL, NULL, 0, NULL }
};

int	tableanalyze_count;
struct tableanalyze_t *tableanalyzes;

static void
tableanalyze_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct tableanalyze_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_TABLES);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = tableanalyze_count;
			tableanalyze_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (tableanalyze_count > i) {
		p = reallocarray(tableanalyzes, tableanalyze_count,
				sizeof(struct tableanalyze_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tableanalyzes = p;
	}

	for (i = 0; i < tableanalyze_count; i++) {
		n = malloc(sizeof(struct tableanalyze_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tableanalyze, &head_tableanalyze, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->n_mod_since_analyze = atoll(PQgetvalue(pgresult, i, 3));

		strncpy(n->last_analyze, PQgetvalue(pgresult, i, 4), TIMESTAMPLEN);
		strncpy(n->last_autoanalyze, PQgetvalue(pgresult, i, 5), TIMESTAMPLEN);

		n->analyze_count = atoll(PQgetvalue(pgresult, i, 6));
		n->autoanalyze_count = atoll(PQgetvalue(pgresult, i, 7));

		memcpy(&tableanalyzes[i], n, sizeof(struct tableanalyze_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tableanalyzecmp(struct tableanalyze_t *e1, struct tableanalyze_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tableanalyze(void)
{
	return (0);
}

int
read_tableanalyze(void)
{
	tableanalyze_info();
	num_disp = tableanalyze_count;
	return (0);
}

int
inittableanalyze(void)
{
	field_view	*v;

	tableanalyzes = NULL;
	tableanalyze_count = 0;

	for (v = views_tableanalyze; v->name != NULL; v++)
		add_view(v);

	read_tableanalyze();

	return(1);
}

void
print_tableanalyze(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tableanalyze_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_TABLEANALYZE_SCHEMA,
						tableanalyzes[i].schemaname);
				print_fld_str(FLD_TABLEANALYZE_NAME, tableanalyzes[i].relname);
				print_fld_uint(FLD_TABLEANALYZE_N_MOD_SINCE_ANALYZE,
						tableanalyzes[i].n_mod_since_analyze);
				print_fld_str(FLD_TABLEANALYZE_LAST_ANALYZE,
						tableanalyzes[i].last_analyze);
				print_fld_str(FLD_TABLEANALYZE_LAST_AUTOANALYZE,
						tableanalyzes[i].last_autoanalyze);
				print_fld_uint(FLD_TABLEANALYZE_ANALYZE_COUNT,
						tableanalyzes[i].analyze_count);
				print_fld_uint(FLD_TABLEANALYZE_AUTOANALYZE_COUNT,
						tableanalyzes[i].autoanalyze_count);
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
sort_tableanalyze(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tableanalyzes == NULL)
		return;
	if (tableanalyze_count <= 0)
		return;

	mergesort(tableanalyzes, tableanalyze_count, sizeof(struct tableanalyze_t),
			ordering->func);
}

int
sort_tableanalyze_n_mod_since_analyze_callback(const void *v1, const void *v2)
{
	struct tableanalyze_t *n1, *n2;
	n1 = (struct tableanalyze_t *) v1;
	n2 = (struct tableanalyze_t *) v2;

	if (n1->n_mod_since_analyze < n2->n_mod_since_analyze)
		return sortdir;
	if (n1->n_mod_since_analyze > n2->n_mod_since_analyze)
		return -sortdir;

	return sort_tableanalyze_relname_callback(v1, v2);
}

int
sort_tableanalyze_relname_callback(const void *v1, const void *v2)
{
	struct tableanalyze_t *n1, *n2;
	n1 = (struct tableanalyze_t *) v1;
	n2 = (struct tableanalyze_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tableanalyze_schemaname_callback(const void *v1, const void *v2)
{
	struct tableanalyze_t *n1, *n2;
	n1 = (struct tableanalyze_t *) v1;
	n2 = (struct tableanalyze_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}

int
sort_tableanalyze_analyze_count_callback(const void *v1, const void *v2)
{
	struct tableanalyze_t *n1, *n2;
	n1 = (struct tableanalyze_t *) v1;
	n2 = (struct tableanalyze_t *) v2;

	if (n1->analyze_count < n2->analyze_count)
		return sortdir;
	if (n1->analyze_count > n2->analyze_count)
		return -sortdir;

	return sort_tableanalyze_relname_callback(v1, v2);
}

int
sort_tableanalyze_autoanalyze_count_callback(const void *v1, const void *v2)
{
	struct tableanalyze_t *n1, *n2;
	n1 = (struct tableanalyze_t *) v1;
	n2 = (struct tableanalyze_t *) v2;

	if (n1->autoanalyze_count < n2->autoanalyze_count)
		return sortdir;
	if (n1->autoanalyze_count > n2->autoanalyze_count)
		return -sortdir;

	return sort_tableanalyze_relname_callback(v1, v2);
}
