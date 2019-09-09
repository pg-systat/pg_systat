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

#define QUERY_STAT_DBXACT \
        "SELECT pg_stat_progress_vacuum.pid, nspname, relname, phase,\n" \
        "       heap_blks_total, heap_blks_scanned, heap_blks_vacuumed,\n" \
        "       index_vacuum_count, max_dead_tuples, num_dead_tuples\n" \
        "FROM pg_stat_progress_vacuum\n" \
        "JOIN pg_class\n" \
        "  ON pg_stat_progress_vacuum.relid = pg_class.oid\n" \
        "JOIN pg_namespace\n" \
        "ON pg_class.relnamespace = pg_namespace.oid;"

struct vacuum_t
{
	RB_ENTRY(vacuum_t) entry;
	long long pid;
	char nspname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];
	char phase[NAMEDATALEN + 1];
	int64_t heap_blks_total;
	int64_t heap_blks_scanned;
	int64_t heap_blks_vacuumed;
	int64_t index_vacuum_count;
	int64_t max_dead_tuples;
	int64_t num_dead_tuples;
};

int vacuumcmp(struct vacuum_t *, struct vacuum_t *);
static void vacuum_info(void);
void print_vacuum(void);
int read_vacuum(void);
int select_vacuum(void);
void sort_vacuum(void);
int sort_vacuum_nspname_callback(const void *, const void *);
int sort_vacuum_phase_callback(const void *, const void *);
int sort_vacuum_relname_callback(const void *, const void *);
int sort_vacuum_heap_blks_scanned_callback(const void *, const void *);
int sort_vacuum_heap_blks_total_callback(const void *, const void *);
int sort_vacuum_heap_blks_vacuumed_callback(const void *, const void *);
int sort_vacuum_index_vacuum_count_callback(const void *, const void *);
int sort_vacuum_max_dead_tuples_callback(const void *, const void *);
int sort_vacuum_num_dead_tuples_callback(const void *, const void *);

RB_HEAD(vacuum, vacuum_t) head_vacuums = RB_INITIALIZER(&head_vacuums);
RB_PROTOTYPE(vacuum, vacuum_t, entry, vacuumcmp)
RB_GENERATE(vacuum, vacuum_t, entry, vacuumcmp)

field_def fields_vacuum[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "TABLENAME", 10, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "PHASE", 6, 25, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "HEAP_BLKS_TOTAL", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "HEAP_BLKS_SCANNED", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "HEAP_BLKS_VACUUMED", 11, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "INDEX_VACUUM_COUNT", 11, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "MAX_DEAD_TUPLES", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "NUM_DEAD_TUPLES", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_VACUUM_NSPNAME            FIELD_ADDR(fields_vacuum, 0)
#define FLD_VACUUM_RELNAME            FIELD_ADDR(fields_vacuum, 1)
#define FLD_VACUUM_PHASE              FIELD_ADDR(fields_vacuum, 2)
#define FLD_VACUUM_HEAP_BLKS_TOTAL    FIELD_ADDR(fields_vacuum, 3)
#define FLD_VACUUM_HEAP_BLKS_SCANNED  FIELD_ADDR(fields_vacuum, 4)
#define FLD_VACUUM_HEAP_BLKS_VACUUMED FIELD_ADDR(fields_vacuum, 5)
#define FLD_VACUUM_INDEX_VACUUM_COUNT FIELD_ADDR(fields_vacuum, 6)
#define FLD_VACUUM_MAX_DEAD_TUPLES    FIELD_ADDR(fields_vacuum, 7)
#define FLD_VACUUM_NUM_DEAD_TUPLES    FIELD_ADDR(fields_vacuum, 8)

/* Define views */
field_def *view_vacuum_0[] = {
    FLD_VACUUM_NSPNAME, FLD_VACUUM_RELNAME, FLD_VACUUM_PHASE,
    FLD_VACUUM_HEAP_BLKS_TOTAL, FLD_VACUUM_HEAP_BLKS_SCANNED,
    FLD_VACUUM_HEAP_BLKS_VACUUMED, FLD_VACUUM_INDEX_VACUUM_COUNT,
    FLD_VACUUM_MAX_DEAD_TUPLES, FLD_VACUUM_NUM_DEAD_TUPLES, NULL
};

order_type vacuum_order_list[] = {
	{"nspname", "nspname", 'n', sort_vacuum_nspname_callback},
	{"relname", "relname", 'b', sort_vacuum_relname_callback},
	{"phase", "phase", 'p', sort_vacuum_phase_callback},
	{"heap_blks_total", "heap_blks_total", 't',
            sort_vacuum_heap_blks_total_callback},
	{"heap_blks_scanned", "heap_blks_scanned", 't',
            sort_vacuum_heap_blks_scanned_callback},
	{"heap_blks_vacuumed", "heap_blks_vacuumed", 't',
            sort_vacuum_heap_blks_vacuumed_callback},
	{"index_vacuum_count", "index_vacuum_count", 't',
            sort_vacuum_index_vacuum_count_callback},
	{"max_dead_tuples", "max_dead_tuples", 't',
            sort_vacuum_max_dead_tuples_callback},
	{"num_dead_tuples", "num_dead_tuples", 't',
            sort_vacuum_num_dead_tuples_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager vacuum_mgr = {
	"vacuum", select_vacuum, read_vacuum, sort_vacuum, print_header,
	print_vacuum, keyboard_callback, vacuum_order_list, vacuum_order_list
};

field_view views_vacuum[] = {
	{ view_vacuum_0, "vacuum", 'V', &vacuum_mgr },
	{ NULL, NULL, 0, NULL }
};

int	vacuum_count;
struct vacuum_t *vacuums;

static void
vacuum_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct vacuum_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_DBXACT);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = vacuum_count;
			vacuum_count = PQntuples(pgresult);
		} else {
			if (strcmp(PQresultErrorField(pgresult,
					PG_DIAG_SQLSTATE), "42P01") == 0)
				error("PostgreSQL 9.6+ required for vacuum view");
			return;
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (vacuum_count > i) {
		p = reallocarray(vacuums, vacuum_count, sizeof(struct vacuum_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		vacuums = p;
	}

	for (i = 0; i < vacuum_count; i++) {
		n = malloc(sizeof(struct vacuum_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->pid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(vacuum, &head_vacuums, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->nspname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);
		strncpy(n->phase, PQgetvalue(pgresult, i, 3), NAMEDATALEN);
		n->heap_blks_total = atoi(PQgetvalue(pgresult, i, 4));
		n->heap_blks_scanned = atoi(PQgetvalue(pgresult, i, 5));
		n->heap_blks_vacuumed = atoi(PQgetvalue(pgresult, i, 6));
		n->index_vacuum_count = atoi(PQgetvalue(pgresult, i, 7));
		n->max_dead_tuples = atoi(PQgetvalue(pgresult, i, 8));
		n->num_dead_tuples = atoi(PQgetvalue(pgresult, i, 9));

		memcpy(&vacuums[i], n, sizeof(struct vacuum_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
vacuumcmp(struct vacuum_t *e1, struct vacuum_t *e2)
{
	return (e1->pid < e2->pid ? -1 : e1->pid > e2->pid);
}

int
select_vacuum(void)
{
	return (0);
}

int
read_vacuum(void)
{
	vacuum_info();
	num_disp = vacuum_count;
	return (0);
}

int
initvacuum(void)
{
	field_view	*v;

	vacuums = NULL;
	vacuum_count = 0;

	for (v = views_vacuum; v->name != NULL; v++)
		add_view(v);

	read_vacuum();

	return(1);
}

void
print_vacuum(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < vacuum_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_VACUUM_NSPNAME, vacuums[i].nspname);
				print_fld_str(FLD_VACUUM_RELNAME, vacuums[i].relname);
				print_fld_ssize(FLD_VACUUM_HEAP_BLKS_TOTAL,
						vacuums[i].heap_blks_total);
				print_fld_ssize(FLD_VACUUM_HEAP_BLKS_SCANNED,
						vacuums[i].heap_blks_scanned);
				print_fld_ssize(FLD_VACUUM_HEAP_BLKS_VACUUMED,
						vacuums[i].heap_blks_vacuumed);
				print_fld_ssize(FLD_VACUUM_INDEX_VACUUM_COUNT,
						vacuums[i].index_vacuum_count);
				print_fld_ssize(FLD_VACUUM_MAX_DEAD_TUPLES,
						vacuums[i].max_dead_tuples);
				print_fld_ssize(FLD_VACUUM_NUM_DEAD_TUPLES,
						vacuums[i].num_dead_tuples);
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
sort_vacuum(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (vacuums == NULL)
		return;
	if (vacuum_count <= 0)
		return;

	mergesort(vacuums, vacuum_count, sizeof(struct vacuum_t), ordering->func);
}

int
sort_vacuum_nspname_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	return strcmp(n1->nspname, n2->nspname) * sortdir;
}

int
sort_vacuum_phase_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (strcmp(n1->phase, n2->phase) < 0)
		return sortdir;
	if (strcmp(n1->phase, n2->phase) > 0)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_relname_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_heap_blks_scanned_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->heap_blks_scanned < n2->heap_blks_scanned)
		return sortdir;
	if (n1->heap_blks_scanned > n2->heap_blks_scanned)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_heap_blks_total_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->heap_blks_total < n2->heap_blks_total)
		return sortdir;
	if (n1->heap_blks_total > n2->heap_blks_total)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_heap_blks_vacuumed_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->heap_blks_vacuumed < n2->heap_blks_vacuumed)
		return sortdir;
	if (n1->heap_blks_vacuumed > n2->heap_blks_vacuumed)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_index_vacuum_count_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->index_vacuum_count < n2->index_vacuum_count)
		return sortdir;
	if (n1->index_vacuum_count > n2->index_vacuum_count)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_max_dead_tuples_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->max_dead_tuples < n2->max_dead_tuples)
		return sortdir;
	if (n1->max_dead_tuples > n2->max_dead_tuples)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}

int
sort_vacuum_num_dead_tuples_callback(const void *v1, const void *v2)
{
	struct vacuum_t *n1, *n2;
	n1 = (struct vacuum_t *) v1;
	n2 = (struct vacuum_t *) v2;

	if (n1->num_dead_tuples < n2->num_dead_tuples)
		return sortdir;
	if (n1->num_dead_tuples > n2->num_dead_tuples)
		return -sortdir;

	return sort_vacuum_relname_callback(v1, v2);
}
