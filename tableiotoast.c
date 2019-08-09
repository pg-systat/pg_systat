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

#define QUERY_STATIO_TABLE_TOAST \
		"SELECT relid, schemaname, relname, toast_blks_read,\n" \
		"       toast_blks_hit\n" \
		"FROM pg_statio_all_tables;"

struct tableio_toast_t
{
	RB_ENTRY(tableio_toast_t) entry;

	long long relid;
	char schemaname[NAMEDATALEN + 1];
	char relname[NAMEDATALEN + 1];

	int64_t toast_blks_read;
	int64_t toast_blks_read_diff;
	int64_t toast_blks_read_old;

	int64_t toast_blks_hit;
	int64_t toast_blks_hit_diff;
	int64_t toast_blks_hit_old;
};

int tableio_toastcmp(struct tableio_toast_t *, struct tableio_toast_t *);
static void tableio_toast_info(void);
void print_tableio_toast(void);
int read_tableio_toast(void);
int select_tableio_toast(void);
void sort_tableio_toast(void);
int sort_tableio_toast_relname_callback(const void *, const void *);
int sort_tableio_toast_schemaname_callback(const void *, const void *);
int sort_tableio_toast_blks_hit_callback(const void *, const void *);
int sort_tableio_toast_blks_read_callback(const void *, const void *);

RB_HEAD(tableio_toast, tableio_toast_t) head_tableio_toasts =
		RB_INITIALIZER(&head_tableio_toasts);
RB_PROTOTYPE(tableio_toast, tableio_toast_t, entry, tableio_toastcmp)
RB_GENERATE(tableio_toast, tableio_toast_t, entry, tableio_toastcmp)

field_def fields_tableio_toast[] = {
	{ "SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "TOAST_BLKS_READ", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TOAST_BLKS_HIT", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_TABLEIO_SCHEMA          FIELD_ADDR(fields_tableio_toast, 0)
#define FLD_TABLEIO_NAME            FIELD_ADDR(fields_tableio_toast, 1)
#define FLD_TABLEIO_TOAST_BLKS_READ FIELD_ADDR(fields_tableio_toast, 2)
#define FLD_TABLEIO_TOAST_BLKS_HIT  FIELD_ADDR(fields_tableio_toast, 3)

/* Define views */
field_def *view_tableio_toast_0[] = {
	FLD_TABLEIO_SCHEMA, FLD_TABLEIO_NAME, FLD_TABLEIO_TOAST_BLKS_READ,
	FLD_TABLEIO_TOAST_BLKS_HIT, NULL
};

order_type tableio_toast_order_list[] = {
	{"schema", "schema", 's', sort_tableio_toast_schemaname_callback},
	{"name", "name", 'n', sort_tableio_toast_relname_callback},
	{"toast_blks_read", "toast_blks_read", 'o',
			sort_tableio_toast_blks_read_callback},
	{"toast_blks_hit", "toast_blks_hit", 'v',
			sort_tableio_toast_blks_hit_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tableio_toast_mgr = {
	"tableiotoast", select_tableio_toast, read_tableio_toast,
	sort_tableio_toast, print_header, print_tableio_toast, keyboard_callback,
	tableio_toast_order_list, tableio_toast_order_list
};

field_view views_tableio_toast[] = {
	{ view_tableio_toast_0, "tableiotoast", 'U', &tableio_toast_mgr },
	{ NULL, NULL, 0, NULL }
};

int	tableio_toast_count;
struct tableio_toast_t *tableio_toasts;

static void
tableio_toast_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct tableio_toast_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STATIO_TABLE_TOAST);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = tableio_toast_count;
			tableio_toast_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (tableio_toast_count > i) {
		p = reallocarray(tableio_toasts, tableio_toast_count,
				sizeof(struct tableio_toast_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tableio_toasts = p;
	}

	for (i = 0; i < tableio_toast_count; i++) {
		n = malloc(sizeof(struct tableio_toast_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tableio_toast, &head_tableio_toasts, n);
		if (p != NULL) {
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->toast_blks_read_old = n->toast_blks_read;
		n->toast_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->toast_blks_read_diff = n->toast_blks_read - n->toast_blks_read_old;

		n->toast_blks_hit_old = n->toast_blks_hit;
		n->toast_blks_hit = atoll(PQgetvalue(pgresult, i, 4));
		n->toast_blks_hit_diff = n->toast_blks_hit - n->toast_blks_hit_old;

		memcpy(&tableio_toasts[i], n, sizeof(struct tableio_toast_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tableio_toastcmp(struct tableio_toast_t *e1, struct tableio_toast_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tableio_toast(void)
{
	return (0);
}

int
read_tableio_toast(void)
{
	tableio_toast_info();
	num_disp = tableio_toast_count;
	return (0);
}

int
inittableiotoast(void)
{
	field_view	*v;

	tableio_toasts = NULL;
	tableio_toast_count = 0;

	for (v = views_tableio_toast; v->name != NULL; v++)
		add_view(v);

	read_tableio_toast();

	return(1);
}

void
print_tableio_toast(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tableio_toast_count; i++) {
		do {
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_TABLEIO_SCHEMA, tableio_toasts[i].schemaname);
				print_fld_str(FLD_TABLEIO_NAME, tableio_toasts[i].relname);
				print_fld_uint(FLD_TABLEIO_TOAST_BLKS_READ,
						tableio_toasts[i].toast_blks_read_diff);
				print_fld_uint(FLD_TABLEIO_TOAST_BLKS_HIT,
						tableio_toasts[i].toast_blks_hit_diff);
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
sort_tableio_toast(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tableio_toasts == NULL)
		return;
	if (tableio_toast_count <= 0)
		return;

	mergesort(tableio_toasts, tableio_toast_count,
			sizeof(struct tableio_toast_t), ordering->func);
}

int
sort_tableio_toast_relname_callback(const void *v1, const void *v2)
{
	struct tableio_toast_t *n1, *n2;
	n1 = (struct tableio_toast_t *) v1;
	n2 = (struct tableio_toast_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tableio_toast_schemaname_callback(const void *v1, const void *v2)
{
	struct tableio_toast_t *n1, *n2;
	n1 = (struct tableio_toast_t *) v1;
	n2 = (struct tableio_toast_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}

int
sort_tableio_toast_blks_read_callback(const void *v1, const void *v2)
{
	struct tableio_toast_t *n1, *n2;
	n1 = (struct tableio_toast_t *) v1;
	n2 = (struct tableio_toast_t *) v2;

	if (n1->toast_blks_read_diff < n2->toast_blks_read_diff)
		return sortdir;
	if (n1->toast_blks_read_diff > n2->toast_blks_read_diff)
		return -sortdir;

	return sort_tableio_toast_relname_callback(v1, v2);
}

int
sort_tableio_toast_blks_hit_callback(const void *v1, const void *v2)
{
	struct tableio_toast_t *n1, *n2;
	n1 = (struct tableio_toast_t *) v1;
	n2 = (struct tableio_toast_t *) v2;

	if (n1->toast_blks_hit_diff < n2->toast_blks_hit_diff)
		return sortdir;
	if (n1->toast_blks_hit_diff > n2->toast_blks_hit_diff)
		return -sortdir;

	return sort_tableio_toast_relname_callback(v1, v2);
}
