/*
 * Copyright (c) 2019 PostgreSQL Global Development Group
 */

#include <stdlib.h>
#ifdef __linux__
#include <bsd/stdlib.h>
#include <bsd/sys/tree.h>
#endif							/* __linux__ */
#include <string.h>
#include <unistd.h>
#include <signal.h>

#include "pg.h"
#include "pg_systat.h"

#define QUERY_STATIO_TABLES_IDX \
		"SELECT relid, schemaname, relname, idx_blks_read, idx_blks_hit\n" \
		"FROM pg_statio_all_tables;"

struct tableio_idx_t
{
	RB_ENTRY(tableio_idx_t) entry;

	long long	relid;
	char		schemaname[NAMEDATALEN + 1];
	char		relname[NAMEDATALEN + 1];

	int64_t		idx_blks_read;
	int64_t		idx_blks_read_diff;
	int64_t		idx_blks_read_old;

	int64_t		idx_blks_hit;
	int64_t		idx_blks_hit_diff;
	int64_t		idx_blks_hit_old;
};

int			tableio_idxcmp(struct tableio_idx_t *, struct tableio_idx_t *);
static void tableio_idx_info(void);
void		print_tableio_idx(void);
int			read_tableio_idx(void);
int			select_tableio_idx(void);
void		sort_tableio_idx(void);
int			sort_tableio_idx_blks_hit_callback(const void *, const void *);
int			sort_tableio_idx_blks_read_callback(const void *, const void *);
int			sort_tableio_idx_relname_callback(const void *, const void *);
int			sort_tableio_idx_schemaname_callback(const void *, const void *);

RB_HEAD(tableioidx, tableio_idx_t) head_tableio_idxs =
RB_INITIALIZER(&head_tableio_idxs);
RB_PROTOTYPE(tableioidx, tableio_idx_t, entry, tableio_idxcmp)
RB_GENERATE(tableioidx, tableio_idx_t, entry, tableio_idxcmp)

field_def fields_tableio_idx[] =
{
	{
		"SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"IDX_BLKS_READ", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"IDX_BLKS_HIT", 13, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_TABLEIO_IDX_SCHEMA          FIELD_ADDR(fields_tableio_idx, 0)
#define FLD_TABLEIO_IDX_NAME            FIELD_ADDR(fields_tableio_idx, 1)
#define FLD_TABLEIO_IDX_BLKS_READ   FIELD_ADDR(fields_tableio_idx, 2)
#define FLD_TABLEIO_IDX_BLKS_HIT    FIELD_ADDR(fields_tableio_idx, 3)

/* Define views */
field_def  *view_tableio_idx_0[] = {
	FLD_TABLEIO_IDX_SCHEMA, FLD_TABLEIO_IDX_NAME, FLD_TABLEIO_IDX_BLKS_READ,
	FLD_TABLEIO_IDX_BLKS_HIT, NULL
};

order_type	tableio_idx_order_list[] = {
	{"schema", "schema", 's', sort_tableio_idx_schemaname_callback},
	{"name", "name", 'n', sort_tableio_idx_relname_callback},
	{"idx_blks_read", "idx_blks_read", 'd',
	sort_tableio_idx_blks_read_callback},
	{"idx_blks_hit", "idx_blks_hit", 'h', sort_tableio_idx_blks_hit_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tableio_idx_mgr = {
	"tableioidx", select_tableio_idx, read_tableio_idx, sort_tableio_idx,
	print_header, print_tableio_idx, keyboard_callback, tableio_idx_order_list,
	tableio_idx_order_list
};

field_view	views_tableio_idx[] = {
	{view_tableio_idx_0, "tableioidx", 'U', &tableio_idx_mgr},
	{NULL, NULL, 0, NULL}
};

int			tableio_idx_count;
struct tableio_idx_t *tableio_idxs;

static void
tableio_idx_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct tableio_idx_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_STATIO_TABLES_IDX);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = tableio_idx_count;
			tableio_idx_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (tableio_idx_count > i)
	{
		p = reallocarray(tableio_idxs, tableio_idx_count, sizeof(struct
																 tableio_idx_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tableio_idxs = p;
	}

	for (i = 0; i < tableio_idx_count; i++)
	{
		n = malloc(sizeof(struct tableio_idx_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tableioidx, &head_tableio_idxs, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->idx_blks_read_old = n->idx_blks_read;
		n->idx_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->idx_blks_read_diff = n->idx_blks_read - n->idx_blks_read_old;

		n->idx_blks_hit_old = n->idx_blks_hit;
		n->idx_blks_hit = atoll(PQgetvalue(pgresult, i, 4));
		n->idx_blks_hit_diff = n->idx_blks_hit - n->idx_blks_hit_old;

		memcpy(&tableio_idxs[i], n, sizeof(struct tableio_idx_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tableio_idxcmp(struct tableio_idx_t *e1, struct tableio_idx_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tableio_idx(void)
{
	return (0);
}

int
read_tableio_idx(void)
{
	tableio_idx_info();
	num_disp = tableio_idx_count;
	return (0);
}

int
inittableioidx(void)
{
	field_view *v;

	tableio_idxs = NULL;
	tableio_idx_count = 0;

	for (v = views_tableio_idx; v->name != NULL; v++)
		add_view(v);

	read_tableio_idx();

	return (1);
}

void
print_tableio_idx(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tableio_idx_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_TABLEIO_IDX_SCHEMA,
							  tableio_idxs[i].schemaname);
				print_fld_str(FLD_TABLEIO_IDX_NAME, tableio_idxs[i].relname);
				print_fld_uint(FLD_TABLEIO_IDX_BLKS_READ,
							   tableio_idxs[i].idx_blks_read_diff);
				print_fld_uint(FLD_TABLEIO_IDX_BLKS_HIT,
							   tableio_idxs[i].idx_blks_hit_diff);
				end_line();
			}
			if (++cur >= end)
				return;
		} while (0);
	}

	do
	{
		if (cur >= dispstart && cur < end)
			end_line();
		if (++cur >= end)
			return;
	} while (0);
}

void
sort_tableio_idx(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tableio_idxs == NULL)
		return;
	if (tableio_idx_count <= 0)
		return;

	mergesort(tableio_idxs, tableio_idx_count, sizeof(struct tableio_idx_t),
			  ordering->func);
}

int
sort_tableio_idx_blks_read_callback(const void *v1, const void *v2)
{
	struct tableio_idx_t *n1,
			   *n2;

	n1 = (struct tableio_idx_t *) v1;
	n2 = (struct tableio_idx_t *) v2;

	if (n1->idx_blks_read_diff < n2->idx_blks_read_diff)
		return sortdir;
	if (n1->idx_blks_read_diff > n2->idx_blks_read_diff)
		return -sortdir;

	return sort_tableio_idx_relname_callback(v1, v2);
}

int
sort_tableio_idx_blks_hit_callback(const void *v1, const void *v2)
{
	struct tableio_idx_t *n1,
			   *n2;

	n1 = (struct tableio_idx_t *) v1;
	n2 = (struct tableio_idx_t *) v2;

	if (n1->idx_blks_hit_diff < n2->idx_blks_hit_diff)
		return sortdir;
	if (n1->idx_blks_hit_diff > n2->idx_blks_hit_diff)
		return -sortdir;

	return sort_tableio_idx_relname_callback(v1, v2);
}

int
sort_tableio_idx_relname_callback(const void *v1, const void *v2)
{
	struct tableio_idx_t *n1,
			   *n2;

	n1 = (struct tableio_idx_t *) v1;
	n2 = (struct tableio_idx_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tableio_idx_schemaname_callback(const void *v1, const void *v2)
{
	struct tableio_idx_t *n1,
			   *n2;

	n1 = (struct tableio_idx_t *) v1;
	n2 = (struct tableio_idx_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
