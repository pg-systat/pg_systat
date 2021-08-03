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

#define QUERY_STATIO_TABLE_TIDX \
		"SELECT relid, schemaname, relname, tidx_blks_read, tidx_blks_hit\n" \
		"FROM pg_statio_all_tables;"

struct tableio_tidx_t
{
	RB_ENTRY(tableio_tidx_t) entry;

	long long	relid;
	char		schemaname[NAMEDATALEN + 1];
	char		relname[NAMEDATALEN + 1];

	int64_t		tidx_blks_read;
	int64_t		tidx_blks_read_diff;
	int64_t		tidx_blks_read_old;

	int64_t		tidx_blks_hit;
	int64_t		tidx_blks_hit_diff;
	int64_t		tidx_blks_hit_old;
};

int			tableio_tidxcmp(struct tableio_tidx_t *, struct tableio_tidx_t *);
static void tableio_tidx_info(void);
void		print_tableio_tidx(void);
int			read_tableio_tidx(void);
int			select_tableio_tidx(void);
void		sort_tableio_tidx(void);
int			sort_tableio_tidx_blks_hit_callback(const void *, const void *);
int			sort_tableio_tidx_blks_read_callback(const void *, const void *);
int			sort_tableio_tidx_relname_callback(const void *, const void *);
int			sort_tableio_tidx_schemaname_callback(const void *, const void *);

RB_HEAD(tableiotidx, tableio_tidx_t) head_tableio_tidxs =
RB_INITIALIZER(&head_tableio_tidxs);
RB_PROTOTYPE(tableiotidx, tableio_tidx_t, entry, tableio_tidxcmp)
RB_GENERATE(tableiotidx, tableio_tidx_t, entry, tableio_tidxcmp)

field_def fields_tableio_tidx[] =
{
	{
		"SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"TIDX_BLKS_READ", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"TIDX_BLKS_HIT", 13, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_TABLEIO_TIDX_SCHEMA    FIELD_ADDR(fields_tableio_tidx, 0)
#define FLD_TABLEIO_TIDX_NAME      FIELD_ADDR(fields_tableio_tidx, 1)
#define FLD_TABLEIO_TIDX_BLKS_READ FIELD_ADDR(fields_tableio_tidx, 2)
#define FLD_TABLEIO_TIDX_BLKS_HIT  FIELD_ADDR(fields_tableio_tidx, 3)

/* Define views */
field_def  *view_tableio_tidx_0[] = {
	FLD_TABLEIO_TIDX_SCHEMA, FLD_TABLEIO_TIDX_NAME, FLD_TABLEIO_TIDX_BLKS_READ,
	FLD_TABLEIO_TIDX_BLKS_HIT, NULL
};

order_type	tableio_tidx_order_list[] = {
	{"schema", "schema", 's', sort_tableio_tidx_schemaname_callback},
	{"name", "name", 'n', sort_tableio_tidx_relname_callback},
	{"tidx_blks_read", "tidx_blks_read", 'd',
	sort_tableio_tidx_blks_read_callback},
	{"tidx_blks_hit", "tidx_blks_hit", 'h', sort_tableio_tidx_blks_hit_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tableio_tidx_mgr = {
	"tableiotidx", select_tableio_tidx, read_tableio_tidx, sort_tableio_tidx,
	print_header, print_tableio_tidx, keyboard_callback, tableio_tidx_order_list,
	tableio_tidx_order_list
};

field_view	views_tableio_tidx[] = {
	{view_tableio_tidx_0, "tableiotidx", 'U', &tableio_tidx_mgr},
	{NULL, NULL, 0, NULL}
};

int			tableio_tidx_count;
struct tableio_tidx_t *tableio_tidxs;

static void
tableio_tidx_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct tableio_tidx_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_STATIO_TABLE_TIDX);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = tableio_tidx_count;
			tableio_tidx_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (tableio_tidx_count > i)
	{
		p = reallocarray(tableio_tidxs, tableio_tidx_count, sizeof(struct
																   tableio_tidx_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tableio_tidxs = p;
	}

	for (i = 0; i < tableio_tidx_count; i++)
	{
		n = malloc(sizeof(struct tableio_tidx_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tableiotidx, &head_tableio_tidxs, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->tidx_blks_read_old = n->tidx_blks_read;
		n->tidx_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->tidx_blks_read_diff = n->tidx_blks_read - n->tidx_blks_read_old;

		n->tidx_blks_hit_old = n->tidx_blks_hit;
		n->tidx_blks_hit = atoll(PQgetvalue(pgresult, i, 4));
		n->tidx_blks_hit_diff = n->tidx_blks_hit - n->tidx_blks_hit_old;

		memcpy(&tableio_tidxs[i], n, sizeof(struct tableio_tidx_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tableio_tidxcmp(struct tableio_tidx_t *e1, struct tableio_tidx_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tableio_tidx(void)
{
	return (0);
}

int
read_tableio_tidx(void)
{
	tableio_tidx_info();
	num_disp = tableio_tidx_count;
	return (0);
}

int
inittableiotidx(void)
{
	field_view *v;

	tableio_tidxs = NULL;
	tableio_tidx_count = 0;

	for (v = views_tableio_tidx; v->name != NULL; v++)
		add_view(v);

	read_tableio_tidx();

	return (1);
}

void
print_tableio_tidx(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tableio_tidx_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_TABLEIO_TIDX_SCHEMA,
							  tableio_tidxs[i].schemaname);
				print_fld_str(FLD_TABLEIO_TIDX_NAME, tableio_tidxs[i].relname);
				print_fld_uint(FLD_TABLEIO_TIDX_BLKS_READ,
							   tableio_tidxs[i].tidx_blks_read_diff);
				print_fld_uint(FLD_TABLEIO_TIDX_BLKS_HIT,
							   tableio_tidxs[i].tidx_blks_hit_diff);
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
sort_tableio_tidx(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tableio_tidxs == NULL)
		return;
	if (tableio_tidx_count <= 0)
		return;

	mergesort(tableio_tidxs, tableio_tidx_count, sizeof(struct tableio_tidx_t),
			  ordering->func);
}

int
sort_tableio_tidx_blks_read_callback(const void *v1, const void *v2)
{
	struct tableio_tidx_t *n1,
			   *n2;

	n1 = (struct tableio_tidx_t *) v1;
	n2 = (struct tableio_tidx_t *) v2;

	if (n1->tidx_blks_read_diff < n2->tidx_blks_read_diff)
		return sortdir;
	if (n1->tidx_blks_read_diff > n2->tidx_blks_read_diff)
		return -sortdir;

	return sort_tableio_tidx_relname_callback(v1, v2);
}

int
sort_tableio_tidx_blks_hit_callback(const void *v1, const void *v2)
{
	struct tableio_tidx_t *n1,
			   *n2;

	n1 = (struct tableio_tidx_t *) v1;
	n2 = (struct tableio_tidx_t *) v2;

	if (n1->tidx_blks_hit_diff < n2->tidx_blks_hit_diff)
		return sortdir;
	if (n1->tidx_blks_hit_diff > n2->tidx_blks_hit_diff)
		return -sortdir;

	return sort_tableio_tidx_relname_callback(v1, v2);
}

int
sort_tableio_tidx_relname_callback(const void *v1, const void *v2)
{
	struct tableio_tidx_t *n1,
			   *n2;

	n1 = (struct tableio_tidx_t *) v1;
	n2 = (struct tableio_tidx_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tableio_tidx_schemaname_callback(const void *v1, const void *v2)
{
	struct tableio_tidx_t *n1,
			   *n2;

	n1 = (struct tableio_tidx_t *) v1;
	n2 = (struct tableio_tidx_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
