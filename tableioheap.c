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

#define QUERY_STATIO_TABLES_HEAP \
		"SELECT relid, schemaname, relname, heap_blks_read, heap_blks_hit\n" \
		"FROM pg_statio_all_tables;"

struct tableio_heap_t
{
	RB_ENTRY(tableio_heap_t) entry;

	long long	relid;
	char		schemaname[NAMEDATALEN + 1];
	char		relname[NAMEDATALEN + 1];

	int64_t		heap_blks_read;
	int64_t		heap_blks_read_diff;
	int64_t		heap_blks_read_old;

	int64_t		heap_blks_hit;
	int64_t		heap_blks_hit_diff;
	int64_t		heap_blks_hit_old;
};

int			tableio_heapcmp(struct tableio_heap_t *, struct tableio_heap_t *);
static void tableio_heap_info(void);
void		print_tableio_heap(void);
int			read_tableio_heap(void);
int			select_tableio_heap(void);
void		sort_tableio_heap(void);
int			sort_tableio_heap_blks_hit_callback(const void *, const void *);
int			sort_tableio_heap_blks_read_callback(const void *, const void *);
int			sort_tableio_heap_relname_callback(const void *, const void *);
int			sort_tableio_heap_schemaname_callback(const void *, const void *);

RB_HEAD(tableio_heap, tableio_heap_t) head_tableio_heaps =
RB_INITIALIZER(&head_tableio_heaps);
RB_PROTOTYPE(tableio_heap, tableio_heap_t, entry, tableio_heapcmp)
RB_GENERATE(tableio_heap, tableio_heap_t, entry, tableio_heapcmp)

field_def fields_tableio_heap[] =
{
	{
		"SCHEMA", 7, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"NAME", 5, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"HEAP_BLKS_READ", 15, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"HEAP_BLKS_HIT", 14, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_TABLEIO_SCHEMA         FIELD_ADDR(fields_tableio_heap, 0)
#define FLD_TABLEIO_NAME           FIELD_ADDR(fields_tableio_heap, 1)
#define FLD_TABLEIO_HEAP_BLKS_READ FIELD_ADDR(fields_tableio_heap, 2)
#define FLD_TABLEIO_HEAP_BLKS_HIT  FIELD_ADDR(fields_tableio_heap, 3)

/* Define views */
field_def  *view_tableio_heap_0[] = {
	FLD_TABLEIO_SCHEMA, FLD_TABLEIO_NAME, FLD_TABLEIO_HEAP_BLKS_READ,
	FLD_TABLEIO_HEAP_BLKS_HIT, NULL
};

order_type	tableio_heap_order_list[] = {
	{"schema", "schema", 's', sort_tableio_heap_schemaname_callback},
	{"name", "name", 'n', sort_tableio_heap_relname_callback},
	{"heap_blks_read", "heap_blks_read", 'i',
	sort_tableio_heap_blks_read_callback},
	{"heap_blks_hit", "heap_blks_hit", 'u',
	sort_tableio_heap_blks_hit_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager tableio_heap_mgr = {
	"tableioheap", select_tableio_heap, read_tableio_heap, sort_tableio_heap,
	print_header, print_tableio_heap, keyboard_callback,
	tableio_heap_order_list, tableio_heap_order_list
};

field_view	views_tableio_heap[] = {
	{view_tableio_heap_0, "tableioheap", 'U', &tableio_heap_mgr},
	{NULL, NULL, 0, NULL}
};

int			tableio_heap_count;
struct tableio_heap_t *tableio_heaps;

static void
tableio_heap_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct tableio_heap_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_STATIO_TABLES_HEAP);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = tableio_heap_count;
			tableio_heap_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (tableio_heap_count > i)
	{
		p = reallocarray(tableio_heaps, tableio_heap_count,
						 sizeof(struct tableio_heap_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		tableio_heaps = p;
	}

	for (i = 0; i < tableio_heap_count; i++)
	{
		n = malloc(sizeof(struct tableio_heap_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(tableio_heap, &head_tableio_heaps, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		strncpy(n->schemaname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		strncpy(n->relname, PQgetvalue(pgresult, i, 2), NAMEDATALEN);

		n->heap_blks_read_old = n->heap_blks_read;
		n->heap_blks_read = atoll(PQgetvalue(pgresult, i, 3));
		n->heap_blks_read_diff = n->heap_blks_read - n->heap_blks_read_old;

		n->heap_blks_hit_old = n->heap_blks_hit;
		n->heap_blks_hit = atoll(PQgetvalue(pgresult, i, 4));
		n->heap_blks_hit_diff = n->heap_blks_hit - n->heap_blks_hit_old;

		memcpy(&tableio_heaps[i], n, sizeof(struct tableio_heap_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
tableio_heapcmp(struct tableio_heap_t *e1, struct tableio_heap_t *e2)
{
	return (e1->relid < e2->relid ? -1 : e1->relid > e2->relid);
}

int
select_tableio_heap(void)
{
	return (0);
}

int
read_tableio_heap(void)
{
	tableio_heap_info();
	num_disp = tableio_heap_count;
	return (0);
}

int
inittableioheap(void)
{
	field_view *v;

	tableio_heaps = NULL;
	tableio_heap_count = 0;

	for (v = views_tableio_heap; v->name != NULL; v++)
		add_view(v);

	read_tableio_heap();

	return (1);
}

void
print_tableio_heap(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < tableio_heap_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_TABLEIO_SCHEMA, tableio_heaps[i].schemaname);
				print_fld_str(FLD_TABLEIO_NAME, tableio_heaps[i].relname);
				print_fld_uint(FLD_TABLEIO_HEAP_BLKS_READ,
							   tableio_heaps[i].heap_blks_read_diff);
				print_fld_uint(FLD_TABLEIO_HEAP_BLKS_HIT,
							   tableio_heaps[i].heap_blks_hit_diff);
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
sort_tableio_heap(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (tableio_heaps == NULL)
		return;
	if (tableio_heap_count <= 0)
		return;

	mergesort(tableio_heaps, tableio_heap_count, sizeof(struct tableio_heap_t),
			  ordering->func);
}

int
sort_tableio_heap_blks_read_callback(const void *v1, const void *v2)
{
	struct tableio_heap_t *n1,
			   *n2;

	n1 = (struct tableio_heap_t *) v1;
	n2 = (struct tableio_heap_t *) v2;

	if (n1->heap_blks_read_diff < n2->heap_blks_read_diff)
		return sortdir;
	if (n1->heap_blks_read_diff > n2->heap_blks_read_diff)
		return -sortdir;

	return sort_tableio_heap_relname_callback(v1, v2);
}

int
sort_tableio_heap_blks_hit_callback(const void *v1, const void *v2)
{
	struct tableio_heap_t *n1,
			   *n2;

	n1 = (struct tableio_heap_t *) v1;
	n2 = (struct tableio_heap_t *) v2;

	if (n1->heap_blks_hit_diff < n2->heap_blks_hit_diff)
		return sortdir;
	if (n1->heap_blks_hit_diff > n2->heap_blks_hit_diff)
		return -sortdir;

	return sort_tableio_heap_relname_callback(v1, v2);
}

int
sort_tableio_heap_relname_callback(const void *v1, const void *v2)
{
	struct tableio_heap_t *n1,
			   *n2;

	n1 = (struct tableio_heap_t *) v1;
	n2 = (struct tableio_heap_t *) v2;

	if (strcmp(n1->relname, n2->relname) < 0)
		return sortdir;
	if (strcmp(n1->relname, n2->relname) > 0)
		return -sortdir;

	return strcmp(n1->schemaname, n2->schemaname) * sortdir;
}

int
sort_tableio_heap_schemaname_callback(const void *v1, const void *v2)
{
	struct tableio_heap_t *n1,
			   *n2;

	n1 = (struct tableio_heap_t *) v1;
	n2 = (struct tableio_heap_t *) v2;

	if (strcmp(n1->schemaname, n2->schemaname) < 0)
		return sortdir;
	if (strcmp(n1->schemaname, n2->schemaname) > 0)
		return -sortdir;

	return strcmp(n1->relname, n2->relname) * sortdir;
}
