/*
 * Copyright (c) 2021 PostgreSQL Global Development Group
 */

#include <stdint.h>
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

#define QUERY_BUFFERCACHESTAT \
		"SELECT bufferid, isdirty, usagecount, pinning_backends\n" \
		"FROM pg_buffercache;"

struct buffercachestat_t
{
	RB_ENTRY(buffercachestat_t) entry;

	char		bufferid[NAMEDATALEN + 1];
	int64_t		isdirty;
	int64_t		usagecount;
	int64_t		pinning_backends;
};

int			buffercachestat_cmp(struct buffercachestat_t *, struct buffercachestat_t *);
static void buffercachestat_info(void);
void		print_buffercachestat(void);
int			read_buffercachestat(void);
int			select_buffercachestat(void);
void		sort_buffercachestat(void);
int			sort_buffercachestat_bufferid_callback(const void *, const void *);
int			sort_buffercachestat_isdirty_callback(const void *, const void *);
int			sort_buffercachestat_usagecount_callback(const void *, const void *);
int			sort_buffercachestat_pinning_backends_callback(const void *, const void *);

RB_HEAD(buffercachestat, buffercachestat_t) head_buffercachestats =
RB_INITIALIZER(&head_buffercachestats);
RB_PROTOTYPE(buffercachestat, buffercachestat_t, entry, buffercachestat_cmp)
RB_GENERATE(buffercachestat, buffercachestat_t, entry, buffercachestat_cmp)

field_def fields_buffercachestat[] =
{
	{
		"BUFFERID", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"ISDIRTY", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"USAGECOUNT", 11, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"PINNING_BACKENDS", 17, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_STMT_BUFFERID        FIELD_ADDR(fields_buffercachestat, 0)
#define FLD_STMT_ISDIRTY          FIELD_ADDR(fields_buffercachestat, 1)
#define FLD_STMT_USAGECOUNT      FIELD_ADDR(fields_buffercachestat, 2)
#define FLD_STMT_PINNING_BACKENDS  FIELD_ADDR(fields_buffercachestat, 3)

/* Define views */
field_def  *view_buffercachestat_0[] = {
	FLD_STMT_BUFFERID, FLD_STMT_ISDIRTY, FLD_STMT_USAGECOUNT,
	FLD_STMT_PINNING_BACKENDS, NULL
};

order_type	buffercachestat_order_list[] = {
	{"bufferid", "bufferid", 'u', sort_buffercachestat_bufferid_callback},
	{"isdirty", "isdirty", 'i', sort_buffercachestat_isdirty_callback},
	{"usagecount", "usagecount", 'u', sort_buffercachestat_usagecount_callback},
	{"pinning_backends", "pinning_backends", 'n',
	sort_buffercachestat_pinning_backends_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager buffercachestat_mgr = {
	"buffercachestat", select_buffercachestat, read_buffercachestat, sort_buffercachestat,
	print_header, print_buffercachestat, keyboard_callback, buffercachestat_order_list,
	buffercachestat_order_list
};

field_view	views_buffercachestat[] = {
	{view_buffercachestat_0, "buffercachestat", 'P', &buffercachestat_mgr},
	{NULL, NULL, 0, NULL}
};

int			buffercachestat_count;
struct buffercachestat_t *buffercachestats;

static void
buffercachestat_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct buffercachestat_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_BUFFERCACHESTAT);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = buffercachestat_count;
			buffercachestat_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (buffercachestat_count > i)
	{
		p = reallocarray(buffercachestats, buffercachestat_count,
						 sizeof(struct buffercachestat_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		buffercachestats = p;
	}

	for (i = 0; i < buffercachestat_count; i++)
	{
		n = malloc(sizeof(struct buffercachestat_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->bufferid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(buffercachestat, &head_buffercachestats, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		n->isdirty = atoll(PQgetvalue(pgresult, i, 1));
		n->usagecount = atoll(PQgetvalue(pgresult, i, 2));
		n->pinning_backends = atoll(PQgetvalue(pgresult, i, 3));

		memcpy(&buffercachestats[i], n, sizeof(struct buffercachestat_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
buffercachestat_cmp(struct buffercachestat_t *e1, struct buffercachestat_t *e2)
{
	return (e1->bufferid < e2->bufferid ? -1 : e1->bufferid > e2->bufferid);
}

int
select_buffercachestat(void)
{
	return (0);
}

int
read_buffercachestat(void)
{
	buffercachestat_info();
	num_disp = buffercachestat_count;
	return (0);
}

int
initbuffercachestat(void)
{
	field_view *v;

	buffercachestats = NULL;
	buffercachestat_count = 0;

	for (v = views_buffercachestat; v->name != NULL; v++)
		add_view(v);
	read_buffercachestat();

	return (1);
}

void
print_buffercachestat(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < buffercachestat_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_STMT_BUFFERID, buffercachestats[i].bufferid);
				print_fld_uint(FLD_STMT_ISDIRTY,
							   buffercachestats[i].isdirty);
				print_fld_uint(FLD_STMT_USAGECOUNT,
							   buffercachestats[i].usagecount);
				print_fld_uint(FLD_STMT_PINNING_BACKENDS,
							   buffercachestats[i].pinning_backends);
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
sort_buffercachestat(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (buffercachestats == NULL)
		return;
	if (buffercachestat_count <= 0)
		return;

	mergesort(buffercachestats, buffercachestat_count, sizeof(struct buffercachestat_t),
			  ordering->func);
}

int
sort_buffercachestat_bufferid_callback(const void *v1, const void *v2)
{
	struct buffercachestat_t *n1,
			   *n2;

	n1 = (struct buffercachestat_t *) v1;
	n2 = (struct buffercachestat_t *) v2;

	return strcmp(n1->bufferid, n2->bufferid) * sortdir;
}

int
sort_buffercachestat_isdirty_callback(const void *v1, const void *v2)
{
	struct buffercachestat_t *n1,
			   *n2;

	n1 = (struct buffercachestat_t *) v1;
	n2 = (struct buffercachestat_t *) v2;

	if (n1->isdirty < n2->isdirty)
		return sortdir;
	if (n1->isdirty > n2->isdirty)
		return -sortdir;

	return sort_buffercachestat_bufferid_callback(v1, v2);
}

int
sort_buffercachestat_usagecount_callback(const void *v1, const void *v2)
{
	struct buffercachestat_t *n1,
			   *n2;

	n1 = (struct buffercachestat_t *) v1;
	n2 = (struct buffercachestat_t *) v2;

	if (n1->usagecount < n2->usagecount)
		return sortdir;
	if (n1->usagecount > n2->usagecount)
		return -sortdir;

	return sort_buffercachestat_bufferid_callback(v1, v2);
}

int
sort_buffercachestat_pinning_backends_callback(const void *v1, const void *v2)
{
	struct buffercachestat_t *n1,
			   *n2;

	n1 = (struct buffercachestat_t *) v1;
	n2 = (struct buffercachestat_t *) v2;

	if (n1->pinning_backends < n2->pinning_backends)
		return sortdir;
	if (n1->pinning_backends > n2->pinning_backends)
		return -sortdir;

	return sort_buffercachestat_bufferid_callback(v1, v2);
}
