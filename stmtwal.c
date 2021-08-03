/*
 * Copyright (c) 2021 PostgreSQL Global Development Group
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

#define QUERY_STAT_WAL \
		"SELECT queryid, wal_records, wal_fpi, wal_bytes\n" \
		"FROM pg_stat_statements;"

struct stmtwal_t
{
	RB_ENTRY(stmtwal_t) entry;

	char		queryid[NAMEDATALEN + 1];
	int64_t		wal_records;
	int64_t		wal_fpi;
	int64_t		wal_bytes;

};

int			stmtwal_cmp(struct stmtwal_t *, struct stmtwal_t *);
static void stmtwal_info(void);
void		print_stmtwal(void);
int			read_stmtwal(void);
int			select_stmtwal(void);
void		sort_stmtwal(void);
int			sort_stmtwal_queryid_callback(const void *, const void *);
int			sort_stmtwal_wal_records_callback(const void *, const void *);
int			sort_stmtwal_wal_fpi_callback(const void *, const void *);
int			sort_stmtwal_wal_bytes_callback(const void *, const void *);

RB_HEAD(stmtwal, stmtwal_t) head_stmtwals = RB_INITIALIZER(&head_stmtwals);
RB_PROTOTYPE(stmtwal, stmtwal_t, entry, stmtwal_cmp)
RB_GENERATE(stmtwal, stmtwal_t, entry, stmtwal_cmp)

field_def fields_stmtwal[] =
{
	{
		"QUERYID", 8, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"WAL_RECORDS", 12, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"WAL_FPI", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"WAL_BYTES", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_STMT_QUERYID        FIELD_ADDR(fields_stmtwal, 0)
#define FLD_STMT_WAL_RECORDS          FIELD_ADDR(fields_stmtwal, 1)
#define FLD_STMT_WAL_FPI      FIELD_ADDR(fields_stmtwal, 2)
#define FLD_STMT_WAL_BYTES  FIELD_ADDR(fields_stmtwal, 3)

/* Define views */
field_def  *view_stmtwal_0[] = {
	FLD_STMT_QUERYID, FLD_STMT_WAL_RECORDS, FLD_STMT_WAL_FPI,
	FLD_STMT_WAL_BYTES, NULL
};

order_type	stmtwal_order_list[] = {
	{"queryid", "queryid", 'u', sort_stmtwal_queryid_callback},
	{"wal_records", "wal_records", 'e', sort_stmtwal_wal_records_callback},
	{"wal_fpi", "wal_fpi", 'f', sort_stmtwal_wal_fpi_callback},
	{"wal_bytes", "wal_bytes", 'v', sort_stmtwal_wal_bytes_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager stmtwal_mgr = {
	"stmtwal", select_stmtwal, read_stmtwal, sort_stmtwal,
	print_header, print_stmtwal, keyboard_callback, stmtwal_order_list,
	stmtwal_order_list
};

field_view	views_stmtwal[] = {
	{view_stmtwal_0, "stmtwal", 'w', &stmtwal_mgr},
	{NULL, NULL, 0, NULL}
};

int			stmtwal_exist = 1;
int			stmtwal_count;
struct stmtwal_t *stmtwals;

static void
stmtwal_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct stmtwal_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		if (PQserverVersion(options.connection) / 100 < 1300)
		{
			stmtwal_exist = 0;
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_STMT_EXIST);
		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK || PQntuples(pgresult) == 0)
		{
			stmtwal_exist = 0;
			PQclear(pgresult);
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_WAL);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = stmtwal_count;
			stmtwal_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (stmtwal_count > i)
	{
		p = reallocarray(stmtwals, stmtwal_count,
						 sizeof(struct stmtwal_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		stmtwals = p;
	}

	for (i = 0; i < stmtwal_count; i++)
	{
		n = malloc(sizeof(struct stmtwal_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		strncpy(n->queryid, PQgetvalue(pgresult, i, 0), NAMEDATALEN);
		p = RB_INSERT(stmtwal, &head_stmtwals, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		n->wal_records = atoll(PQgetvalue(pgresult, i, 1));
		n->wal_fpi = atoll(PQgetvalue(pgresult, i, 2));
		n->wal_bytes = atoll(PQgetvalue(pgresult, i, 3));

		memcpy(&stmtwals[i], n, sizeof(struct stmtwal_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
stmtwal_cmp(struct stmtwal_t *e1, struct stmtwal_t *e2)
{
	return (e1->queryid < e2->queryid ? -1 : e1->queryid > e2->queryid);
}

int
select_stmtwal(void)
{
	return (0);
}

int
read_stmtwal(void)
{
	stmtwal_info();
	num_disp = stmtwal_count;
	return (0);
}

int
initstmtwal(void)
{
	if (pg_version() < 1300)
	{
		return 0;
	}

	field_view *v;

	stmtwals = NULL;
	stmtwal_count = 0;

	read_stmtwal();
	if (stmtwal_exist == 0)
	{
		return 0;
	}

	for (v = views_stmtwal; v->name != NULL; v++)
		add_view(v);

	return (1);
}

void
print_stmtwal(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < stmtwal_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_STMT_QUERYID, stmtwals[i].queryid);
				print_fld_uint(FLD_STMT_WAL_RECORDS, stmtwals[i].wal_records);
				print_fld_uint(FLD_STMT_WAL_FPI, stmtwals[i].wal_fpi);
				print_fld_uint(FLD_STMT_WAL_BYTES, stmtwals[i].wal_bytes);
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
sort_stmtwal(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (stmtwals == NULL)
		return;
	if (stmtwal_count <= 0)
		return;

	mergesort(stmtwals, stmtwal_count, sizeof(struct stmtwal_t),
			  ordering->func);
}

int
sort_stmtwal_queryid_callback(const void *v1, const void *v2)
{
	struct stmtwal_t *n1,
			   *n2;

	n1 = (struct stmtwal_t *) v1;
	n2 = (struct stmtwal_t *) v2;

	return strcmp(n1->queryid, n2->queryid) * sortdir;
}

int
sort_stmtwal_wal_records_callback(const void *v1, const void *v2)
{
	struct stmtwal_t *n1,
			   *n2;

	n1 = (struct stmtwal_t *) v1;
	n2 = (struct stmtwal_t *) v2;

	if (n1->wal_records < n2->wal_records)
		return sortdir;
	if (n1->wal_records > n2->wal_records)
		return -sortdir;

	return sort_stmtwal_queryid_callback(v1, v2);
}

int
sort_stmtwal_wal_fpi_callback(const void *v1, const void *v2)
{
	struct stmtwal_t *n1,
			   *n2;

	n1 = (struct stmtwal_t *) v1;
	n2 = (struct stmtwal_t *) v2;

	if (n1->wal_fpi < n2->wal_fpi)
		return sortdir;
	if (n1->wal_fpi > n2->wal_fpi)
		return -sortdir;

	return sort_stmtwal_queryid_callback(v1, v2);
}

int
sort_stmtwal_wal_bytes_callback(const void *v1, const void *v2)
{
	struct stmtwal_t *n1,
			   *n2;

	n1 = (struct stmtwal_t *) v1;
	n2 = (struct stmtwal_t *) v2;

	if (n1->wal_bytes < n2->wal_bytes)
		return sortdir;
	if (n1->wal_bytes > n2->wal_bytes)
		return -sortdir;

	return sort_stmtwal_queryid_callback(v1, v2);
}
