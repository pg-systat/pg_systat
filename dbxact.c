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

#define QUERY_STAT_DBXACT \
		"SELECT datid, coalesce(datname, '<shared relation objects>'),\n" \
		"       numbackends, xact_commit, xact_rollback, deadlocks\n" \
		"FROM pg_stat_database;"

struct dbxact_t
{
	RB_ENTRY(dbxact_t) entry;
	long long	datid;
	char		datname[NAMEDATALEN + 1];
	unsigned int numbackends;
	int64_t		xact_commit;
	int64_t		xact_commit_diff;
	int64_t		xact_commit_old;
	int64_t		xact_rollback;
	int64_t		xact_rollback_diff;
	int64_t		xact_rollback_old;
	int64_t		deadlocks;
	int64_t		deadlocks_diff;
	int64_t		deadlocks_old;
};

int			dbxactcmp(struct dbxact_t *, struct dbxact_t *);
static void dbxact_info(void);
void		print_dbxact(void);
int			read_dbxact(void);
int			select_dbxact(void);
void		sort_dbxact(void);
int			sort_dbxact_commit_callback(const void *, const void *);
int			sort_dbxact_datname_callback(const void *, const void *);
int			sort_dbxact_deadlocks_callback(const void *, const void *);
int			sort_dbxact_numbackends_callback(const void *, const void *);
int			sort_dbxact_rollback_callback(const void *, const void *);

RB_HEAD(dbxact, dbxact_t) head_dbxacts = RB_INITIALIZER(&head_dbxacts);
RB_PROTOTYPE(dbxact, dbxact_t, entry, dbxactcmp)
RB_GENERATE(dbxact, dbxact_t, entry, dbxactcmp)

field_def fields_dbxact[] =
{
	{
		"DATABASE", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"CONNECTIONS", 12, 12, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"COMMITS", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"COMMITS/s", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"ROLLBACKS", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"ROLLBACKS/s", 12, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"DEADLOCKS", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_DB_DATNAME       FIELD_ADDR(fields_dbxact, 0)
#define FLD_DB_NUMBACKENDS   FIELD_ADDR(fields_dbxact, 1)
#define FLD_DB_XACT_COMMIT   FIELD_ADDR(fields_dbxact, 2)
#define FLD_DB_XACT_COMMIT_RATE   FIELD_ADDR(fields_dbxact, 3)
#define FLD_DB_XACT_ROLLBACK FIELD_ADDR(fields_dbxact, 4)
#define FLD_DB_XACT_ROLLBACK_RATE   FIELD_ADDR(fields_dbxact, 5)
#define FLD_DB_DEADLOCKS     FIELD_ADDR(fields_dbxact, 6)

/* Define views */
field_def  *view_dbxact_0[] = {
	FLD_DB_DATNAME, FLD_DB_NUMBACKENDS, FLD_DB_XACT_COMMIT,
	FLD_DB_XACT_COMMIT_RATE, FLD_DB_XACT_ROLLBACK, FLD_DB_XACT_ROLLBACK_RATE,
	FLD_DB_DEADLOCKS, NULL
};

order_type	dbxact_order_list[] = {
	{"datname", "datname", 'n', sort_dbxact_datname_callback},
	{"numbackends", "numbackends", 'b', sort_dbxact_numbackends_callback},
	{"xact_commit", "xact_commit", 'c', sort_dbxact_commit_callback},
	{"xact_rollback", "xact_rollback", 'r', sort_dbxact_rollback_callback},
	{"deadlocks", "deadlocks", 'd', sort_dbxact_deadlocks_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager dbxact_mgr = {
	"dbxact", select_dbxact, read_dbxact, sort_dbxact, print_header,
	print_dbxact, keyboard_callback, dbxact_order_list, dbxact_order_list
};

field_view	views_dbxact[] = {
	{view_dbxact_0, "dbxact", 'D', &dbxact_mgr},
	{NULL, NULL, 0, NULL}
};

int			dbxact_count;
struct dbxact_t *dbxacts;

static void
dbxact_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct dbxact_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_STAT_DBXACT);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = dbxact_count;
			dbxact_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (dbxact_count > i)
	{
		p = reallocarray(dbxacts, dbxact_count, sizeof(struct dbxact_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		dbxacts = p;
	}

	for (i = 0; i < dbxact_count; i++)
	{
		n = malloc(sizeof(struct dbxact_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->datid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(dbxact, &head_dbxacts, n);
		if (p == NULL)
			strncpy(n->datname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		else
		{
			free(n);
			n = p;
		}
		n->numbackends = atoi(PQgetvalue(pgresult, i, 2));

		n->xact_commit_old = n->xact_commit;
		n->xact_commit = atoll(PQgetvalue(pgresult, i, 3));
		n->xact_commit_diff = n->xact_commit - n->xact_commit_old;

		n->xact_rollback_old = n->xact_rollback;
		n->xact_rollback = atoll(PQgetvalue(pgresult, i, 4));
		n->xact_rollback_diff = n->xact_rollback - n->xact_rollback_old;

		n->deadlocks_old = n->deadlocks;
		n->deadlocks = atoll(PQgetvalue(pgresult, i, 5));
		n->deadlocks_diff = n->deadlocks - n->deadlocks_old;

		memcpy(&dbxacts[i], n, sizeof(struct dbxact_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
dbxactcmp(struct dbxact_t *e1, struct dbxact_t *e2)
{
	return (e1->datid < e2->datid ? -1 : e1->datid > e2->datid);
}

int
select_dbxact(void)
{
	return (0);
}

int
read_dbxact(void)
{
	dbxact_info();
	num_disp = dbxact_count;
	return (0);
}

int
initdbxact(void)
{
	field_view *v;

	dbxacts = NULL;
	dbxact_count = 0;

	for (v = views_dbxact; v->name != NULL; v++)
		add_view(v);

	read_dbxact();

	return (1);
}

void
print_dbxact(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < dbxact_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_DB_DATNAME, dbxacts[i].datname);
				print_fld_uint(FLD_DB_NUMBACKENDS, dbxacts[i].numbackends);
				print_fld_ssize(FLD_DB_XACT_COMMIT,
								dbxacts[i].xact_commit_diff);
				print_fld_ssize(FLD_DB_XACT_COMMIT_RATE,
								dbxacts[i].xact_commit_diff /
								((int64_t) udelay / 1000000));
				print_fld_ssize(FLD_DB_XACT_ROLLBACK,
								dbxacts[i].xact_rollback_diff);
				print_fld_ssize(FLD_DB_XACT_ROLLBACK_RATE,
								dbxacts[i].xact_rollback_diff /
								((int64_t) udelay / 1000000));
				print_fld_ssize(FLD_DB_DEADLOCKS, dbxacts[i].deadlocks_diff);
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
sort_dbxact(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (dbxacts == NULL)
		return;
	if (dbxact_count <= 0)
		return;

	mergesort(dbxacts, dbxact_count, sizeof(struct dbxact_t), ordering->func);
}

int
sort_dbxact_commit_callback(const void *v1, const void *v2)
{
	struct dbxact_t *n1,
			   *n2;

	n1 = (struct dbxact_t *) v1;
	n2 = (struct dbxact_t *) v2;

	if (n1->xact_commit_diff < n2->xact_commit_diff)
		return sortdir;
	if (n1->xact_commit_diff > n2->xact_commit_diff)
		return -sortdir;

	return sort_dbxact_datname_callback(v1, v2);
}

int
sort_dbxact_datname_callback(const void *v1, const void *v2)
{
	struct dbxact_t *n1,
			   *n2;

	n1 = (struct dbxact_t *) v1;
	n2 = (struct dbxact_t *) v2;

	return strcmp(n1->datname, n2->datname) * sortdir;
}

int
sort_dbxact_deadlocks_callback(const void *v1, const void *v2)
{
	struct dbxact_t *n1,
			   *n2;

	n1 = (struct dbxact_t *) v1;
	n2 = (struct dbxact_t *) v2;

	if (n1->deadlocks_diff < n2->deadlocks_diff)
		return sortdir;
	if (n1->deadlocks_diff > n2->deadlocks_diff)
		return -sortdir;

	return sort_dbxact_datname_callback(v1, v2);
}

int
sort_dbxact_numbackends_callback(const void *v1, const void *v2)
{
	struct dbxact_t *n1,
			   *n2;

	n1 = (struct dbxact_t *) v1;
	n2 = (struct dbxact_t *) v2;

	if (n1->numbackends < n2->numbackends)
		return sortdir;
	if (n1->numbackends > n2->numbackends)
		return -sortdir;

	return sort_dbxact_datname_callback(v1, v2);
}

int
sort_dbxact_rollback_callback(const void *v1, const void *v2)
{
	struct dbxact_t *n1,
			   *n2;

	n1 = (struct dbxact_t *) v1;
	n2 = (struct dbxact_t *) v2;

	if (n1->xact_rollback_diff < n2->xact_rollback_diff)
		return sortdir;
	if (n1->xact_rollback_diff > n2->xact_rollback_diff)
		return -sortdir;

	return sort_dbxact_datname_callback(v1, v2);
}
