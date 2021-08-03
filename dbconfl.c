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

#define QUERY_STAT_DBCONFL \
		"SELECT a.datid, a.datname, conflicts, confl_tablespace,\n" \
		"       confl_lock, confl_snapshot, confl_bufferpin,\n" \
		"       confl_deadlock\n" \
		"FROM pg_stat_database a, pg_stat_database_conflicts b\n" \
		"WHERE a.datid = b.datid;"

struct dbconfl_t
{
	RB_ENTRY(dbconfl_t) entry;
	long long	datid;
	char		datname[NAMEDATALEN + 1];
	int64_t		conflicts;
	int64_t		conflicts_diff;
	int64_t		conflicts_old;
	int64_t		confl_tablespace;
	int64_t		confl_tablespace_diff;
	int64_t		confl_tablespace_old;
	int64_t		confl_lock;
	int64_t		confl_lock_diff;
	int64_t		confl_lock_old;
	int64_t		confl_snapshot;
	int64_t		confl_snapshot_diff;
	int64_t		confl_snapshot_old;
	int64_t		confl_bufferpin;
	int64_t		confl_bufferpin_diff;
	int64_t		confl_bufferpin_old;
	int64_t		confl_deadlock;
	int64_t		confl_deadlock_diff;
	int64_t		confl_deadlock_old;
};

int			dbconflcmp(struct dbconfl_t *, struct dbconfl_t *);
static void dbconfl_info(void);
void		print_dbconfl(void);
int			read_dbconfl(void);
int			select_dbconfl(void);
void		sort_dbconfl(void);
int			sort_dbconfl_bufferpin_callback(const void *, const void *);
int			sort_dbconfl_conflicts_callback(const void *, const void *);
int			sort_dbconfl_datname_callback(const void *, const void *);
int			sort_dbconfl_deadlock_callback(const void *, const void *);
int			sort_dbconfl_lock_callback(const void *, const void *);
int			sort_dbconfl_snapshot_callback(const void *, const void *);
int			sort_dbconfl_tablespace_callback(const void *, const void *);

RB_HEAD(dbconfl, dbconfl_t) head_dbconfls = RB_INITIALIZER(&head_dbconfls);
RB_PROTOTYPE(dbconfl, dbconfl_t, entry, dbconflcmp)
RB_GENERATE(dbconfl, dbconfl_t, entry, dbconflcmp)

field_def fields_dbconfl[] =
{
	{
		"DATABASE", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"CONFLICTS", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"TABLESPACE", 11, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"LOCK", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"SNAPSHOT", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"BUFFERPIN", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"DEADLOCK", 9, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
};

#define FLD_DB_DATNAME          FIELD_ADDR(fields_dbconfl, 0)
#define FLD_DB_CONFLICTS        FIELD_ADDR(fields_dbconfl, 1)
#define FLD_DB_CONFL_TABLESPACE FIELD_ADDR(fields_dbconfl, 2)
#define FLD_DB_CONFL_LOCK       FIELD_ADDR(fields_dbconfl, 3)
#define FLD_DB_CONFL_SNAPSHOT   FIELD_ADDR(fields_dbconfl, 4)
#define FLD_DB_CONFL_BUFFERPIN  FIELD_ADDR(fields_dbconfl, 5)
#define FLD_DB_CONFL_DEADLOCK   FIELD_ADDR(fields_dbconfl, 6)

/* Define views */
field_def  *view_dbconfl_0[] = {
	FLD_DB_DATNAME, FLD_DB_CONFLICTS, FLD_DB_CONFL_TABLESPACE,
	FLD_DB_CONFL_LOCK, FLD_DB_CONFL_SNAPSHOT, FLD_DB_CONFL_BUFFERPIN,
	FLD_DB_CONFL_DEADLOCK, NULL
};

order_type	dbconfl_order_list[] = {
	{"datname", "datname", 'n', sort_dbconfl_datname_callback},
	{"conflicts", "conflicts", 'c', sort_dbconfl_conflicts_callback},
	{"confl_tablespace", "confl_tablespace", 't',
	sort_dbconfl_tablespace_callback},
	{"confl_lock", "confl_lock", 'l', sort_dbconfl_lock_callback},
	{"confl_snapshot", "confl_snapshot", 's', sort_dbconfl_snapshot_callback},
	{"confl_bufferpin", "confl_bufferpin", 'b',
	sort_dbconfl_bufferpin_callback},
	{"confl_deadlock", "confl_deadlock", 'd', sort_dbconfl_deadlock_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager dbconfl_mgr = {
	"dbconfl", select_dbconfl, read_dbconfl, sort_dbconfl, print_header,
	print_dbconfl, keyboard_callback, dbconfl_order_list, dbconfl_order_list
};

field_view	views_dbconfl[] = {
	{view_dbconfl_0, "dbconfl", 'C', &dbconfl_mgr},
	{NULL, NULL, 0, NULL}
};

int			dbconfl_count;
struct dbconfl_t *dbconfls;

static void
dbconfl_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct dbconfl_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		pgresult = PQexec(options.connection, QUERY_STAT_DBCONFL);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = dbconfl_count;
			dbconfl_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (dbconfl_count > i)
	{
		p = reallocarray(dbconfls, dbconfl_count, sizeof(struct dbconfl_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		dbconfls = p;
	}

	for (i = 0; i < dbconfl_count; i++)
	{
		n = malloc(sizeof(struct dbconfl_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->datid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(dbconfl, &head_dbconfls, n);
		if (p == NULL)
			strncpy(n->datname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		else
		{
			free(n);
			n = p;
		}

		n->conflicts_old = n->conflicts;
		n->conflicts = atoll(PQgetvalue(pgresult, i, 2));
		n->conflicts_diff = n->conflicts - n->conflicts_old;

		n->confl_tablespace_old = n->confl_tablespace;
		n->confl_tablespace = atoll(PQgetvalue(pgresult, i, 3));
		n->confl_tablespace_diff = n->confl_tablespace - n->confl_tablespace_old;

		n->confl_lock_old = n->confl_lock;
		n->confl_lock = atoll(PQgetvalue(pgresult, i, 4));
		n->confl_lock_diff = n->confl_lock - n->confl_lock_old;

		n->confl_snapshot_old = n->confl_snapshot;
		n->confl_snapshot = atoll(PQgetvalue(pgresult, i, 5));
		n->confl_snapshot_diff = n->confl_snapshot - n->confl_snapshot_old;

		n->confl_bufferpin_old = n->confl_bufferpin;
		n->confl_bufferpin = atoll(PQgetvalue(pgresult, i, 6));
		n->confl_bufferpin_diff = n->confl_bufferpin - n->confl_bufferpin_old;

		n->confl_deadlock_old = n->confl_deadlock;
		n->confl_deadlock = atoll(PQgetvalue(pgresult, i, 7));
		n->confl_deadlock_diff = n->confl_deadlock - n->confl_deadlock_old;

		memcpy(&dbconfls[i], n, sizeof(struct dbconfl_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
dbconflcmp(struct dbconfl_t *e1, struct dbconfl_t *e2)
{
	return (e1->datid < e2->datid ? -1 : e1->datid > e2->datid);
}

int
select_dbconfl(void)
{
	return (0);
}

int
read_dbconfl(void)
{
	dbconfl_info();
	num_disp = dbconfl_count;
	return (0);
}

int
initdbconfl(void)
{
	field_view *v;

	dbconfls = NULL;
	dbconfl_count = 0;

	for (v = views_dbconfl; v->name != NULL; v++)
		add_view(v);

	read_dbconfl();

	return (1);
}

void
print_dbconfl(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < dbconfl_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_str(FLD_DB_DATNAME, dbconfls[i].datname);
				print_fld_ssize(FLD_DB_CONFLICTS, dbconfls[i].conflicts_diff);
				print_fld_ssize(FLD_DB_CONFL_TABLESPACE,
								dbconfls[i].confl_tablespace_diff);
				print_fld_ssize(FLD_DB_CONFL_LOCK,
								dbconfls[i].confl_lock_diff);
				print_fld_ssize(FLD_DB_CONFL_SNAPSHOT,
								dbconfls[i].confl_snapshot_diff);
				print_fld_ssize(FLD_DB_CONFL_BUFFERPIN,
								dbconfls[i].confl_bufferpin_diff);
				print_fld_ssize(FLD_DB_CONFL_DEADLOCK,
								dbconfls[i].confl_deadlock_diff);
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
sort_dbconfl(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (dbconfls == NULL)
		return;
	if (dbconfl_count <= 0)
		return;

	mergesort(dbconfls, dbconfl_count, sizeof(struct dbconfl_t), ordering->func);
}

int
sort_dbconfl_bufferpin_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->confl_bufferpin_diff < n2->confl_bufferpin_diff)
		return sortdir;
	if (n1->confl_bufferpin_diff > n2->confl_bufferpin_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}

int
sort_dbconfl_datname_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	return strcmp(n1->datname, n2->datname) * sortdir;
}

int
sort_dbconfl_conflicts_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->conflicts_diff < n2->conflicts_diff)
		return sortdir;
	if (n1->conflicts_diff > n2->conflicts_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}

int
sort_dbconfl_deadlock_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->confl_deadlock_diff < n2->confl_deadlock_diff)
		return sortdir;
	if (n1->confl_deadlock_diff > n2->confl_deadlock_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}

int
sort_dbconfl_lock_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->confl_lock_diff < n2->confl_lock_diff)
		return sortdir;
	if (n1->confl_lock_diff > n2->confl_lock_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}

int
sort_dbconfl_tablespace_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->confl_tablespace_diff < n2->confl_tablespace_diff)
		return sortdir;
	if (n1->confl_tablespace_diff > n2->confl_tablespace_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}

int
sort_dbconfl_snapshot_callback(const void *v1, const void *v2)
{
	struct dbconfl_t *n1,
			   *n2;

	n1 = (struct dbconfl_t *) v1;
	n2 = (struct dbconfl_t *) v2;

	if (n1->confl_snapshot_diff < n2->confl_snapshot_diff)
		return sortdir;
	if (n1->confl_snapshot_diff > n2->confl_snapshot_diff)
		return -sortdir;

	return sort_dbconfl_datname_callback(v1, v2);
}
