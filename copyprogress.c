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

#define QUERY_STAT_COPY_PROCESS \
		"SELECT pid, relid, command, type, bytes_processed,\n" \
		"bytes_total, tuples_processed, tuples_excluded\n"\
		"FROM pg_stat_progress_copy;"

struct copyprogress_t
{
	RB_ENTRY(copyprogress_t) entry;

	int64_t		pid;
	int64_t		relid;
	char		command[NAMEDATALEN + 1];
	char		type[NAMEDATALEN + 1];
	int64_t		bytes_processed;
	int64_t		bytes_total;
	int64_t		tuples_processed;
	int64_t		tuples_excluded;
};

int			copyprogress_cmp(struct copyprogress_t *, struct copyprogress_t *);
static void copyprogress_info(void);
void		print_copyprogress(void);
int			read_copyprogress(void);
int			select_copyprogress(void);
void		sort_copyprogress(void);
int			sort_copyprogress_pid_callback(const void *, const void *);
int			sort_copyprogress_relid_callback(const void *, const void *);
int			sort_copyprogress_command_callback(const void *, const void *);
int			sort_copyprogress_type_callback(const void *, const void *);
int			sort_copyprogress_bytes_processed_callback(const void *, const void *);
int			sort_copyprogress_bytes_total_callback(const void *, const void *);
int			sort_copyprogress_tuples_processed_callback(const void *, const void *);
int			sort_copyprogress_tuples_excluded_callback(const void *, const void *);

RB_HEAD(copyprogress, copyprogress_t) head_copyprogresses = RB_INITIALIZER(&head_copyprogresses);
RB_PROTOTYPE(copyprogress, copyprogress_t, entry, copyprogress_cmp)
RB_GENERATE(copyprogress, copyprogress_t, entry, copyprogress_cmp)

field_def fields_copyprogress[] =
{
	{
		"PID", 4, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0
	},
	{
		"RELID", 6, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"COMMAND", 8, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"TYPE", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"BYTES_PROCESSED", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"BYTES_TOTAL", 12, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"TUPLES_PROCESSED", 17, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},
	{
		"TUPLES_EXCLUDED", 16, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0
	},

};

#define FLD_COPY_PID                    FIELD_ADDR(fields_copyprogress, 0)
#define FLD_COPY_REILD                  FIELD_ADDR(fields_copyprogress, 1)
#define FLD_COPY_COMMAND                FIELD_ADDR(fields_copyprogress, 2)
#define FLD_COPY_TYPE                   FIELD_ADDR(fields_copyprogress, 3)
#define FLD_COPY_BYTES_PROCESSED        FIELD_ADDR(fields_copyprogress, 4)
#define FLD_COPY_BYTES_TOTAL            FIELD_ADDR(fields_copyprogress, 5)
#define FLD_COPY_TUPLES_PROCESSED  FIELD_ADDR(fields_copyprogress, 6)
#define FLD_COPY_TUPLES_EXCLUDED        FIELD_ADDR(fields_copyprogress, 7)

/* Define views */
field_def  *view_copyprogress_0[] = {
	FLD_COPY_PID, FLD_COPY_REILD, FLD_COPY_COMMAND,
	FLD_COPY_TYPE, FLD_COPY_BYTES_PROCESSED, FLD_COPY_BYTES_TOTAL,
	FLD_COPY_TUPLES_PROCESSED, FLD_COPY_TUPLES_EXCLUDED, NULL
};

order_type	copyprogress_order_list[] = {
	{"pid", "pid", 'u', sort_copyprogress_pid_callback},
	{"relid", "relid", 'e', sort_copyprogress_relid_callback},
	{"command", "command", 'f', sort_copyprogress_command_callback},
	{"type", "type", 'v', sort_copyprogress_type_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager copyprogress_mgr = {
	"copyprogress", select_copyprogress, read_copyprogress, sort_copyprogress,
	print_header, print_copyprogress, keyboard_callback, copyprogress_order_list,
	copyprogress_order_list
};

field_view	views_copyprogress[] = {
	{view_copyprogress_0, "copyprogress", 'w', &copyprogress_mgr},
	{NULL, NULL, 0, NULL}
};

int			copyprogress_exist = 1;
int			copyprogress_count;
struct copyprogress_t *copyprogresses;

static void
copyprogress_info(void)
{
	int			i;
	PGresult   *pgresult = NULL;

	struct copyprogress_t *n,
			   *p;

	connect_to_db();
	if (options.connection != NULL)
	{
		if (PQserverVersion(options.connection) / 100 < 1300)
		{
			copyprogress_exist = 0;
			return;
		}

		pgresult = PQexec(options.connection, QUERY_STAT_COPY_PROCESS);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
		{
			i = copyprogress_count;
			copyprogress_count = PQntuples(pgresult);
		}
	}
	else
	{
		error("Cannot connect to database");
		return;
	}

	if (copyprogress_count > i)
	{
		p = reallocarray(copyprogresses, copyprogress_count,
						 sizeof(struct copyprogress_t));
		if (p == NULL)
		{
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		copyprogresses = p;
	}

	for (i = 0; i < copyprogress_count; i++)
	{
		n = malloc(sizeof(struct copyprogress_t));
		if (n == NULL)
		{
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->pid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(copyprogress, &head_copyprogresses, n);
		if (p != NULL)
		{
			free(n);
			n = p;
		}
		n->relid = atoll(PQgetvalue(pgresult, i, 1));
		strncpy(n->command, PQgetvalue(pgresult, i, 2), NAMEDATALEN);
		strncpy(n->type, PQgetvalue(pgresult, i, 3), NAMEDATALEN);
		n->bytes_processed = atoll(PQgetvalue(pgresult, i, 4));
		n->bytes_total = atoll(PQgetvalue(pgresult, i, 5));
		n->tuples_processed = atoll(PQgetvalue(pgresult, i, 6));
		n->tuples_excluded = atoll(PQgetvalue(pgresult, i, 7));

		memcpy(&copyprogresses[i], n, sizeof(struct copyprogress_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
copyprogress_cmp(struct copyprogress_t *e1, struct copyprogress_t *e2)
{
	return (e1->pid < e2->pid ? -1 : e1->pid > e2->pid);
}

int
select_copyprogress(void)
{
	return (0);
}

int
read_copyprogress(void)
{
	copyprogress_info();
	num_disp = copyprogress_count;
	return (0);
}

int
initcopyprogress(void)
{
	if (pg_version() < 1400)
	{
		return 0;
	}

	field_view *v;

	copyprogresses = NULL;
	copyprogress_count = 0;

	read_copyprogress();
	if (copyprogress_exist == 0)
	{
		return 0;
	}

	for (v = views_copyprogress; v->name != NULL; v++)
		add_view(v);

	return (1);
}

void
print_copyprogress(void)
{
	int			cur = 0,
				i;
	int			end = dispstart + maxprint;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < copyprogress_count; i++)
	{
		do
		{
			if (cur >= dispstart && cur < end)
			{
				print_fld_uint(FLD_COPY_PID, copyprogresses[i].pid);
				print_fld_uint(FLD_COPY_REILD, copyprogresses[i].relid);
				print_fld_str(FLD_COPY_COMMAND, copyprogresses[i].command);
				print_fld_str(FLD_COPY_TYPE, copyprogresses[i].type);
				print_fld_uint(FLD_COPY_BYTES_PROCESSED, copyprogresses[i].bytes_processed);
				print_fld_uint(FLD_COPY_BYTES_TOTAL, copyprogresses[i].bytes_total);
				print_fld_uint(FLD_COPY_TUPLES_PROCESSED, copyprogresses[i].tuples_processed);
				print_fld_uint(FLD_COPY_TUPLES_EXCLUDED, copyprogresses[i].tuples_excluded);
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
sort_copyprogress(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (copyprogresses == NULL)
		return;
	if (copyprogress_count <= 0)
		return;

	mergesort(copyprogresses, copyprogress_count, sizeof(struct copyprogress_t),
			  ordering->func);
}

int
sort_copyprogress_pid_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	return (n1->pid < n2->pid) * sortdir;
}

int
sort_copyprogress_relid_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (n1->relid < n2->relid)
		return sortdir;
	if (n1->relid > n2->relid)
		return -sortdir;

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_command_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (strcmp(n1->command, n2->command) < 0)
		return sortdir;
	if (strcmp(n1->command, n2->command) > 0)
		return -sortdir;

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_type_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (strcmp(n1->type, n2->type) < 0)
		return sortdir;
	if (strcmp(n1->type, n2->type) > 0)
		return -sortdir;

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_bytes_processed_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (n1->bytes_processed < n2->bytes_processed)
	{
		return sortdir;
	}
	if (n1->bytes_processed > n2->bytes_processed)
	{
		return -sortdir;
	}

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_bytes_total_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (n1->bytes_total < n2->bytes_total)
	{
		return sortdir;
	}
	if (n1->bytes_total > n2->bytes_total)
	{
		return -sortdir;
	}

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_tuples_processed_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (n1->tuples_processed < n2->tuples_processed)
	{
		return sortdir;
	}
	if (n1->tuples_processed > n2->tuples_processed)
	{
		return -sortdir;
	}

	return sort_copyprogress_pid_callback(v1, v2);
}

int
sort_copyprogress_tuples_excluded_callback(const void *v1, const void *v2)
{
	struct copyprogress_t *n1,
			   *n2;

	n1 = (struct copyprogress_t *) v1;
	n2 = (struct copyprogress_t *) v2;

	if (n1->tuples_excluded < n2->tuples_excluded)
	{
		return sortdir;
	}
	if (n1->tuples_excluded > n2->tuples_excluded)
	{
		return -sortdir;
	}

	return sort_copyprogress_pid_callback(v1, v2);
}
