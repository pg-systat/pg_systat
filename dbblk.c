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

#define QUERY_STAT_DBBLK \
		"SELECT datid, coalesce(datname, '<shared relation objects>'),\n" \
		"       blks_read, blks_hit, temp_files, temp_bytes,\n" \
		"       blk_read_time, blk_write_time\n" \
		"FROM pg_stat_database;"

struct dbblk_t
{
	RB_ENTRY(dbblk_t) entry;
	long long datid;
	char datname[NAMEDATALEN + 1];
	int64_t blks_read;
	int64_t blks_read_diff;
	int64_t blks_read_old;
	int64_t blks_hit;
	int64_t blks_hit_diff;
	int64_t blks_hit_old;
	int64_t temp_files;
	int64_t temp_files_diff;
	int64_t temp_files_old;
	int64_t temp_bytes;
	int64_t temp_bytes_diff;
	int64_t temp_bytes_old;
	int64_t blk_read_time;
	int64_t blk_read_time_diff;
	int64_t blk_read_time_old;
	int64_t blk_write_time;
	int64_t blk_write_time_diff;
	int64_t blk_write_time_old;
};

int dbblkcmp(struct dbblk_t *, struct dbblk_t *);
static void dbblk_info(void);
void print_dbblk(void);
int read_dbblk(void);
int select_dbblk(void);
void sort_dbblk(void);
int sort_dbblk_datname_callback(const void *, const void *);
int sort_dbblk_hit_callback(const void *, const void *);
int sort_dbblk_read_callback(const void *, const void *);
int sort_dbblk_read_time_callback(const void *, const void *);
int sort_dbblk_temp_files_callback(const void *, const void *);
int sort_dbblk_temp_bytes_callback(const void *, const void *);
int sort_dbblk_write_time_callback(const void *, const void *);

RB_HEAD(dbblk, dbblk_t) head_dbblks = RB_INITIALIZER(&head_dbblks);
RB_PROTOTYPE(dbblk, dbblk_t, entry, dbblkcmp)
RB_GENERATE(dbblk, dbblk_t, entry, dbblkcmp)

field_def fields_dbblk[] = {
	{ "DATABASE", 9, NAMEDATALEN, 1, FLD_ALIGN_LEFT, -1, 0, 0, 0 },
	{ "READ", 5, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "READ/s", 7, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "HIT", 4, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "HIT%", 5, 5, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "R_TIME", 7, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "W_TIME", 7, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TMP_FILES", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
	{ "TMP_BYTES", 10, 19, 1, FLD_ALIGN_RIGHT, -1, 0, 0, 0 },
};

#define FLD_DB_DATNAME        FIELD_ADDR(fields_dbblk, 0)
#define FLD_DB_BLKS_READ      FIELD_ADDR(fields_dbblk, 1)
#define FLD_DB_BLKS_READ_RATE FIELD_ADDR(fields_dbblk, 2)
#define FLD_DB_BLKS_HIT       FIELD_ADDR(fields_dbblk, 3)
#define FLD_DB_BLKS_HIT_PER   FIELD_ADDR(fields_dbblk, 4)
#define FLD_DB_BLK_READ_TIME  FIELD_ADDR(fields_dbblk, 5)
#define FLD_DB_BLK_WRITE_TIME FIELD_ADDR(fields_dbblk, 6)
#define FLD_DB_TEMP_FILES     FIELD_ADDR(fields_dbblk, 7)
#define FLD_DB_TEMP_BYTES     FIELD_ADDR(fields_dbblk, 8)

/* Define views */
field_def *view_dbblk_0[] = {
	FLD_DB_DATNAME, FLD_DB_BLKS_READ, FLD_DB_BLKS_READ_RATE, FLD_DB_BLKS_HIT,
	FLD_DB_BLKS_HIT_PER, FLD_DB_BLK_READ_TIME, FLD_DB_BLK_WRITE_TIME,
	FLD_DB_TEMP_FILES, FLD_DB_TEMP_BYTES, NULL
};

order_type dbblk_order_list[] = {
	{"datname", "datname", 'n', sort_dbblk_datname_callback},
	{"blks_read", "blks_read", 'r', sort_dbblk_read_callback},
	{"blks_hit", "blks_hit", 'h', sort_dbblk_hit_callback},
	{"temp_files", "temp_files", 'f', sort_dbblk_temp_files_callback},
	{"temp_bytes", "temp_bytes", 'b', sort_dbblk_temp_bytes_callback},
	{"blk_read_time", "blk_read_time", 'R', sort_dbblk_read_time_callback},
	{"blk_write_time", "blk_write_time", 'W', sort_dbblk_write_time_callback},
	{NULL, NULL, 0, NULL}
};

/* Define view managers */
struct view_manager dbblk_mgr = {
	"dbblk", select_dbblk, read_dbblk, sort_dbblk, print_header,
	print_dbblk, keyboard_callback, dbblk_order_list, dbblk_order_list
};

field_view views_dbblk[] = {
	{ view_dbblk_0, "dbblk", 'B', &dbblk_mgr },
	{ NULL, NULL, 0, NULL }
};

int	dbblk_count;
struct dbblk_t *dbblks;

static void
dbblk_info(void)
{
	int i;
	PGresult	*pgresult = NULL;

	struct dbblk_t *n, *p;

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection, QUERY_STAT_DBBLK);
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK) {
			i = dbblk_count;
			dbblk_count = PQntuples(pgresult);
		}
	} else {
		error("Cannot connect to database");
		return;
	}

	if (dbblk_count > i) {
		p = reallocarray(dbblks, dbblk_count, sizeof(struct dbblk_t));
		if (p == NULL) {
			error("reallocarray error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		dbblks = p;
	}

	for (i = 0; i < dbblk_count; i++) {
		n = malloc(sizeof(struct dbblk_t));
		if (n == NULL) {
			error("malloc error");
			if (pgresult != NULL)
				PQclear(pgresult);
			disconnect_from_db();
			return;
		}
		n->datid = atoll(PQgetvalue(pgresult, i, 0));
		p = RB_INSERT(dbblk, &head_dbblks, n);
		if (p == NULL)
			strncpy(n->datname, PQgetvalue(pgresult, i, 1), NAMEDATALEN);
		else {
			free(n);
			n = p;
		}

		n->blks_read_old = n->blks_read;
		n->blks_read = atoll(PQgetvalue(pgresult, i, 2));
		n->blks_read_diff = n->blks_read - n->blks_read_old;

		n->blks_hit_old = n->blks_hit;
		n->blks_hit = atoll(PQgetvalue(pgresult, i, 3));
		n->blks_hit_diff = n->blks_hit - n->blks_hit_old;

		n->temp_files_old = n->temp_files;
		n->temp_files = atoll(PQgetvalue(pgresult, i, 4));
		n->temp_files_diff = n->temp_files - n->temp_files_old;

		n->temp_bytes_old = n->temp_bytes;
		n->temp_bytes = atoll(PQgetvalue(pgresult, i, 5));
		n->temp_bytes_diff = n->temp_bytes - n->temp_bytes_old;

		n->blk_read_time_old = n->blk_read_time;
		n->blk_read_time = atoll(PQgetvalue(pgresult, i, 6));
		n->blk_read_time_diff = n->blk_read_time - n->blk_read_time_old;

		n->blk_write_time_old = n->blk_write_time;
		n->blk_write_time = atoll(PQgetvalue(pgresult, i, 7));
		n->blk_write_time_diff = n->blk_write_time - n->blk_write_time_old;

		memcpy(&dbblks[i], n, sizeof(struct dbblk_t));
	}

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();
}

int
dbblkcmp(struct dbblk_t *e1, struct dbblk_t *e2)
{
	return (e1->datid < e2->datid ? -1 : e1->datid > e2->datid);
}

int
select_dbblk(void)
{
	return (0);
}

int
read_dbblk(void)
{
	dbblk_info();
	num_disp = dbblk_count;
	return (0);
}

int
initdbblk(void)
{
	field_view	*v;

	dbblks = NULL;
	dbblk_count = 0;

	for (v = views_dbblk; v->name != NULL; v++)
		add_view(v);

	read_dbblk();

	return(1);
}

void
print_dbblk(void)
{
	int cur = 0, i;
	int end = dispstart + maxprint;
	int hit_per;

	if (end > num_disp)
		end = num_disp;

	for (i = 0; i < dbblk_count; i++) {
		do {
			if (dbblks[i].blks_read_diff + dbblks[i].blks_hit_diff > 0)
				hit_per = 100 * dbblks[i].blks_hit_diff /
						(dbblks[i].blks_read_diff + dbblks[i].blks_hit_diff);
			else
				hit_per = 0;
			if (cur >= dispstart && cur < end) {
				print_fld_str(FLD_DB_DATNAME, dbblks[i].datname);
				print_fld_uint(FLD_DB_BLKS_READ, dbblks[i].blks_read_diff);
				print_fld_uint(FLD_DB_BLKS_READ_RATE,
						dbblks[i].blks_read_diff /
								((int64_t) udelay / 1000000));
				print_fld_ssize(FLD_DB_BLKS_HIT, dbblks[i].blks_hit_diff);
				print_fld_ssize(FLD_DB_BLKS_HIT_PER, hit_per);
				print_fld_ssize(FLD_DB_BLK_READ_TIME,
						dbblks[i].blk_read_time_diff);
				print_fld_ssize(FLD_DB_BLK_WRITE_TIME,
						dbblks[i].blk_write_time_diff);
				print_fld_ssize(FLD_DB_TEMP_FILES, dbblks[i].temp_files_diff);
				print_fld_ssize(FLD_DB_TEMP_BYTES, dbblks[i].temp_bytes_diff);
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
sort_dbblk(void)
{
	order_type *ordering;

	if (curr_mgr == NULL)
		return;

	ordering = curr_mgr->order_curr;

	if (ordering == NULL)
		return;
	if (ordering->func == NULL)
		return;
	if (dbblks == NULL)
		return;
	if (dbblk_count <= 0)
		return;

	mergesort(dbblks, dbblk_count, sizeof(struct dbblk_t), ordering->func);
}

int
sort_dbblk_datname_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	return strcmp(n1->datname, n2->datname) * sortdir;
}

int
sort_dbblk_hit_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->blks_hit_diff < n2->blks_hit_diff)
		return sortdir;
	if (n1->blks_hit_diff > n2->blks_hit_diff)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}

int
sort_dbblk_read_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->blks_read_diff < n2->blks_read_diff)
		return sortdir;
	if (n1->blks_read_diff > n2->blks_read_diff)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}

int
sort_dbblk_read_time_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->blk_read_time_diff < n2->blk_read_time_diff)
		return sortdir;
	if (n1->blk_read_time_diff > n2->blk_read_time_diff)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}

int
sort_dbblk_temp_bytes_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->temp_bytes_diff < n2->temp_bytes_diff)
		return sortdir;
	if (n1->temp_bytes_diff > n2->temp_bytes_diff)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}

int
sort_dbblk_temp_files_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->temp_files < n2->temp_files)
		return sortdir;
	if (n1->temp_files > n2->temp_files)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}

int
sort_dbblk_write_time_callback(const void *v1, const void *v2)
{
	struct dbblk_t *n1, *n2;
	n1 = (struct dbblk_t *) v1;
	n2 = (struct dbblk_t *) v2;

	if (n1->blk_write_time_diff < n2->blk_write_time_diff)
		return sortdir;
	if (n1->blk_write_time_diff > n2->blk_write_time_diff)
		return -sortdir;

	return sort_dbblk_datname_callback(v1, v2);
}
