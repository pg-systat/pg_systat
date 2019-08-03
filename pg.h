/*
 * Copyright (c) 2019 PostgreSQL Global Development Group
 */

#ifndef _PG_H_
#define _PG_H_

#include <libpq-fe.h>
#include "pg_config_manual.h"

#define TIMESTAMPLEN 29

enum pgparams
{
	PG_HOST,
	PG_PORT,
	PG_USER,
	PG_PASSWORD,
	PG_DBNAME
};

struct adhoc_opts
{
	int persistent;
	PGconn *connection;
	const char *values[6];
};

extern struct adhoc_opts options;

void connect_to_db();
void disconnect_from_db();

#endif /* _PG_H_ */
