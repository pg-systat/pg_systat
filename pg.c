/*
 * Copyright (c) 2019 PostgreSQL Global Development Group
 */

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "pg.h"

const char *keywords[6] = {"host", "port", "user", "password", "dbname", NULL};
struct adhoc_opts options;

void
connect_to_db()
{
	int			i;

	if (options.persistent && PQsocket(options.connection) >= 0)
		return;

	options.connection = PQconnectdbParams(keywords, options.values, 1);
	if (PQstatus(options.connection) != CONNECTION_OK)
	{
		PQfinish(options.connection);
		options.connection = NULL;
		return;
	}

	if (options.persistent)
		for (i = 0; i < 5; i++)
			if (options.values[i] != NULL)
				free((void *) options.values[i]);

	PQexec(options.connection,
		   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " \
		   "READ UNCOMMITTED;");
}

void
disconnect_from_db()
{
	if (options.persistent)
		return;
	PQfinish(options.connection);
}

int
pg_version()
{
	connect_to_db();
	int			version = PQserverVersion(options.connection) / 100;

	disconnect_from_db();
	return version;
}
