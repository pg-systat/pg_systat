/*
 * Copyright (c) 2001, 2007 Can Erkin Acar
 * Copyright (c) 2001 Daniel Hartmeier
 * Copyright (c) 2019 PostgreSQL Global Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *    - Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    - Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifdef __linux__
#define _GNU_SOURCE
#endif /* __linux__ */

#include <sys/types.h>
#include <sys/sysctl.h>


#include <ctype.h>
#include <curses.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <bsd/stdlib.h>
#include <bsd/string.h>
#endif /* __linux__ */
#include <stdarg.h>
#include <getopt.h>
#include <unistd.h>
#include <utmp.h>
#include <time.h>
#include <sys/time.h>

#include "engine.h"
#include "pg_systat.h"
#include "pg.h"
#include "port.h"

#define TIMEPOS (80 - 8 - 20 - 1)
#define PGSTRBUF 30
#define DATABASE_NAME_MAX 63
#define USER_NAME_MAX 32
#define PORT_LEN 5
#define NUM_STRINGS 8

double	naptime = 5.0;

void usage(void);

/* command prompt */

void cmd_delay(const char *);
void cmd_count(const char *);
void cmd_compat(const char *);

struct command cm_compat = {"Command", cmd_compat};
struct command cm_delay = {"Seconds to delay", cmd_delay};
struct command cm_count = {"Number of lines to display", cmd_count};


/*
 * "Safe" wrapper around strdup().
 */

char *
_strdup(const char *in)
{
	char	   *tmp;

	if (!in) {
		fprintf(stderr, "cannot duplicate null pointer (internal error)\n");
		exit(EXIT_FAILURE);
	}
	tmp = strdup(in);
	if (!tmp) {
		fprintf(stderr, "out of memory\n");
		exit(EXIT_FAILURE);
	}
	return tmp;
}

/* display functions */

int
print_header(void)
{
	time_t now;
	int start = dispstart + 1, end = dispstart + maxprint;

	char header[MAX_LINE_BUF];
	char pgstr[PGSTRBUF + 1] = "";
	char tmpbuf[TIMEPOS];
	char timebuf[26];
	char database[DATABASE_NAME_MAX + 1] = "";
	char hostname[HOST_NAME_MAX + 1] = "";
	char username[USER_NAME_MAX + 1] = "";
	char port[PORT_LEN + 1] = "";

	PGresult	*pgresult = NULL;
	const char	*pgdb;
	const char	*pghost;
	const char	*pgport;
	const char	*pguser;

	if (end > num_disp)
		end = num_disp;

	tb_start();

	if (!paused) {
		char *ctim;

		time(&now);
		ctim = ctime(&now);
		ctim[11+8] = '\0';
		strlcpy(timebuf, ctim + 11, sizeof(timebuf));
	}

	connect_to_db();
	if (options.connection != NULL) {
		pgresult = PQexec(options.connection,
				"SELECT regexp_split_to_table(version(), '\\s+')");
		if (PQresultStatus(pgresult) == PGRES_TUPLES_OK)
			snprintf(pgstr, sizeof(pgstr), "%s %s", PQgetvalue(pgresult, 0, 0),
					PQgetvalue(pgresult, 1, 0));

		pgdb = PQdb(options.connection);
		if (pgdb && pgdb[0])
			strncpy(database, pgdb, DATABASE_NAME_MAX);

		pghost = PQhost(options.connection);
		if (pghost && pghost[0])
			strncpy(hostname, pghost, HOST_NAME_MAX);

		pgport = PQport(options.connection);
		if (pgport && pgport[0])
			strncpy(port, pgport, PORT_LEN);

		pguser = PQuser(options.connection);
		if (pguser && pguser[0])
			strncpy(username, pguser, USER_NAME_MAX);
	}

	if (num_disp && (start > 1 || end != num_disp))
		snprintf(tmpbuf, sizeof(tmpbuf),
				"(%u-%u of %u) %s%s", start, end, num_disp,
				paused ? "PAUSED " : "", pgstr);
	else
		snprintf(tmpbuf, sizeof(tmpbuf), "%s%s",
				paused ? "PAUSED " : "", pgstr);
		
	snprintf(header, sizeof(header), "%s %s %s@%s:%s/%s", timebuf, tmpbuf,
			username, hostname, port, database);

	if (pgresult != NULL)
		PQclear(pgresult);
	disconnect_from_db();

	if (rawmode)
		printf("\n\n%s\n", header);
	else
		mvprintw(0, 0, "%s", header);

	return (1);
}

/* compatibility functions, rearrange later */
void
error(const char *fmt, ...)
{
	va_list ap;
	char buf[MAX_LINE_BUF];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof buf, fmt, ap);
	va_end(ap);

	message_set(buf);
}

void
die(void)
{
	if (!rawmode)
		endwin();
	exit(0);
}


int
prefix(char *s1, char *s2)
{

	while (*s1 == *s2) {
		if (*s1 == '\0')
			return (1);
		s1++, s2++;
	}
	return (*s1 == '\0');
}

/* main program functions */

void
usage(void)
{
	extern char *__progname;
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  %s [OPTION]... [VIEW] [DELAY]\n", __progname);
	fprintf(stderr, "\nGeneral options:\n");
	fprintf(stderr, "  -a           display all lines\n");
	fprintf(stderr, "  -B           non-interactive mode, exit after two "
			"update\n");
	fprintf(stderr, "  -b           non-interactive mode, exit after one "
			"update\n");
	fprintf(stderr, "  -d count     exit after count screen updates\n");
	fprintf(stderr, "  -i           interactive mode\n");
	fprintf(stderr, "\nConnection options:\n");
	fprintf(stderr, "  -d dbname    database name to connect to\n");
	fprintf(stderr, "  -h host      database server host or socket "
			"directory\n");
	fprintf(stderr, "  -p port      database server port\n");
	fprintf(stderr, "  -U username  database user name\n");
	exit(1);
}

void
show_view(void)
{
	if (rawmode)
		return;

	tb_start();
	tbprintf("%s %g", curr_view->name, naptime);
	tb_end();
	message_set(tmp_buf);
}

void
add_view_tb(field_view *v)
{
	if (curr_view == v)
		tbprintf("[%s] ", v->name);
	else
		tbprintf("%s ", v->name);
}

void
show_help(void)
{
	if (rawmode)
		return;

	tb_start();
	foreach_view(add_view_tb);
	tb_end();
	message_set(tmp_buf);
}

void
add_order_tb(order_type *o)
{
	if (curr_view->mgr->order_curr == o)
		tbprintf("[%s%s(%c)] ", o->name,
		    o->func != NULL && sortdir == -1 ? "^" : "",
		    (char) o->hotkey);
	else
		tbprintf("%s(%c) ", o->name, (char) o->hotkey);
}

void
show_order(void)
{
	if (rawmode)
		return;

	tb_start();
	if (foreach_order(add_order_tb) == -1) {
		tbprintf("No orders available");
	}
	tb_end();
	message_set(tmp_buf);
}

void
cmd_compat(const char *buf)
{
	const char *s;

	if (strcasecmp(buf, "help") == 0) {
		show_help();
		need_update = 1;
		return;
	}
	if (strcasecmp(buf, "quit") == 0 || strcasecmp(buf, "q") == 0) {
		gotsig_close = 1;
		return;
	}
	if (strcasecmp(buf, "stop") == 0) {
		paused = 1;
		gotsig_alarm = 1;
		return;
	}
	if (strncasecmp(buf, "start", 5) == 0) {
		paused = 0;
		gotsig_alarm = 1;
		cmd_delay(buf + 5);
		return;
	}
	if (strncasecmp(buf, "order", 5) == 0) {
		show_order();
		need_update = 1;
		return;
	}

	for (s = buf; *s && strchr("0123456789+-.eE", *s) != NULL; s++)
		;
	if (*s) {
		if (set_view(buf))
			error("Invalid/ambiguous view: %s", buf);
	} else
		cmd_delay(buf);
}

void
cmd_delay(const char *buf)
{
	double del;
	del = atof(buf);

	if (del > 0) {
		udelay = (useconds_t)(del * 1000000);
		gotsig_alarm = 1;
		naptime = del;
	}
}

void
cmd_count(const char *buf)
{
	const char *errstr;

	maxprint = strtonum(buf, 1, lines - HEADER_LINES, &errstr);
	if (errstr)
		maxprint = lines - HEADER_LINES;
}

/*
 * format_b(amt) - format a byte memory value, returning a string
 *		suitable for display.  Returns a pointer to a static
 *		area that changes each call.  "amt" is converted to a
 *		string with a trailing "B".  If "amt" is 10000 or greater,
 *		then it is formatted as megabytes (rounded) with a
 *		trailing "K".  And so on...
 */

char *
format_b(long long amt)

{
	static char retarray[NUM_STRINGS][16];
	static int	index = 0;
	register char *ret;
	register char tag = 'B';

	ret = retarray[index];
	index = (index + 1) % NUM_STRINGS;

	if (amt >= 10000) {
		amt = (amt + 512) / 1024;
		tag = 'K';
		if (amt >= 10000) {
			amt = (amt + 512) / 1024;
			tag = 'B';
			if (amt >= 10000) {
				amt = (amt + 512) / 1024;
				tag = 'G';
			}
		}
	}

	snprintf(ret, sizeof(retarray[index]) - 1, "%lld%c", amt, tag);

	return (ret);
}

int
keyboard_callback(int ch)
{
	switch (ch) {
	case '?':
		/* FALLTHROUGH */
	case 'h':
		show_help();
		need_update = 1;
		break;
	case CTRL_G:
		show_view();
		need_update = 1;
		break;
	case 'l':
		command_set(&cm_count, NULL);
		break;
	case 's':
		command_set(&cm_delay, NULL);
		break;
	case ',':
		separate_thousands = !separate_thousands;
		gotsig_alarm = 1;
		break;
	case ':':
		command_set(&cm_compat, NULL);
		break;
	default:
		return 0;
	};

	return 1;
}

void
initialize(void)
{
	engine_initialize();

	/* Initialize in order to appear in interactive mode. */
	initdbxact();
	initdbblk();
	initdbconfl();
	initdbfs();
	initdbtup();
	initindex();
	inittableanalyze();
	inittableioheap();
	inittableioidx();
	inittableiotidx();
	inittableiotoast();
	inittablescan();
	inittabletup();
	inittablevac();
}

int
main(int argc, char *argv[])
{
	const char *errstr;
	extern char *optarg;
	extern int optind;
	double delay = 5;

	char *viewstr = NULL;

	int countmax = 0;
	int maxlines = 0;

	int ch;
	int optindex;
	static struct option long_options[] = {
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{NULL, 0, NULL, 0}
	};

	memset(&options, 0, sizeof(struct adhoc_opts));
	while ((ch = getopt_long(argc, argv, "BCU:Wabd:h:ip:s:", long_options,
			&optindex)) != -1) {
		switch (ch) {
		case 'C':
			countmax = strtonum(optarg, 1, INT_MAX, &errstr);
			if (errstr)
				errx(1, "-d %s: %s", optarg, errstr);
			break;
		case 'U':
			options.values[PG_USER] = _strdup(optarg);
			break;
		case 'W':
			options.persistent = 1;
			options.values[PG_PASSWORD] = simple_prompt("Password: ", 1000, 0);
			break;
		case 'a':
			maxlines = -1;
			break;
		case 'B':
			averageonly = 1;
			if (countmax < 2)
				countmax = 2;
			/* FALLTHROUGH to 'b' */
		case 'b':
			rawmode = 1;
			interactive = 0;
			break;
		case 'd':
			options.values[PG_DBNAME] = _strdup(optarg);
			break;
		case 'h':
			options.values[PG_HOST] = _strdup(optarg);
			break;
		case 'i':
			interactive = 1;
			break;
		case 'p':
			options.values[PG_PORT] = _strdup(optarg);
			break;
		case 's':
			delay = atof(optarg);
			if (delay <= 0)
				delay = 5;
			break;
		case 'w':
			rawwidth = strtonum(optarg, 1, MAX_LINE_BUF-1, &errstr);
			if (errstr)
				errx(1, "-w %s: %s", optarg, errstr);
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	}

	argc -= optind;
	argv += optind;

	if (argc == 1) {
		double del = atof(argv[0]);
		if (del == 0)
			viewstr = argv[0];
		else
			delay = del;
	} else if (argc == 2) {
		viewstr = argv[0];
		delay = atof(argv[1]);
		if (delay <= 0)
			delay = 5;
	}

	udelay = (useconds_t)(delay * 1000000.0);
	if (udelay < 1)
		udelay = 1;

	naptime = (double)udelay / 1000000.0;

	initialize();

	set_order(NULL);
	if (viewstr && set_view(viewstr)) {
		fprintf(stderr, "Unknown/ambiguous view name: %s\n", viewstr);
		return 1;
	}

	if (check_termcap()) {
		rawmode = 1;
		interactive = 0;
	}

	setup_term(maxlines);

	if (rawmode && countmax == 0)
		countmax = 1;

	gotsig_alarm = 1;

	engine_loop(countmax);

	return 0;
}
