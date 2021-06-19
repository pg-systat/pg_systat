.. Copyright (c) 1985, 1990, 1993
.. The Regents of the University of California.  All rights reserved.
.. Copyright (c) 2019 PostgreSQL Global Development Group
..
.. Redistribution and use in source and binary forms, with or without
.. modification, are permitted provided that the following conditions
.. are met:
.. 1. Redistributions of source code must retain the above copyright
.. notice, this list of conditions and the following disclaimer.
.. 2. Redistributions in binary form must reproduce the above copyright
.. notice, this list of conditions and the following disclaimer in the
.. documentation and/or other materials provided with the distribution.
.. 3. Neither the name of the University nor the names of its contributors
.. may be used to endorse or promote products derived from this software
.. without specific prior written permission.
..
.. THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
.. ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
.. IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
.. ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
.. FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
.. DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
.. OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
.. HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
.. LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
.. OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
.. SUCH DAMAGE.

===========
 pg_systat
===========

-----------------------------
display PostgreSQL statistics
-----------------------------

SYNOPSIS
========

**pg_systat** [*option...*] [*view*] [*delay*]

DESCRIPTION
===========

**pg_systat** displays various PostgreSQL statistics in a screen-oriented
fashion using the curses(3) screen display library.

While **pg_systat** is running, the screen is divided into different areas.
The top line displays the system time.  The bottom line of the screen is
reserved for user input and error messages.  The information displayed in the
rest of the screen comprises a *view*, and is the main interface for displaying
different types of PostgreSQL statistics.  The **dbxact** view is the default.

Certain information may be discarded when the screen size is insufficient for
display.  For example, in an instance with 24 database the **dbxact**
statistics displays only 21 databases on a 24 line terminal.

OPTIONS
=======

-a   Display all lines.
-B   Raw, non-interactive mode.  The default is to exit after two screen
     updates, with statistics only ever displayed once.  Useful for views such
     as **cpu**, where initial calculations are useless.
-b   Raw, non-interactive mode.  The default is to exit after one screen
     update, with statistics displayed every update.
-C count   Exit after *count* screen updates.
-d dbname   Specifies the name of the database to connect to. This is
            equivalent to specifying dbname as the first non-option argument on
            the command line.

            If this parameter contains an = sign or starts with a valid URI
            prefix (postgresql:// or postgres://), it is treated as a conninfo
            string.
-h host   Specifies the host name of the machine on which the server is
          running. If the value begins with a slash, it is used as the
          directory for the Unix-domain socket.
-i   Interactive mode.
-p port   Specifies the TCP port or the local Unix-domain socket file extension
          on which the server is listening for connections. Defaults to the
          value of the PGPORT environment variable or, if not set, to the port
          specified at compile time, usually 5432.
-s delay   Specifies the screen refresh time interval in seconds.  This option
           is overridden by the final *delay* argument, if given.  The default
           interval is 5 seconds.
-U username   Connect to the database as the user *username* instead of the
              default. (You must have permission to do so, of course.)
-w width   Specifies the maximum width of the output in raw, non-interactive
           mode.

:view: The *view* argument expects to be one of: **dbtup**, or **dbxact**.
       These displays can also be requested interactively and are described in
       full detail below.  *view* may be abbreviated to the minimum unambiguous
       prefix; for example, "dbx" for "dbxact".
:delay: The *delay* argument specifies the screen refresh time interval in
        seconds.  This is provided for backwards compatibility, and overrides
        any interval specified with the **-s** flag.  The default interval is 5
        seconds.

Certain characters cause immediate action by **pg_systat**.  These are:

:\:: Move the cursor to the command line and interpret the input line typed as a
    command.  While entering a command the current character erase, word erase,
    and line kill characters may be used.
:o: Select the next ordering which sorts the rows according to a combination of
    columns.  Available orderings depend on the view.  Not all views support
    orderings.

:p: Pause **pg_systat**.
:q: Quit **pg_systat**.
:r: Reverse the selected ordering if supported by the view.
:,: Print numbers with thousand separators, where applicable.
:^A | (Home): Jump to the beginning of the current view.
:^B | (right arrow): Select the previous view.
:^E | (End): Jump to the end of the current view.
:^F | (left arrow): Select the next view.
:^G: Print the name of the current view being shown and the refresh interval.
:^L: Refresh the screen.
:^N | (down arrow): Scroll current view down by one line.
:^P | (up arrow): Scroll current view up by one line.
:^V | (Page Down): Scroll current view down by one page.
:Alt-V | (Page Up): Scroll current view up by one page.
:^Z: Suspend **pg_systat**.

The following commands are interpreted by the "global" command interpreter.

:help: Print the names of the available views on the command line.
:order: Print the names of the available orderings on the command line.
:quit: Quit **pg_systat**.  (This may be abbreviated to *q*.)
:stop: Stop refreshing the screen.
:[start] [number]: Start (continue) refreshing the screen.  If a second,
                   numeric, argument is provided it is interpreted as a refresh
                   interval (in seconds).  Supplying only a number will set the
                   refresh interval to this value.

*view* may be abbreviated to the minimum unambiguous prefix.  The available
views are:

:dbblk: Display database block statistics:

  :DATABASE: name of the database
  :READ: disk blocks read
  :READ/s: disk blocks read per second
  :HIT: disk blocks found in the buffer cache, so that a read was not necessary
        (this only includes hits in the PostgreSQL buffer cache, not the
        operating system's file system cache)
  :HIT%: percentage of total (READ + HIT) blocks read from the PostgreSQL
         buffer cache
  :R_TIME: time spent reading data file blocks by backends, in milliseconds
  :W_TIME: time spent writing data file blocks by backends, in milliseconds
  :TMP_FILES: temporary files created by queries
  :TMP_BYTES: data written to temporary files by queries
:dbconfl: Display database conflicts with recovery (applies only to standby
          servers)

  :CONFLICTS: queries canceled due to conflicts
  :TABLESPACE: queries canceled due to dropped tablespaces
  :LOCK: queries canceled due to lock timeouts
  :SNAPSHOT: queries canceled due to old snapshots
  :BUFFERPIN: queries canceled due to pinned buffers
  :DEADLOCK: queries canceled due to deadlocks

:dbtup: Display database tuple statistics:

  :DATABASE: name of the database
  :R/s: FETCHED rows per second
  :W/s: rows modified (INSERTED + UPDATED + DELETED) per second
  :RETURNED: rows returned by queries
  :FETCHED: rows fetched by queries
  :INSERTED: rows inserted by queries
  :UPDATED: rows updated by queries
  :DELETED: rows deleted by queries

:dbxact: Display database transaction statistics:

  :DATABASE: name of the database
  :CONNECTIONS: backends currently connected
  :COMMIT: transactions that have been committed
  :COMMIT/s: committed transaction rate per second
  :ROLLBACK: transactions that have been rolled back
  :ROLLBACK/s: rolled back transaction rate per second
  :DEADLOCKS: deadlocks detected

:index: Display index statistics:

  :SCHEMA: schema name
  :INDEXNAME: index name
  :TABLENAME: table name
  :IDX_SCAN: number of index scans initiated on this index
  :IDX_TUP_READ: number of index entries returned by scans on this index
  :IDX_TUP_FETCH: number of live table rows fetched by simple index scans using
                  this index

:indexio: Display index I/O statistics:

  :SCHEMA: schema name
  :INDEXNAME: index name
  :TABLENAME: table name
  :IDX_BLKS_READ: disk blocks read from this index
  :IDX_BLKS_HIT: buffer hits in this index

:tableanalyze: Display table analyze statistics:

  :SCHEMA: schema name
  :NAME: table name
  :N_MOD_SINCE_ANALYZE: estimated number of rows modified since this table was
                        last analyzed
  :LAST_ANALYZE: last time this table was manually vacuumed (not counting
                 VACUUM FULL)
  :LAST_AUTOANALYZE: last time this table was vacuumed by the autovacuum daemon
  :ANALYZE_COUNT: number of times this table has been manually vacuumed (not
                  counting VACUUM FULL)
  :AUTOANALYZE_COUNT: number of times this table has been vacuumed by the
                      autovacuum daemon

:tableioheap: Display table heap I/O statistics:

  :SCHEMA: schema name
  :NAME: table name
  :HEAP_BLKS_READ: disk blocks read from this table
  :HEAP_BLKS_HIT: buffer hits in this table

:tableioidx: Display table index I/O statistics:

  :SCHEMA: schema name
  :NAME: table name
  :IDX_BLKS_READ: disk blocks read from all indexes on this table
  :IDX_BLKS_HIT: buffer hits in all indexes on this table

:tableiotidx: Display toast table index I/O statistics:

  :SCHEMA: schema name
  :NAME: table name
  :TIDX_BLKS_READ: disk blocks read from this table's TOAST table indexes (if
                   any)
  :TIDX_BLKS_HIT: of buffer hits in this table's TOAST table indexes (if any)

:tableiotoast: Display toast table I/O statistics:

  :SCHEMA: schema name
  :NAME: table name
  :TOAST_BLKS_READ: disk blocks read from this table's TOAST table (if any)
  :TOAST_BLKS_HIT: buffer hits in this table's TOAST table (if any)

:tablescan: Display table and index scan statistics:

  :SCHEMA: schema name
  :NAME: table name
  :SEQ_SCAN: number of sequential scans
  :SEQ_TUP_READ: number of live rows fetched by sequential scans
  :IDX_SCAN: number of index scans
  :IDX_TUP_FETCH: number of live rows fetched by index scans

:tabletup: Display table row modification statistics:

  :SCHEMA: schema name
  :NAME: table name
  :INS: rows inserted
  :UPD: rows updated (includes HOT updated rows)
  :DEL: rows deleted
  :HOT_UPD: rows HOT updated (i.e., with no separate index update required)
  :LIVE: estimated number of live rows
  :DEAD: estimated number of dead rows

:tablevac: Display table vacuum statistics:

  :SCHEMA: schema name
  :NAME: table name
  :LAST_VACUUM: last time this table was manually vacuumed (not counting VACUUM
                FULL)
  :LAST_AUTOVACUUM: last time this table was vacuumed by the autovacuum daemon
  :VACUUM_COUNT: number of times this table has been manually vacuumed (not
                 counting VACUUM FULL)
  :AUTOVACUUM_COUNT: number of times this table has been vacuumed by the
                     autovacuum daemon

:stmtplan: Display statement plan statistics:

  :QUERYID: internal hash code for query
  :PLANS: number of times the statement was planned
  :TOTAL_PLAN_TIME: total time spent planning the statement
  :MIN_PLAN_TIME: minimum time spent planning the statement
  :MAX_PLAN_TIME: maximum time spent planning the statement
  :MEAN_PLAN_TIME: mean time spent planning the statement
  :STDDEV_PLAN_TIME: population standard deviation of time spent planning the statement

:stmtexec: Display statement execute statistics:

  :QUERYID: internal hash code for query
  :CALLS: number of times the statement was executed
  :TOTAL_EXEC_TIME: total time spent executing the statement
  :MIN_EXEC_TIME: minimum time spent executing the statement
  :MAX_EXEC_TIME: maximum time spent executing the statement
  :MEAN_EXEC_TIME: mean time spent executing the statement
  :STDDEV_EXEC_TIME: population standard deviation of time spent executing the statement

SEE ALSO
========

pg_top(1)

HISTORY
=======

The **pg_systat** program was adapted from systat.
