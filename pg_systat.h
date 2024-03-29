/*-
 * Copyright (c) 1980, 1989, 1992, 1993
 *	The Regents of the University of California.  All rights reserved.
 * Copyright (c) 2019 PostgreSQL Global Development Group
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 *	@(#)systat.h	8.1 (Berkeley) 6/6/93
 */

#ifndef _PGSTAT_H_
#define _PGSTAT_H_

#include "engine.h"

#define FIELD_ADDR(struct, x) (&struct[x])

void		die(void);
int			print_header(void);
int			keyboard_callback(int);
int			initdbblk(void);
int			initdbconfl(void);
int			initdbfs(void);
int			initdbtup(void);
int			initdbxact(void);
int			initindex(void);
int			initindexio(void);
int			inittableanalyze(void);
int			inittableioheap(void);
int			inittableioidx(void);
int			inittableiotidx(void);
int			inittableiotoast(void);
int			inittablescan(void);
int			inittabletup(void);
int			inittablevac(void);
int			initvacuum(void);
int			initstmtplan(void);
int			initstmtexec(void);
int			initstmtsharedblk(void);
int			initstmtlocalblk(void);
int			initstmttempblk(void);
int			initstmtwal(void);
int			initcopyprogress(void);
int			initbuffercacherel(void);
int			initbuffercachestat(void);

void		error(const char *fmt,...);
char	   *format_b(long long);

#endif							/* _PGSTAT_H_ */
