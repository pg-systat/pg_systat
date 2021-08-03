pg_systat
=========
pg_systat displays various PostgreSQL statistics in a screen-oriented fashion.
It allows you to monitor various PostgreSQL statistics tables from a terminal window.
It currently supports following statistics:

* pg_stat_database
* pg_tablespace
* pg_stat_all_tables
* pg_statio_all_tables
* pg_stat_progress_vacuum
* pg_stat_progress_vacuum
* pg_stat_progress_copy
* pg_buffercache

Installation
------------

Installation from Source Code::

	cmake CMakeLists.txt
	make
	make install

Uninstalling::

	xargs rm < install_manifest.txt

Availability
------------

Project home page:

  https://pg_systat.gitlab.io/


If you have git, you can download the source code::

  git clone git@gitlab.com:pg_systat/pg_systat.git
