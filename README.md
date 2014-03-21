grimoire-api
============

Proof of concept of the Grimoire API, using SQLAlchemy. Currently only a part of the support for SCM (CVSAnalY databases) is provided.

== scm_query.py

Low level interface. Provides three main elements:

Classes for each of the tables in CVSAnalY databases: SCMLog, People, Actions, PeopleUPeople, UPeople, Actions.

SCMQuery class, which provides a interface to work with SCM tables. It inherites from Query, which means that can be used as SQLAlchemy Query is used, but providing some specialized methods as well.

buildSession function, to create a SQL session that works with the SCMQuery class instead of the standard Query class.

=== SCMQuery class

Provides an enriched interface to deal with SCM queries, maintaining the interface of the base Query class. This interface includes four kinds of functions:

* Select (select_nscmlog, select_listcommits, select_listpersons_uid, select_listpersons, select_nbranches, select_listbranches). Add the corresponding fields to the select part of the query, joining with the neeeded tables (when applicable).

* Filter (filter_nomerges, filter_branches, filter_period, filter_persons). Filters to specify some conditions on the query (usually, WHERE clauses), including joining with the needed tables (when applicable).

* Group_by (group_by_period). Grouping of results of the query.

* Return (timeseries). Return specific objects as a result of the query. These are similar to all() or value(), but returning classes provided by this interface.

In addition, __repr__ and __str__ are specialized, to facilitate debugging.
