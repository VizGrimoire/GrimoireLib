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

Examples of use:

```
session = buildSession(
    database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
    echo=False)
# Number of commits
res = session.query().select_nscmlog(["commits",]) \
    .filter_period(start=datetime(2012,9,1),
                   end=datetime(2014,1,1))
print res.scalar()
# Time series of commits
res = session.query().select_nscmlog(["commits",]) \
    .group_by_period() \
    .filter_period(end=datetime(2014,1,1))
ts = res.timeseries ()
print (ts)
```


== timeseries.py

Provides the TimeSeries class, for dealing with time series. This is one of the abstract data types that this module provides. Time series maintained by it keep track of empty periods, and zero them conveniently. Methods for conversion to JSON, for representation, etc. are provided.

Example of use:

```
data = [(datetime(2011,12,1), (1, 2, 3)),
            (datetime(2012,1,1), (4, 5, 6)),
            (datetime(2012,2,1), (7, 8, 9))]
ts = TimeSeries ("months", start=None, end=datetime(2014,8,1),
                 data=data, zerovalue=0)
```

== scm.py

High level interface to SCM (CVSAnalY) databases. Its main component is the SCM class, which models a SCM variable. The variable in particular that an SCM object represents is defined when instantiating the class (ncommits, nauthors, listcommits, listauthors).

Provided methods (return the defined variable in different formats):

* timeseries: returns a timeseries (timeseries of var). Works with "n" variables.

* total: returns total count (total of var). Works with "n" variables.

* list: returns a list (list of var). Works with "list" variables.


Examples of use:

```
data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
            var = "ncommits", dates = (None, None))
print data.timeseries()
print data.total()
data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
            var = "nauthors", dates = (datetime(2013,1,1), None))
print data.timeseries()
```

== Tests

There are some tests available:

* test_scm_query.py
