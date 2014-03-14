#! /usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2014 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Package to deal with queries for SCM data from *Grimoire
##  (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy import create_engine, func, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.sql import label
from datetime import datetime
from timeseries import TimeSeries

Base = declarative_base(cls=DeferredReflection)

def buildSession(database, echo):
    """Create a session with the database
        
    - database: string, url of the database, in the format
       mysql://user:passwd@host:port/database
    - echo: boolean, output SQL to stdout or not
        
    Instantiatates an engine and a session to work with it
    """

    engine = create_engine(database, encoding='utf8', echo=echo)
    Base.prepare(engine)
    # Create a session linked to the SCMQuery class
    Session = sessionmaker(bind=engine, query_cls=SCMQuery)
    session = Session()
    return (session)

class SCMLog(Base):
    """scmlog table"""
    __tablename__ = 'scmlog'
    author_id = Column(Integer, ForeignKey('people_upeople.people_id'))

class Actions(Base):
    """actions table"""

    __tablename__ = 'actions'
    commit_id = Column(Integer, ForeignKey('scmlog.id'))

class PeopleUPeople(Base):
    """people_upeople"""

    __tablename__ = 'people_upeople'
    upeople_id = Column(Integer, ForeignKey('upeople.id'))

class UPeople(Base):
    """upeople"""

    __tablename__ = 'upeople'


class SCMQuery (Query):
    """Class for dealing with SCM queries"""


    def select_ncommits(self):
        """Select number (count) of commits"""

        return self.add_columns (
            label("ncommits", func.count(func.distinct(SCMLog.id)))
            ) \
            .join(Actions)

    def select_listcommits(self):
        """Select a list of commits"""
        
        return self \
            .add_columns (label("id", func.distinct(SCMLog.id)),
                          label("date", SCMLog.date)) \
            .join(Actions)

    def select_nauthors(self):
        """Select number (count) of authors"""

        return self.add_columns (
            label("nauthors", func.count(func.distinct(SCMLog.author_id)))
            ) \
            .join(Actions)

    def select_listauthors(self):
        """Select a list of authors"""
        
        return self \
            .add_columns (label("id", func.distinct(UPeople.id)),
                          label("name", UPeople.identifier)) \
            .join (PeopleUPeople, UPeople.id == PeopleUPeople.upeople_id) \
            .join (SCMLog, PeopleUPeople.people_id == SCMLog.author_id)

    def filter_period(self, start = None, end = None):
        """Filter variable for a period

        - start: datetime, starting date
        - end: datetime, end date

        Commits considered are between starting date and end date
        (exactly: start <= date < end)
        """

        query = self
        if start is not None:
            self.start = start
            query = query.filter(SCMLog.date >= start.isoformat())
        if end is not None:
            self.end = end
            query = query.filter(SCMLog.date < end.isoformat())
        return query

    def group_by_period (self):
        """Group by time period (per month)"""

        return self \
            .add_columns (label("month", func.month(SCMLog.date)),
                          label("year", func.year(SCMLog.date))) \
            .group_by("month", "year").order_by("year", "month")

    def timeseries (self):
        """Return a TimeSeries object.

        The query has to include a group_by_period filter
        """

        data = []
        for row in self.all():
            # Extract real values (entries which are not year or month)
            # FIXME: this could be done much better, probably the
            # intermediary dict is not needed. The keyt is to extract
            # the tuple of real data, excluding year and month.
            values = []
            # doctRow used because row is KeyedTuple, and does not
            # support doctRow[key]
            dictRow = row._asdict()
            for key in row.keys():
                if key not in ("year", "month"):
                    print key, type(row), dictRow[key]
                    values.append (dictRow[key])
            data.append ((datetime (row.year, row.month, 1),
                         values))
        return TimeSeries (period = "months",
                           start = self.start, end = self.end,
                           data = data)
                        

    def __repr__ (self):

        if self.start is not None:
            start = self.start.isoformat()
        else:
            start = "ever"
        if self.end is not None:
            end = self.end.isoformat()
        else:
            end = "ever"
        repr = "SCMQuery from %s to %s\n" % (start, end)
        repr += Query.__str__(self)
        return repr

    def __str__ (self):

        return self.__repr__()

    def __init__ (self, entities, session):
        """Initialize the object
        
        self.start and self.end will be used in case there are temporal
        limits for the query (useful to produce TimeSeries objects,
        which needs those.
        """

        self.start = None
        self.end = None
        Query.__init__(self, entities, session)


if __name__ == "__main__":


    session = buildSession(
        database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo=False)

    # Number of commits
    res = session.query().select_ncommits() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_ncommits() \
        .filter_period(end=datetime(2014,1,1))
    print res.scalar()

    # Time series of commits
    res = session.query().select_ncommits() \
        .group_by_period() \
        .filter_period(end=datetime(2014,1,1))
    ts = res.timeseries ()
    print (ts)

    # List of commits
    res = session.query() \
        .select_listcommits() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    for row in res.limit(10).all():
        print row.id, row.date

    # Number of authors
    res = session.query().select_nauthors() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    # List of authors
    res = session.query() \
        .select_listauthors() \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1))
    print res
    for row in res.limit(10).all():
        print row.id, row.name
