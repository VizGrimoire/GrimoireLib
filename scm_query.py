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

from sqlalchemy import create_engine, func, Column, Integer, ForeignKey, or_
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
    author_id = Column(Integer, ForeignKey('people.id'))
    committer_id = Column(Integer, ForeignKey('people_upeople.people_id'))
    committer_id = Column(Integer, ForeignKey('people.id'))

class People(Base):
    """upeople"""

    __tablename__ = 'people'

class Actions(Base):
    """actions table"""

    __tablename__ = 'actions'
    commit_id = Column(Integer, ForeignKey('scmlog.id'))
    branch_id = Column(Integer, ForeignKey('branches.id'))

class PeopleUPeople(Base):
    """people_upeople"""

    __tablename__ = 'people_upeople'
    upeople_id = Column(Integer, ForeignKey('upeople.id'))

class UPeople(Base):
    """upeople"""

    __tablename__ = 'upeople'

class Branches(Base):
    """branches"""

    __tablename__ = 'branches'


class SCMQuery (Query):
    """Class for dealing with SCM queries"""

    def select_nscmlog(self, variables):
        """Select a variable which is a field in Scmlog.

        - variables (list): variables to select
            Currently supported: "commits", "authors", "committers"
        """

        if not isinstance(variables, (list, tuple)):
            raise Exception ("select_nscmlog: Argument is not list or tuple")
        elif len (variables) == 0:
            raise Exception ("select_nscmlog: No variables")
        fields = []
        for variable in variables:
            if variable == "commits":
                name = "nocommits"
                field = SCMLog.id
            elif variable == "authors":
                name = "nauthors"
                field = SCMLog.author_id
            elif variable == "committers":
                name = "ncommitters"
                field = SCMLog.committer_id
            else:
                raise Exception ("select_nscmlog: Unknown variable %s." \
                                     % variable)
            fields.append (label (name,
                                  func.count(func.distinct(field))))
        return self.add_columns (*fields)

    def select_listcommits(self):
        """Select a list of commits"""
        
        return self \
            .add_columns (label("id", func.distinct(SCMLog.id)),
                          label("date", SCMLog.date))

    def select_listpersons_uid(self, kind = "all"):
        """Select a list of persons (authors, committers), using uids

        - kind: kind of person to select
           authors: authors of commits
           committers: committers of commits
           all: authors and committers
        Returns a SCMQuery object, with (id, name, email) selected.
        """
        
        query = self.add_columns (label("id", func.distinct(UPeople.id)),
                                  label("name", UPeople.identifier)) \
                .join (PeopleUPeople, UPeople.id == PeopleUPeople.upeople_id)
        if kind == "authors":
            return query.join (SCMLog,
                               PeopleUPeople.people_id == SCMLog.author_id)
        elif kind == "committers":
            return query.join (SCMLog,
                               PeopleUPeople.people_id == SCMLog.committer_id)
        elif kind == "all":
            return query.join (SCMLog,
                               PeopleUPeople.people_id == SCMLog.author_id or
                               PeopleUPeople.people_id == SCMLog.committer_id)
        else:
            raise Exception ("select_listpersons_uid: Unknown kind %s." \
                             % kind)

    def select_listpersons(self, kind = "all"):
        """Select a list of persons (authors, committers)

        - kind: kind of person to select
           authors: authors of commits
           committers: committers of commits
           all: authors and committers

        Returns a SCMQuery object, with (id, name, email) selected.
        """

        query = self.add_columns (label("id", func.distinct(People.id)),
                                  label("name", People.name),
                                  label('email', People.email))
        if kind == "authors":
            return query.join (SCMLog, People.id == SCMLog.author_id)    
        elif kind == "committers":
            return query.join (SCMLog, People.id == SCMLog.committer_id)    
        elif kind == "all":
            return query.join (SCMLog,
                               People.id == SCMLog.author_id or
                               People.id == SCMLog.committer_id)
        else:
            raise Exception ("select_listpersons: Unknown kind %s." \
                             % kind)

    def select_nbranches(self):
        """Select number of branches in repo.

        The Actions table is used, instead of the Branches table,
        so that some filters, such as filter_period() can be used
        """

        query = self.add_columns (
            label("nbranches",
                  func.count(func.distinct(Actions.branch_id)))
            )
        return query

    def select_listbranches(self):
        """Select list of branches in repo

        Returns a list with a tuple (id, name) per branch
        The Actions table is used, instead of the Branches table,
        so that some filters, such as filter_period() can be used
        """

        query = self.add_columns (label("id",
                                        func.distinct(Actions.branch_id)),
                                  label("name",
                                        Branches.name))
        query = query.join(Branches)
        return query

    def filter_nomerges (self):
        """Consider only commits that touch files (no merges)

        For considering only those commits, join with Actions,
        where merge commits are not represented.
        """

        return self.join(Actions)

    def filter_branches (self, branches):
        """Filter variables for a set of branches

        For considering only those commits that happened in a branch.
        """

        query = self.join(Actions,Branches).filter(Branches.name.in_(branches))
        return query


    def filter_period(self, start = None, end = None, date = "commit"):
        """Filter variable for a period

        - start: datetime, starting date
        - end: datetime, end date
        - date: "commit" or "author"
            Git maintains "commit" and "author" date

        Commits considered are between starting date and end date
        (exactly: start <= date < end)
        """

        query = self
        if date == "author":
            scmlog_date = SCMLog.author_date
        elif date == "commit":
            scmlog_date = SCMLog.date
        if start is not None:
            self.start = start
            query = query.filter(scmlog_date >= start.isoformat())
        if end is not None:
            self.end = end
            query = query.filter(scmlog_date < end.isoformat())
        return query


    def filter_persons (self, list, kind = "all"):
        """Fiter for a certain list of persons (committers, authors)

        - list: list of People.id
        - kind: kind of person to select
           authors: authors of commits
           committers: committers of commits
           all: authors and committers
        Returns a SCMQuery object.
        """

        query = self
        if kind == "authors":
            return query.filter (SCMLog.author_id.in_(list))    
        elif kind == "committers":
            return query.filter (SCMLog.committer_id.in_(list))    
        elif kind == "all":
            return query.filter (or_(SCMLog.author_id.in_(list),
                                     SCMLog.committer_id.in_(list)))


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
            values = tuple(row[i] for i, k in enumerate(row.keys())
                           if not k in ("year", "month"))
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
    res = session.query().select_nscmlog(["commits",]) \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_nscmlog(["commits",]) \
        .filter_nomerges() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_nscmlog(["commits",]) \
        .filter_period(end=datetime(2014,1,1))
    print res.scalar()

    # Time series of commits
    res = session.query().select_nscmlog(["commits",]) \
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
    res = session.query().select_nscmlog(["authors",]) \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    # List of authors
    res = session.query() \
        .select_listpersons("authors") \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1))
    for row in res.limit(10).all():
        print row.id, row.name
    res = res.filter_branches(("master",))
    print res.all()
    # List of branches
    res = session.query().select_listbranches()
    print res.all()
    res = session.query().select_listbranches() \
        .join(SCMLog) \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1))
    print res.all()
